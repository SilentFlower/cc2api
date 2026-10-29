//! 请求专用的编码链解析、严格有界解码及最终正文重编码。

use std::borrow::Cow;
use std::io::{Read, Write};

use axum::http::{HeaderMap, header::CONTENT_ENCODING};
use brotli::{
    BrotliDecompressStream, BrotliResult, BrotliState, CompressorReader, HeapAlloc, HuffmanCode,
};
use bytes::Bytes;
use flate2::bufread::MultiGzDecoder;
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::{Compression, Decompress, FlushDecompress, Status};
use tokio::task::spawn_blocking;
use zstd::stream::{encode_all, read::Decoder as ZstdDecoder};

/// 压缩请求原字节及每层解码结果的大小上限。
pub(crate) const REQUEST_BODY_LIMIT: usize = 10 * 1024 * 1024;

const MAX_ENCODING_FRAGMENTS: usize = 32;
const MAX_ENCODING_LAYERS: usize = 4;
const CODEC_BUFFER_SIZE: usize = 8192;

/// 请求编码声明或编解码失败的稳定分类，不携带原始头、正文或库错误。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestBodyCodecError {
    /// 声明不满足完整 token 语法，或列表片段过多。
    InvalidEncoding,
    /// 编码未知，或实际编码层数超出支持范围。
    UnsupportedEncoding,
    /// 原字节或任意一层解码结果超限。
    TooLarge,
    /// 正文无法在支持范围内完整解码。
    InvalidBody,
    /// 后台 worker 或本地编码器异常。
    Internal,
}

/// 请求声明的有效编码链；默认无编码，身份编码和空项不会保留。
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestBodyEncoding {
    layers: Vec<ContentCoding>,
}

/// 单层已支持编码；独立保留 x-gzip，以免重编码时改变调用方别名。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentCoding {
    Gzip,
    XGzip,
    Deflate,
    Brotli,
    Zstd,
}

impl RequestBodyEncoding {
    /// 从原始多值请求头解析并冻结有序编码链。
    ///
    /// @param headers 尚未经过有损头收集的原始请求头。
    /// @return 有效编码计划，或非法声明／不支持编码分类。
    pub(crate) fn from_headers(headers: &HeaderMap) -> Result<Self, RequestBodyCodecError> {
        let mut layers = Vec::new();
        let mut fragments = 0;
        let mut unsupported = false;

        for value in headers.get_all(CONTENT_ENCODING).iter() {
            let value = value
                .to_str()
                .map_err(|_| RequestBodyCodecError::InvalidEncoding)?;
            for fragment in value.split(',') {
                fragments += 1;
                if fragments > MAX_ENCODING_FRAGMENTS {
                    return Err(RequestBodyCodecError::InvalidEncoding);
                }
                let token = fragment.trim_matches([' ', '\t']);
                if token.is_empty() {
                    continue;
                }
                if !token.bytes().all(is_token_byte) {
                    return Err(RequestBodyCodecError::InvalidEncoding);
                }
                let coding = match token.to_ascii_lowercase().as_str() {
                    "identity" => continue,
                    "gzip" => ContentCoding::Gzip,
                    "x-gzip" => ContentCoding::XGzip,
                    "deflate" => ContentCoding::Deflate,
                    "br" => ContentCoding::Brotli,
                    "zstd" => ContentCoding::Zstd,
                    _ => {
                        unsupported = true;
                        continue;
                    }
                };
                layers.push(coding);
            }
        }

        // 先完成整条声明的语法校验，避免未知 token 掩盖后续非法片段。
        if unsupported || layers.len() > MAX_ENCODING_LAYERS {
            return Err(RequestBodyCodecError::UnsupportedEncoding);
        }
        Ok(Self { layers })
    }

    /// 判断是否存在需要实际处理的压缩层。
    ///
    /// @return 至少包含一个有效压缩层时为 true。
    pub(crate) fn is_encoded(&self) -> bool {
        !self.layers.is_empty()
    }

    /// 生成与最终编码字节一致的规范化 Content-Encoding。
    ///
    /// @return 按原顺序的小写有效编码链；无编码时为 None。
    pub(crate) fn content_encoding(&self) -> Option<String> {
        self.is_encoded().then(|| {
            self.layers
                .iter()
                .map(|coding| coding.token())
                .collect::<Vec<_>>()
                .join(", ")
        })
    }

    /// 逆序完整解码并检查原字节及每层输出的大小。
    ///
    /// @param body 入站原始正文；无编码时原样返回，不增加复制或 worker。
    /// @return 可供业务策略使用的逻辑正文，或稳定的拒绝分类。
    pub(crate) async fn decode(&self, body: Bytes) -> Result<Bytes, RequestBodyCodecError> {
        if !self.is_encoded() {
            return Ok(body);
        }
        if body.len() > REQUEST_BODY_LIMIT {
            return Err(RequestBodyCodecError::TooLarge);
        }
        let layers = self.layers.clone();
        spawn_blocking(move || {
            let mut decoded = body;
            for coding in layers.into_iter().rev() {
                decoded = Bytes::from(decode_layer(coding, &decoded)?);
            }
            Ok(decoded)
        })
        .await
        .map_err(|_| RequestBodyCodecError::Internal)?
    }

    /// 对本次改写完成的逻辑正文按原链正序重编码。
    ///
    /// @param body 已完成身份、Session 和 CCH 等改写的最终逻辑字节。
    /// @return 无编码时借用输入，否则返回全部编码成功后的发送字节。
    pub(crate) async fn encode<'a>(
        &self,
        body: &'a [u8],
    ) -> Result<Cow<'a, [u8]>, RequestBodyCodecError> {
        if !self.is_encoded() {
            return Ok(Cow::Borrowed(body));
        }
        let layers = self.layers.clone();
        let body = body.to_vec();
        let encoded = spawn_blocking(move || -> Result<Vec<u8>, RequestBodyCodecError> {
            let mut encoded = body;
            for coding in layers {
                encoded = encode_layer(coding, &encoded)?;
            }
            Ok(encoded)
        })
        .await
        .map_err(|_| RequestBodyCodecError::Internal)??;
        Ok(Cow::Owned(encoded))
    }
}

impl ContentCoding {
    fn token(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::XGzip => "x-gzip",
            Self::Deflate => "deflate",
            Self::Brotli => "br",
            Self::Zstd => "zstd",
        }
    }
}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn decode_layer(coding: ContentCoding, body: &[u8]) -> Result<Vec<u8>, RequestBodyCodecError> {
    if body.is_empty() {
        return Err(RequestBodyCodecError::InvalidBody);
    }
    match coding {
        ContentCoding::Gzip | ContentCoding::XGzip => {
            let mut decoder = MultiGzDecoder::new(body);
            let decoded = read_limited(&mut decoder)?;
            if !decoder.get_ref().is_empty() {
                return Err(RequestBodyCodecError::InvalidBody);
            }
            Ok(decoded)
        }
        ContentCoding::Deflate => match decode_flate(body, true) {
            // 仅格式失败才尝试 raw；超限不能触发第二次解压。
            Err(RequestBodyCodecError::InvalidBody) => decode_flate(body, false),
            result => result,
        },
        ContentCoding::Brotli => decode_brotli(body),
        ContentCoding::Zstd => {
            let mut decoder =
                ZstdDecoder::with_buffer(body).map_err(|_| RequestBodyCodecError::Internal)?;
            // 必须在第一次读取前设限，后续 frame 也沿用该窗口限制。
            decoder
                .window_log_max(24)
                .map_err(|_| RequestBodyCodecError::Internal)?;
            let decoded = read_limited(&mut decoder)?;
            if !decoder.get_ref().is_empty() {
                return Err(RequestBodyCodecError::InvalidBody);
            }
            Ok(decoded)
        }
    }
}

fn read_limited(reader: &mut impl Read) -> Result<Vec<u8>, RequestBodyCodecError> {
    let mut decoded = Vec::new();
    // 多读一个字节区分超限；恰好达到上限仍需验证 CRC、校验和及完整结尾。
    let result = reader
        .take((REQUEST_BODY_LIMIT + 1) as u64)
        .read_to_end(&mut decoded);
    if decoded.len() > REQUEST_BODY_LIMIT {
        return Err(RequestBodyCodecError::TooLarge);
    }
    result.map_err(|_| RequestBodyCodecError::InvalidBody)?;
    Ok(decoded)
}

fn decode_flate(body: &[u8], zlib_header: bool) -> Result<Vec<u8>, RequestBodyCodecError> {
    let mut decoder = Decompress::new(zlib_header);
    let mut decoded = Vec::new();
    let mut chunk = [0; CODEC_BUFFER_SIZE];
    loop {
        let before_in = decoder.total_in();
        let before_out = decoder.total_out();
        let available = chunk.len().min(REQUEST_BODY_LIMIT + 1 - decoded.len());
        let result = decoder.decompress(
            &body[before_in as usize..],
            &mut chunk[..available],
            FlushDecompress::None,
        );
        let produced = (decoder.total_out() - before_out) as usize;
        decoded.extend_from_slice(&chunk[..produced]);
        if decoded.len() > REQUEST_BODY_LIMIT {
            return Err(RequestBodyCodecError::TooLarge);
        }
        match result.map_err(|_| RequestBodyCodecError::InvalidBody)? {
            Status::StreamEnd if decoder.total_in() as usize == body.len() => return Ok(decoded),
            Status::StreamEnd => return Err(RequestBodyCodecError::InvalidBody),
            _ if before_in == decoder.total_in() && before_out == decoder.total_out() => {
                // 普通 reader 的 EOF 可能掩盖缺失结尾，这里必须明确到达 StreamEnd。
                return Err(RequestBodyCodecError::InvalidBody);
            }
            _ => {}
        }
    }
}

fn decode_brotli(body: &[u8]) -> Result<Vec<u8>, RequestBodyCodecError> {
    // strict 仅接受标准窗口，禁止会突破既定窗口范围的 large-window 扩展。
    let mut state = BrotliState::new_strict(
        HeapAlloc::<u8>::default(),
        HeapAlloc::<u32>::default(),
        HeapAlloc::<HuffmanCode>::default(),
    );
    let mut available_in = body.len();
    let mut input_offset = 0;
    let mut total_out = 0;
    let mut decoded = Vec::new();
    let mut chunk = [0; CODEC_BUFFER_SIZE];
    loop {
        let before_in = input_offset;
        let mut available_out = chunk.len().min(REQUEST_BODY_LIMIT + 1 - decoded.len());
        let mut output_offset = 0;
        let result = BrotliDecompressStream(
            &mut available_in,
            &mut input_offset,
            body,
            &mut available_out,
            &mut output_offset,
            &mut chunk,
            &mut total_out,
            &mut state,
        );
        decoded.extend_from_slice(&chunk[..output_offset]);
        if decoded.len() > REQUEST_BODY_LIMIT {
            return Err(RequestBodyCodecError::TooLarge);
        }
        match result {
            BrotliResult::ResultSuccess if available_in == 0 && input_offset == body.len() => {
                return Ok(decoded);
            }
            BrotliResult::NeedsMoreOutput if before_in != input_offset || output_offset != 0 => {}
            _ => return Err(RequestBodyCodecError::InvalidBody),
        }
    }
}

fn encode_layer(coding: ContentCoding, body: &[u8]) -> Result<Vec<u8>, RequestBodyCodecError> {
    match coding {
        ContentCoding::Gzip | ContentCoding::XGzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(body)
                .map_err(|_| RequestBodyCodecError::Internal)?;
            encoder
                .finish()
                .map_err(|_| RequestBodyCodecError::Internal)
        }
        ContentCoding::Deflate => {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(body)
                .map_err(|_| RequestBodyCodecError::Internal)?;
            encoder
                .finish()
                .map_err(|_| RequestBodyCodecError::Internal)
        }
        ContentCoding::Brotli => {
            let mut encoder = CompressorReader::new(body, CODEC_BUFFER_SIZE, 4, 22);
            let mut encoded = Vec::new();
            encoder
                .read_to_end(&mut encoded)
                .map_err(|_| RequestBodyCodecError::Internal)?;
            Ok(encoded)
        }
        ContentCoding::Zstd => encode_all(body, 1).map_err(|_| RequestBodyCodecError::Internal),
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::{Read, Write};

    use axum::http::{HeaderMap, HeaderValue, header::CONTENT_ENCODING};
    use brotli::enc::BrotliEncoderParams;
    use brotli::{BrotliCompress, Decompressor as BrotliDecoder};
    use bytes::Bytes;
    use flate2::Compression;
    use flate2::bufread::MultiGzDecoder;
    use flate2::read::ZlibDecoder;
    use flate2::write::DeflateEncoder;
    use zstd::stream::{read::Decoder as ZstdDecoder, write::Encoder as ZstdEncoder};

    use super::{
        ContentCoding, REQUEST_BODY_LIMIT, RequestBodyCodecError, RequestBodyEncoding, encode_layer,
    };

    fn encoding(value: &str) -> RequestBodyEncoding {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, HeaderValue::from_str(value).unwrap());
        RequestBodyEncoding::from_headers(&headers).unwrap()
    }

    fn parse(value: &str) -> Result<RequestBodyEncoding, RequestBodyCodecError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, HeaderValue::from_str(value).unwrap());
        RequestBodyEncoding::from_headers(&headers)
    }

    fn read_all(mut reader: impl Read) -> Vec<u8> {
        let mut body = Vec::new();
        reader.read_to_end(&mut body).unwrap();
        body
    }

    fn raw_deflate(body: &[u8]) -> Vec<u8> {
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    fn checksummed_zstd(body: &[u8]) -> Vec<u8> {
        let mut encoder = ZstdEncoder::new(Vec::new(), 1).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    fn skippable_frame(size: usize) -> Vec<u8> {
        assert!(size >= 8);
        let mut frame = vec![0; size];
        frame[..4].copy_from_slice(&[0x50, 0x2a, 0x4d, 0x18]);
        frame[4..8].copy_from_slice(&((size - 8) as u32).to_le_bytes());
        frame
    }

    #[test]
    fn headers_preserve_multiple_values_order_and_aliases() {
        let mut headers = HeaderMap::new();
        headers.append(
            "Content-Encoding",
            HeaderValue::from_static(" \tGZIP, identity,,X-GZIP\t"),
        );
        headers.append("content-encoding", HeaderValue::from_static(" Br, ZSTD "));
        let plan = RequestBodyEncoding::from_headers(&headers).unwrap();
        assert_eq!(
            plan.content_encoding().as_deref(),
            Some("gzip, x-gzip, br, zstd")
        );
        assert!(plan.is_encoded());
    }

    #[test]
    fn headers_ignore_only_identity_empty_lists_and_accept_encoding() {
        for value in ["", " , , \t", " IDENTITY, identity,, \t"] {
            let plan = encoding(value);
            assert!(!plan.is_encoded());
            assert!(plan.content_encoding().is_none());
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept-encoding",
            HeaderValue::from_static("gzip, deflate, br, zstd"),
        );
        assert!(
            !RequestBodyEncoding::from_headers(&headers)
                .unwrap()
                .is_encoded()
        );
    }

    #[test]
    fn headers_distinguish_invalid_grammar_from_unknown_complete_tokens() {
        for value in [
            "gzip;q=1",
            "\"gzip\"",
            "gzip br",
            "gzip\tbr",
            "gzip/br",
            "compress, gzip;q=1",
        ] {
            assert_eq!(
                parse(value).unwrap_err(),
                RequestBodyCodecError::InvalidEncoding,
                "{value:?}"
            );
        }
        for value in ["compress", "gzipped", "*", "x!#$%&'*+-.^_`|~9"] {
            assert_eq!(
                parse(value).unwrap_err(),
                RequestBodyCodecError::UnsupportedEncoding,
                "{value:?}"
            );
        }
        let mut headers = HeaderMap::new();
        headers.append(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        headers.append(CONTENT_ENCODING, HeaderValue::from_bytes(b"\x80").unwrap());
        assert_eq!(
            RequestBodyEncoding::from_headers(&headers).unwrap_err(),
            RequestBodyCodecError::InvalidEncoding
        );
    }

    #[test]
    fn headers_bound_fragments_across_fields_and_actual_coding_layers() {
        assert!(!encoding(&["identity"; 32].join(",")).is_encoded());
        assert!(!encoding(&[""; 32].join(",")).is_encoded());
        assert_eq!(
            parse(&["identity"; 33].join(",")).unwrap_err(),
            RequestBodyCodecError::InvalidEncoding
        );
        assert_eq!(
            parse(&[""; 33].join(",")).unwrap_err(),
            RequestBodyCodecError::InvalidEncoding
        );
        let mut headers = HeaderMap::new();
        headers.append(
            CONTENT_ENCODING,
            HeaderValue::from_str(&[""; 16].join(",")).unwrap(),
        );
        headers.append(
            CONTENT_ENCODING,
            HeaderValue::from_str(&["identity"; 16].join(",")).unwrap(),
        );
        assert!(RequestBodyEncoding::from_headers(&headers).is_ok());
        headers.append(CONTENT_ENCODING, HeaderValue::from_static(""));
        assert_eq!(
            RequestBodyEncoding::from_headers(&headers).unwrap_err(),
            RequestBodyCodecError::InvalidEncoding
        );
        assert_eq!(
            encoding("gzip, identity, br, zstd, deflate, identity")
                .content_encoding()
                .as_deref(),
            Some("gzip, br, zstd, deflate")
        );
        assert_eq!(
            parse("gzip, br, zstd, deflate, gzip").unwrap_err(),
            RequestBodyCodecError::UnsupportedEncoding
        );
    }

    #[tokio::test]
    async fn unencoded_body_keeps_storage_and_borrowing_without_sniffing() {
        let body = Bytes::from_static(b"\x1f\x8b\x08\x00synthetic");
        for plan in [RequestBodyEncoding::default(), encoding(", identity, ")] {
            let pointer = body.as_ptr();
            let decoded = plan.decode(body.clone()).await.unwrap();
            assert_eq!(decoded.as_ptr(), pointer);
            assert_eq!(decoded, body);
            let encoded = plan.encode(&body).await.unwrap();
            assert!(matches!(encoded, Cow::Borrowed(_)));
            assert_eq!(encoded.as_ptr(), pointer);
        }
    }

    #[tokio::test]
    async fn supported_codecs_and_four_layer_chains_roundtrip() {
        let body = b"synthetic byte payload\0\xff with repeated repeated bytes";
        for value in [
            "gzip",
            "x-gzip",
            "deflate",
            "br",
            "zstd",
            "gzip, br, zstd, deflate",
            "x-gzip, gzip, gzip, br",
        ] {
            let plan = encoding(value);
            for body in [body.as_slice(), b"".as_slice()] {
                let wire = plan.encode(body).await.unwrap();
                assert!(matches!(wire, Cow::Owned(_)));
                let decoded = plan.decode(Bytes::from(wire.into_owned())).await.unwrap();
                assert_eq!(decoded.as_ref(), body, "{value}");
            }
        }

        // 用独立 reader 按声明逆序拆开，防止两个方向同时写反却仍往返成功。
        let plan = encoding("gzip, br, zstd, deflate");
        let wire = plan.encode(body).await.unwrap();
        let zstd = read_all(ZlibDecoder::new(wire.as_ref()));
        let brotli = read_all(ZstdDecoder::with_buffer(zstd.as_slice()).unwrap());
        let gzip = read_all(BrotliDecoder::new(brotli.as_slice(), 4096));
        assert_eq!(read_all(MultiGzDecoder::new(gzip.as_slice())), body);
    }

    #[tokio::test]
    async fn raw_deflate_input_is_reencoded_as_standard_zlib() {
        let plan = encoding("deflate");
        let body = b"synthetic raw deflate compatibility";
        assert_eq!(
            plan.decode(Bytes::from(raw_deflate(body)))
                .await
                .unwrap()
                .as_ref(),
            body
        );
        let final_body = b"synthetic body changed before sending";
        let wire = plan.encode(final_body).await.unwrap();
        assert_eq!(read_all(ZlibDecoder::new(wire.as_ref())), final_body);
    }

    #[tokio::test]
    async fn all_codecs_reject_truncated_prefixes_and_trailing_garbage() {
        let body = b"synthetic complete stream with checksum boundaries";
        for value in ["gzip", "x-gzip", "deflate", "br", "zstd", "raw"] {
            let plan = encoding(if value == "raw" { "deflate" } else { value });
            let wire = if value == "raw" {
                raw_deflate(body)
            } else {
                plan.encode(body).await.unwrap().into_owned()
            };
            for end in 0..wire.len() {
                assert_eq!(
                    plan.decode(Bytes::copy_from_slice(&wire[..end]))
                        .await
                        .unwrap_err(),
                    RequestBodyCodecError::InvalidBody,
                    "{value} prefix={end}"
                );
            }
            for tail in [b"garbage".as_slice(), &[0; 16]] {
                assert_eq!(
                    plan.decode(Bytes::from([wire.as_slice(), tail].concat()))
                        .await
                        .unwrap_err(),
                    RequestBodyCodecError::InvalidBody,
                    "{value} trailing={tail:?}"
                );
            }
            if matches!(value, "deflate" | "raw" | "br") {
                assert_eq!(
                    plan.decode(Bytes::from([wire.as_slice(), wire.as_slice()].concat()))
                        .await
                        .unwrap_err(),
                    RequestBodyCodecError::InvalidBody,
                    "{value} second stream"
                );
            }
        }
    }

    #[tokio::test]
    async fn gzip_accepts_all_members_and_verifies_each_crc_and_isize() {
        let plan = encoding("gzip");
        let first = plan.encode(b"synthetic ").await.unwrap().into_owned();
        let second = plan.encode(b"members").await.unwrap().into_owned();
        let members = [first.as_slice(), second.as_slice()].concat();
        assert_eq!(
            plan.decode(Bytes::from(members.clone()))
                .await
                .unwrap()
                .as_ref(),
            b"synthetic members"
        );
        for trailer_offset in [8, 4] {
            let mut broken = members.clone();
            let index = broken.len() - trailer_offset;
            broken[index] ^= 1;
            assert_eq!(
                plan.decode(Bytes::from(broken)).await.unwrap_err(),
                RequestBodyCodecError::InvalidBody
            );
        }
        let empty_member = plan.encode(b"").await.unwrap().into_owned();
        let with_empty_members = [empty_member.repeat(8), members, empty_member.repeat(8)].concat();
        assert_eq!(
            plan.decode(Bytes::from(with_empty_members))
                .await
                .unwrap()
                .as_ref(),
            b"synthetic members"
        );
    }

    #[tokio::test]
    async fn deflate_verifies_adler_and_requires_complete_stream_end() {
        let plan = encoding("deflate");
        let wire = plan.encode(b"synthetic zlib checksum").await.unwrap();
        let mut broken = wire.to_vec();
        let last = broken.len() - 1;
        broken[last] ^= 1;
        for body in [broken, wire[..wire.len() - 4].to_vec()] {
            assert_eq!(
                plan.decode(Bytes::from(body)).await.unwrap_err(),
                RequestBodyCodecError::InvalidBody
            );
        }
    }

    #[tokio::test]
    async fn zstd_validates_all_frames_skippable_lengths_and_optional_checksums() {
        let plan = encoding("zstd");
        let first = plan.encode(b"synthetic ").await.unwrap().into_owned();
        let second = checksummed_zstd(b"frames");
        let skip = skippable_frame(11);
        let frames = [
            skip.as_slice(),
            first.as_slice(),
            skip.as_slice(),
            second.as_slice(),
            skip.as_slice(),
        ]
        .concat();
        assert_eq!(
            plan.decode(Bytes::from(frames)).await.unwrap().as_ref(),
            b"synthetic frames"
        );
        assert!(
            plan.decode(Bytes::from(skip.clone()))
                .await
                .unwrap()
                .is_empty()
        );
        for end in 0..skip.len() {
            assert_eq!(
                plan.decode(Bytes::copy_from_slice(&skip[..end]))
                    .await
                    .unwrap_err(),
                RequestBodyCodecError::InvalidBody
            );
        }
        let mut broken = second;
        let last = broken.len() - 1;
        broken[last] ^= 1;
        assert_eq!(
            plan.decode(Bytes::from([first.as_slice(), broken.as_slice()].concat()))
                .await
                .unwrap_err(),
            RequestBodyCodecError::InvalidBody
        );
    }

    #[tokio::test]
    async fn window_limits_apply_to_first_and_subsequent_frames() {
        let plan = encoding("zstd");
        // 此合法 frame 输出两个字节，但声明 32 MiB 窗口，不能仅按输出量放行。
        let high_window = [0x28, 0xb5, 0x2f, 0xfd, 0, 0x78, 0x11, 0, 0, b'o', b'k'];
        assert_eq!(
            read_all(ZstdDecoder::with_buffer(high_window.as_slice()).unwrap()),
            b"ok"
        );
        let mut supported_window = high_window;
        supported_window[5] = 0x70;
        assert_eq!(
            plan.decode(Bytes::copy_from_slice(&supported_window))
                .await
                .unwrap()
                .as_ref(),
            b"ok"
        );
        for wire in [
            high_window.to_vec(),
            [supported_window.as_slice(), high_window.as_slice()].concat(),
        ] {
            assert_eq!(
                plan.decode(Bytes::from(wire)).await.unwrap_err(),
                RequestBodyCodecError::InvalidBody
            );
        }

        let plan = encoding("br");
        for (lgwin, large_window) in [(24, false), (25, true)] {
            let params = BrotliEncoderParams {
                quality: 4,
                lgwin,
                large_window,
                ..Default::default()
            };
            let mut source = b"synthetic brotli window".as_slice();
            let mut wire = Vec::new();
            BrotliCompress(&mut source, &mut wire, &params).unwrap();
            assert_eq!(
                read_all(BrotliDecoder::new(wire.as_slice(), 4096)),
                b"synthetic brotli window"
            );
            let decoded = plan.decode(Bytes::from(wire)).await;
            if large_window {
                assert_eq!(decoded.unwrap_err(), RequestBodyCodecError::InvalidBody);
            } else {
                assert_eq!(decoded.unwrap().as_ref(), b"synthetic brotli window");
            }
        }
    }

    #[tokio::test]
    async fn decoded_limit_is_inclusive_and_still_validates_stream_end() {
        let exact = vec![b'x'; REQUEST_BODY_LIMIT];
        let too_large = vec![b'x'; REQUEST_BODY_LIMIT + 1];
        for value in ["gzip", "x-gzip", "deflate", "br", "zstd", "raw"] {
            let plan = encoding(if value == "raw" { "deflate" } else { value });
            let wire = if value == "raw" {
                raw_deflate(&exact)
            } else {
                plan.encode(&exact).await.unwrap().into_owned()
            };
            assert_eq!(
                plan.decode(Bytes::copy_from_slice(&wire))
                    .await
                    .unwrap()
                    .as_ref(),
                exact,
                "{value} exact limit"
            );
            assert_eq!(
                plan.decode(Bytes::copy_from_slice(&wire[..wire.len() - 1]))
                    .await
                    .unwrap_err(),
                RequestBodyCodecError::InvalidBody,
                "{value} missing final byte at limit"
            );
            let wire = if value == "raw" {
                raw_deflate(&too_large)
            } else {
                plan.encode(&too_large).await.unwrap().into_owned()
            };
            assert_eq!(
                plan.decode(Bytes::from(wire)).await.unwrap_err(),
                RequestBodyCodecError::TooLarge,
                "{value} limit plus one"
            );
        }
        let plan = encoding("gzip");
        let mut broken = plan.encode(&exact).await.unwrap().into_owned();
        let crc = broken.len() - 8;
        broken[crc] ^= 1;
        assert_eq!(
            plan.decode(Bytes::from(broken)).await.unwrap_err(),
            RequestBodyCodecError::InvalidBody
        );
    }

    #[tokio::test]
    async fn wire_size_limit_is_inclusive_and_checked_before_decoding() {
        let plan = encoding("zstd");
        // skippable payload 允许精确构造合法的 10 MiB 原始流，解码结果为空。
        assert!(
            plan.decode(Bytes::from(skippable_frame(REQUEST_BODY_LIMIT)))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            plan.decode(Bytes::from(skippable_frame(REQUEST_BODY_LIMIT + 1)))
                .await
                .unwrap_err(),
            RequestBodyCodecError::TooLarge
        );
        assert_eq!(
            encoding("gzip")
                .decode(Bytes::from(vec![0; REQUEST_BODY_LIMIT + 1]))
                .await
                .unwrap_err(),
            RequestBodyCodecError::TooLarge
        );
    }

    #[tokio::test]
    async fn intermediate_layer_limit_is_enforced_even_when_final_body_is_empty() {
        let plan = encoding("zstd, gzip");
        for size in [REQUEST_BODY_LIMIT, REQUEST_BODY_LIMIT + 1] {
            let intermediate = skippable_frame(size);
            assert!(
                read_all(ZstdDecoder::with_buffer(intermediate.as_slice()).unwrap()).is_empty()
            );
            let wire = encode_layer(ContentCoding::Gzip, &intermediate).unwrap();
            assert!(wire.len() < REQUEST_BODY_LIMIT);
            let decoded = plan.decode(Bytes::from(wire)).await;
            if size == REQUEST_BODY_LIMIT {
                assert!(decoded.unwrap().is_empty());
            } else {
                assert_eq!(decoded.unwrap_err(), RequestBodyCodecError::TooLarge);
            }
        }
    }

    #[tokio::test]
    async fn reencoding_allows_actual_container_expansion_beyond_inbound_limit() {
        // 确定性噪声确保编码结果实际超过上限，避免可压缩样本掩盖错误的出站限制。
        let mut state = 0x4d65_9218_e32a_3268_u64;
        let body: Vec<u8> = (0..REQUEST_BODY_LIMIT)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        let wire = encoding("gzip").encode(&body).await.unwrap();
        assert!(wire.len() > REQUEST_BODY_LIMIT);
        assert_eq!(read_all(MultiGzDecoder::new(wire.as_ref())), body);
    }
}
