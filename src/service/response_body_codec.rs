//! 响应统计专用增量解码；所有输出立即下推，不积累整个响应。

use std::io::{self, Write};
use std::mem::take;

use brotli::{BrotliDecompressStream, BrotliResult, BrotliState, HeapAlloc, HuffmanCode};
use flate2::write::MultiGzDecoder;
use flate2::{Decompress, FlushDecompress, Status};
use zstd::stream::raw::{DParameter, Decoder, Operation};

const OUTPUT_BLOCK: usize = 8192;
const MAX_CODING_LAYERS: usize = 4;

/// 旁路解码的受控失败，不向业务响应暴露 codec 错误文本。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseDecodeError {
    InvalidData,
    SamplingLimit,
}

/// 持有解码链与末端观察器；空间取决于窗口和层数，不取决于响应长度。
pub(crate) struct ResponseBodyDecoder<W: Write> {
    stage: Option<Stage<W>>,
    configured: bool,
    supplied: bool,
    failure: Option<ResponseDecodeError>,
}

impl<W: Write> ResponseBodyDecoder<W> {
    /// 建立无编码观察链。
    /// @param sink 立即消费解码字节的末端观察器。
    /// @return 尚未接收输入的解码器。
    pub(crate) fn new(sink: W) -> Self {
        Self {
            stage: Some(Stage::Sink(Some(sink))),
            configured: false,
            supplied: false,
            failure: None,
        }
    }

    /// 在首块输入前配置既有响应头解析所得的编码链。
    /// @param codings 按应用顺序排列的规范编码名称。
    /// @return 成功或受控观察失败，失败不消费末端摘要。
    pub(crate) fn configure(&mut self, codings: &[String]) -> Result<(), ResponseDecodeError> {
        if self.configured || self.supplied {
            return self.fail(ResponseDecodeError::InvalidData);
        }
        self.configured = true;
        let effective = codings
            .iter()
            .filter(|coding| coding.as_str() != "identity")
            .count();
        if effective > MAX_CODING_LAYERS {
            return self.fail(ResponseDecodeError::SamplingLimit);
        }
        if codings.iter().any(|coding| {
            !matches!(
                coding.as_str(),
                "identity" | "gzip" | "x-gzip" | "deflate" | "br" | "zstd"
            )
        }) {
            return self.fail(ResponseDecodeError::InvalidData);
        }
        // 按应用顺序包裹，最后应用的编码自然成为最先解码的外层。
        for coding in codings
            .iter()
            .filter(|coding| coding.as_str() != "identity")
        {
            let zstd = if coding == "zstd" {
                let prepared = Decoder::new().and_then(|mut decoder| {
                    decoder.set_parameter(DParameter::WindowLogMax(24))?;
                    Ok(decoder)
                });
                match prepared {
                    Ok(decoder) => Some(decoder),
                    Err(_) => return self.fail(ResponseDecodeError::InvalidData),
                }
            } else {
                None
            };
            let next = Box::new(self.stage.take().expect("解码链始终保留末端观察器"));
            self.stage = Some(match coding.as_str() {
                "gzip" | "x-gzip" => Stage::Gzip(MultiGzDecoder::new(next)),
                "deflate" => Stage::Deflate(Box::new(DeflateStage {
                    decoder: None,
                    prefix: Vec::with_capacity(2),
                    ended: false,
                    next,
                })),
                "br" => Stage::Brotli(Box::new(BrotliStage {
                    decoder: BrotliState::new_strict(
                        HeapAlloc::default(),
                        HeapAlloc::default(),
                        HeapAlloc::default(),
                    ),
                    total_out: 0,
                    ended: false,
                    next,
                })),
                "zstd" => Stage::Zstd(Box::new(ZstdStage {
                    decoder: zstd.expect("zstd 在移动下游前已初始化"),
                    at_boundary: false,
                    next,
                })),
                _ => unreachable!("编码名称已经完整验证"),
            });
        }
        Ok(())
    }

    /// 逐块观察原始响应，失败后不再使用损坏的 codec 状态。
    /// @param bytes 上游原始响应片段。
    /// @return 成功或该观察链的固定失败类别。
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Result<(), ResponseDecodeError> {
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        self.supplied |= !bytes.is_empty();
        let stage = self.stage.as_mut().expect("解码链始终保留末端观察器");
        for part in bytes.chunks(OUTPUT_BLOCK) {
            if stage.write_all(part).and_then(|()| stage.flush()).is_err() {
                return self.fail(ResponseDecodeError::InvalidData);
            }
        }
        Ok(())
    }

    /// 校验每层结束，从外到内排出已接收的字节，不读取网络。
    /// @return 完整结束或受控失败。
    pub(crate) fn finish(&mut self) -> Result<(), ResponseDecodeError> {
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        if self
            .stage
            .as_mut()
            .expect("解码链始终保留末端观察器")
            .finish()
            .is_err()
        {
            return self.fail(ResponseDecodeError::InvalidData);
        }
        Ok(())
    }

    /// 访问仍然由解码链拥有的安全观察状态。
    /// @return 尚未取走的末端状态。
    pub(crate) fn sink_mut(&mut self) -> Option<&mut W> {
        self.stage.as_mut()?.sink_mut()
    }

    /// 一次性移交摘要，之后隐式 codec Drop 不再修改观察结果。
    /// @return 原末端观察状态，重复取走返回空。
    pub(crate) fn take_sink(&mut self) -> Option<W> {
        self.stage.as_mut()?.take_sink()
    }

    fn fail(&mut self, failure: ResponseDecodeError) -> Result<(), ResponseDecodeError> {
        Err(*self.failure.get_or_insert(failure))
    }
}

impl<W: Write> Drop for ResponseBodyDecoder<W> {
    fn drop(&mut self) {
        // gzip 的 Drop 会隐式 flush；先移走末端，避免再次运行已失败的内层 codec。
        let _ = self.take_sink();
    }
}

enum Stage<W: Write> {
    Sink(Option<W>),
    Gzip(MultiGzDecoder<Box<Stage<W>>>),
    Deflate(Box<DeflateStage<W>>),
    Brotli(Box<BrotliStage<W>>),
    Zstd(Box<ZstdStage<W>>),
}

impl<W: Write> Stage<W> {
    fn sink_mut(&mut self) -> Option<&mut W> {
        match self {
            Self::Sink(sink) => sink.as_mut(),
            Self::Gzip(decoder) => decoder.get_mut().sink_mut(),
            Self::Deflate(stage) => stage.next.sink_mut(),
            Self::Brotli(stage) => stage.next.sink_mut(),
            Self::Zstd(stage) => stage.next.sink_mut(),
        }
    }

    fn take_sink(&mut self) -> Option<W> {
        match self {
            Self::Sink(sink) => sink.take(),
            Self::Gzip(decoder) => decoder.get_mut().take_sink(),
            Self::Deflate(stage) => stage.next.take_sink(),
            Self::Brotli(stage) => stage.next.take_sink(),
            Self::Zstd(stage) => stage.next.take_sink(),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        match self {
            Self::Sink(sink) => sink.as_mut().map_or(Ok(()), Write::flush),
            Self::Gzip(decoder) => {
                // 使用非消费式校验；CRC 错误时仍能取回下游已有摘要。
                decoder.try_finish()?;
                decoder.get_mut().finish()
            }
            Self::Deflate(stage) => {
                stage.feed(&[])?;
                if !stage.ended {
                    return Err(invalid_data());
                }
                stage.next.finish()
            }
            Self::Brotli(stage) => {
                stage.feed(&[])?;
                if !stage.ended {
                    return Err(invalid_data());
                }
                stage.next.finish()
            }
            Self::Zstd(stage) => {
                stage.feed(&[])?;
                if !stage.at_boundary {
                    return Err(invalid_data());
                }
                stage.next.finish()
            }
        }
    }
}

impl<W: Write> Write for Stage<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.sink_mut().is_none() {
            return Ok(bytes.len());
        }
        match self {
            Self::Sink(sink) => {
                if let Some(sink) = sink {
                    sink.write_all(bytes)?;
                }
            }
            Self::Gzip(decoder) => {
                decoder.write_all(bytes)?;
            }
            Self::Deflate(stage) => stage.feed(bytes)?,
            Self::Brotli(stage) => stage.feed(bytes)?,
            Self::Zstd(stage) => stage.feed(bytes)?,
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.sink_mut().is_none() {
            return Ok(());
        }
        match self {
            Self::Sink(sink) => sink.as_mut().map_or(Ok(()), Write::flush),
            Self::Gzip(decoder) => decoder.flush(),
            Self::Deflate(stage) => stage.next.flush(),
            Self::Brotli(stage) => stage.next.flush(),
            Self::Zstd(stage) => stage.next.flush(),
        }
    }
}

struct DeflateStage<W: Write> {
    decoder: Option<Decompress>,
    prefix: Vec<u8>,
    ended: bool,
    next: Box<Stage<W>>,
}

impl<W: Write> DeflateStage<W> {
    fn feed(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        if self.decoder.is_none() {
            let count = (2 - self.prefix.len()).min(bytes.len());
            self.prefix.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.prefix.len() < 2 {
                return Ok(());
            }
            let header = u16::from_be_bytes([self.prefix[0], self.prefix[1]]);
            let zlib =
                self.prefix[0] & 15 == 8 && self.prefix[0] >> 4 <= 7 && header.is_multiple_of(31);
            // 有限头分流不能证明格式唯一；晚期失败保守终止观察，不重放整流。
            self.decoder = Some(Decompress::new(zlib));
            let prefix = take(&mut self.prefix);
            self.decode(&prefix)?;
        }
        self.decode(bytes)
    }

    fn decode(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.ended {
            return if bytes.is_empty() {
                Ok(())
            } else {
                Err(invalid_data())
            };
        }
        let decoder = self.decoder.as_mut().expect("前缀已选择解码格式");
        let mut offset = 0;
        loop {
            let mut output = [0; OUTPUT_BLOCK];
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let status = decoder.decompress(&bytes[offset..], &mut output, FlushDecompress::None);
            let consumed = (decoder.total_in() - before_in) as usize;
            let produced = (decoder.total_out() - before_out) as usize;
            offset += consumed;
            self.next.write_all(&output[..produced])?;
            match status {
                Ok(Status::StreamEnd) => {
                    self.ended = true;
                    return if offset == bytes.len() {
                        Ok(())
                    } else {
                        Err(invalid_data())
                    };
                }
                Err(_) => return Err(invalid_data()),
                _ => {}
            }
            if consumed == 0 && produced == 0 {
                return if offset == bytes.len() {
                    Ok(())
                } else {
                    Err(invalid_data())
                };
            }
            if offset == bytes.len() && produced < OUTPUT_BLOCK {
                return Ok(());
            }
        }
    }
}

struct BrotliStage<W: Write> {
    decoder: BrotliState<HeapAlloc<u8>, HeapAlloc<u32>, HeapAlloc<HuffmanCode>>,
    total_out: usize,
    ended: bool,
    next: Box<Stage<W>>,
}

impl<W: Write> BrotliStage<W> {
    fn feed(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.ended {
            return if bytes.is_empty() {
                Ok(())
            } else {
                Err(invalid_data())
            };
        }
        let mut available_in = bytes.len();
        let mut offset = 0;
        loop {
            let mut output = [0; OUTPUT_BLOCK];
            let mut available_out = output.len();
            let mut produced = 0;
            let before = offset;
            let status = BrotliDecompressStream(
                &mut available_in,
                &mut offset,
                bytes,
                &mut available_out,
                &mut produced,
                &mut output,
                &mut self.total_out,
                &mut self.decoder,
            );
            self.next.write_all(&output[..produced])?;
            match status {
                BrotliResult::ResultSuccess => {
                    self.ended = true;
                    return if available_in == 0 {
                        Ok(())
                    } else {
                        Err(invalid_data())
                    };
                }
                BrotliResult::ResultFailure => return Err(invalid_data()),
                BrotliResult::NeedsMoreInput => {
                    return if available_in == 0 {
                        Ok(())
                    } else {
                        Err(invalid_data())
                    };
                }
                BrotliResult::NeedsMoreOutput => {
                    if before == offset && produced == 0 {
                        return Err(invalid_data());
                    }
                }
            }
        }
    }
}

struct ZstdStage<W: Write> {
    decoder: Decoder<'static>,
    at_boundary: bool,
    next: Box<Stage<W>>,
}

impl<W: Write> ZstdStage<W> {
    fn feed(&mut self, bytes: &[u8]) -> io::Result<()> {
        // 空输入不能把已经完整的 frame 边界误记成“下一 frame 被截断”。
        if bytes.is_empty() && self.at_boundary {
            return Ok(());
        }
        let mut offset = 0;
        loop {
            let mut output = [0; OUTPUT_BLOCK];
            let status = self.decoder.run_on_buffers(&bytes[offset..], &mut output)?;
            offset += status.bytes_read;
            self.next.write_all(&output[..status.bytes_written])?;
            self.at_boundary = status.remaining == 0;
            if self.at_boundary {
                if offset == bytes.len() {
                    return Ok(());
                }
            } else if status.bytes_read == 0 && status.bytes_written == 0 {
                return if offset == bytes.len() {
                    Ok(())
                } else {
                    Err(invalid_data())
                };
            } else if offset == bytes.len() && status.bytes_written < OUTPUT_BLOCK {
                return Ok(());
            }
        }
    }
}

fn invalid_data() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "响应统计解码未完整结束")
}

#[cfg(test)]
mod tests {
    use super::{ResponseBodyDecoder, ResponseDecodeError};
    use brotli::enc::BrotliEncoderParams;
    use brotli::{BrotliCompress, CompressorReader};
    use flate2::Compression;
    use flate2::read::DeflateDecoder;
    use flate2::write::{DeflateEncoder, GzEncoder, ZlibEncoder};
    use sha2::{Digest, Sha256};
    use std::io::{self, Read, Write};
    use zstd::stream::encode_all as zstd_encode_all;

    #[derive(Default)]
    struct Counter {
        length: usize,
        max_write: usize,
        hash: Sha256,
    }

    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.length += bytes.len();
            self.max_write = self.max_write.max(bytes.len());
            self.hash.update(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn encode(source: &[u8], coding: &str) -> Vec<u8> {
        match coding {
            "gzip" | "x-gzip" => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(source).unwrap();
                encoder.finish().unwrap()
            }
            "deflate" => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(source).unwrap();
                encoder.finish().unwrap()
            }
            "raw" => {
                let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(source).unwrap();
                encoder.finish().unwrap()
            }
            "br" => {
                let mut encoder = CompressorReader::new(source, 4096, 4, 22);
                let mut output = Vec::new();
                encoder.read_to_end(&mut output).unwrap();
                output
            }
            "zstd" => zstd_encode_all(source, 1).unwrap(),
            _ => source.to_vec(),
        }
    }

    fn decoder(codings: &[&str]) -> ResponseBodyDecoder<Counter> {
        let mut decoder = ResponseBodyDecoder::new(Counter::default());
        decoder
            .configure(
                &codings
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        decoder
    }

    #[test]
    fn all_codecs_stream_long_output_in_bounded_writes() {
        let source = b"synthetic-response-payload-0123456789\n".repeat(65536);
        for coding in ["identity", "gzip", "x-gzip", "deflate", "raw", "br", "zstd"] {
            let encoded = encode(&source, coding);
            let header = if coding == "raw" { "deflate" } else { coding };
            for chunk in [1, 7, 1024, encoded.len()] {
                let mut decoder = decoder(&[header]);
                for part in encoded.chunks(chunk) {
                    decoder.feed(part).unwrap();
                }
                assert_eq!(
                    decoder.sink_mut().unwrap().length,
                    source.len(),
                    "{coding}/{chunk} 结束前输出"
                );
                decoder.finish().unwrap();
                let sink = decoder.take_sink().unwrap();
                assert_eq!(
                    sink.hash.finalize(),
                    Sha256::digest(&source),
                    "{coding}/{chunk}"
                );
                assert!(sink.max_write <= 32 * 1024, "{coding} 出口不得聚合整个响应");
                assert!(decoder.take_sink().is_none());
            }
        }
    }

    #[test]
    fn coding_chain_streams_in_reverse_order() {
        let source = b"layered-synthetic-data\n".repeat(65536);
        let codings = ["gzip", "br", "deflate", "zstd"];
        let mut encoded = source.clone();
        for coding in codings {
            encoded = encode(&encoded, coding);
        }
        let mut decoder = decoder(&codings);
        for part in encoded.chunks(7) {
            decoder.feed(part).unwrap();
        }
        decoder.finish().unwrap();
        let sink = decoder.take_sink().unwrap();
        assert_eq!(sink.length, source.len());
        assert_eq!(sink.hash.finalize(), Sha256::digest(&source));
        assert!(sink.max_write <= 32 * 1024);
    }

    #[test]
    fn truncated_and_trailing_streams_never_finish_successfully() {
        let source = b"synthetic-data\n".repeat(16384);
        for coding in ["gzip", "deflate", "raw", "br", "zstd"] {
            let encoded = encode(&source, coding);
            let header = if coding == "raw" { "deflate" } else { coding };
            let mut trailing = encoded.clone();
            trailing.extend_from_slice(b"invalid-tail");
            let mut bad_checksum = encoded.clone();
            if coding == "gzip" {
                let index = bad_checksum.len() - 8;
                bad_checksum[index] ^= 1;
            } else if coding == "deflate" {
                let index = bad_checksum.len() - 1;
                bad_checksum[index] ^= 1;
            }
            let mut invalid = vec![encoded[..encoded.len() - 1].to_vec(), trailing];
            if coding == "gzip" || coding == "deflate" {
                invalid.push(bad_checksum);
            }
            for wire in invalid {
                let mut decoder = decoder(&[header]);
                let result = wire.chunks(7).try_for_each(|part| decoder.feed(part));
                assert!(
                    result.and_then(|()| decoder.finish()).is_err(),
                    "{coding} 不完整容器"
                );
                assert!(decoder.take_sink().is_some(), "错误也必须保留已有摘要");
            }
        }
    }

    #[test]
    fn gzip_members_and_zstd_frames_validate_every_part() {
        let source = b"synthetic-member";
        for coding in ["gzip", "zstd"] {
            let member = encode(source, coding);
            let mut wire = member.clone();
            if coding == "zstd" {
                wire.extend_from_slice(&[0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 1, 2, 3]);
            }
            wire.extend_from_slice(&member);
            let mut observer = decoder(&[coding]);
            for part in wire.chunks(1) {
                observer.feed(part).unwrap();
            }
            observer.finish().unwrap();
            let sink = observer.take_sink().unwrap();
            assert_eq!(sink.length, source.len() * 2);
            let mut hash = Sha256::new();
            hash.update(source);
            hash.update(source);
            assert_eq!(sink.hash.finalize(), hash.finalize());
            wire.pop();
            let mut observer = decoder(&[coding]);
            let result = observer.feed(&wire);
            assert!(result.and_then(|()| observer.finish()).is_err());
            assert!(observer.take_sink().unwrap().length >= source.len());
        }
        for coding in ["deflate", "raw", "br"] {
            let member = encode(source, coding);
            let mut observer = decoder(&[if coding == "raw" { "deflate" } else { coding }]);
            assert!(
                observer
                    .feed(&[member.as_slice(), member.as_slice()].concat())
                    .is_err()
            );
        }
    }

    #[test]
    fn coding_limits_fail_only_the_observer_and_keep_its_sink() {
        let mut observer = ResponseBodyDecoder::new(Counter::default());
        assert_eq!(
            observer.configure(&vec!["gzip".into(); 5]),
            Err(ResponseDecodeError::SamplingLimit)
        );
        assert_eq!(
            observer.feed(b"ignored"),
            Err(ResponseDecodeError::SamplingLimit)
        );
        assert_eq!(observer.take_sink().unwrap().length, 0);
        let mut observer = ResponseBodyDecoder::new(Counter::default());
        assert_eq!(
            observer.configure(&["unknown".into()]),
            Err(ResponseDecodeError::InvalidData)
        );
        assert!(observer.take_sink().is_some());
        let mut observer = decoder(&["identity"]);
        observer.feed(b"known").unwrap();
        assert_eq!(
            observer.configure(&["gzip".into()]),
            Err(ResponseDecodeError::InvalidData)
        );
        assert_eq!(observer.take_sink().unwrap().length, 5);
    }

    #[test]
    fn strict_windows_apply_to_first_and_later_frames() {
        let high = [0x28, 0xb5, 0x2f, 0xfd, 0, 0x78, 0x11, 0, 0, b'o', b'k'];
        let mut normal = high;
        normal[5] = 0x70;
        let mut observer = decoder(&["zstd"]);
        observer.feed(&normal).unwrap();
        observer.finish().unwrap();
        assert_eq!(observer.take_sink().unwrap().length, 2);
        for wire in [high.to_vec(), [normal.as_slice(), high.as_slice()].concat()] {
            let mut observer = decoder(&["zstd"]);
            assert!(observer.feed(&wire).is_err());
        }
        for (lgwin, large_window) in [(24, false), (25, true)] {
            let params = BrotliEncoderParams {
                quality: 4,
                lgwin,
                large_window,
                ..Default::default()
            };
            let mut source = b"synthetic-window".as_slice();
            let mut wire = Vec::new();
            BrotliCompress(&mut source, &mut wire, &params).unwrap();
            let mut observer = decoder(&["br"]);
            let result = observer.feed(&wire).and_then(|()| observer.finish());
            assert_eq!(result.is_err(), large_window);
        }
    }

    #[test]
    fn ambiguous_raw_prefix_is_conservatively_incomplete() {
        let mut wire = vec![0x78, 0x9c, 0x00, 0x63, 0xff];
        wire.extend_from_slice(&[b'A'; 156]);
        wire.extend_from_slice(&[1, 0, 0, 0xff, 0xff]);
        let mut raw = Vec::new();
        DeflateDecoder::new(wire.as_slice())
            .read_to_end(&mut raw)
            .unwrap();
        assert_eq!(raw, vec![b'A'; 156]);
        let mut observer = decoder(&["deflate"]);
        let result = observer.feed(&wire).and_then(|()| observer.finish());
        assert_eq!(result, Err(ResponseDecodeError::InvalidData));
        assert!(observer.take_sink().is_some());
    }
}
