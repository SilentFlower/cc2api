use std::io::{self, Read};
use std::sync::Arc;

use brotli::Decompressor;
use chrono::{DateTime, Utc};
use flate2::read::{DeflateDecoder, GzDecoder, ZlibDecoder};
use serde_json::Value;
use tokio::runtime::Handle;
use tracing::warn;
use uuid::Uuid;
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::service::telemetry::{MessageTelemetryUsage, merge_message_usage};
use crate::service::usage_pricing::calculate_cost;
use crate::store::usage_store::{UsageRecord, UsageStore};

/// 单行 SSE 或完整 JSON 的采样上限，防止正文使旁路统计无限占用内存。
pub(crate) const USAGE_BUFFER_LIMIT: usize = 1024 * 1024;

/// 一次真实调用的用量观察器；只保存临时解析缓冲与安全摘要。
pub struct UsageCapture {
    store: Option<Arc<UsageStore>>,
    record: UsageRecord,
    speed: String,
    buffer: Vec<u8>,
    is_sse: Option<bool>,
    discard_line: bool,
    saw_input: bool,
    saw_output: bool,
    saw_end: bool,
    response_model: Option<String>,
    message_id: Option<String>,
    stop_reason: Option<String>,
    error_type: Option<String>,
    observation_error: Option<&'static str>,
    tokens: [Option<u64>; 6],
}

/// 与计费无关的响应观察结果，只保留白名单元信息。
pub(crate) struct MessageObservation {
    pub response_model: Option<String>,
    pub message_id: Option<String>,
    pub stop_reason: Option<String>,
    pub error_type: Option<String>,
    pub observation_error: Option<&'static str>,
    pub tokens: [Option<u64>; 6],
    pub partial: bool,
}

impl UsageCapture {
    /// 为已准备转发的正式 messages 调用建立独立观察器。
    /// @param store 共享统计存储。
    /// @param account_id 实际承载账号。
    /// @param request 最终上游请求，仅读取模型与计价模式。
    /// @param started_at 该次上游调用开始时间。
    /// @return 尚未观察响应的采集器。
    pub fn new(
        store: Arc<UsageStore>,
        account_id: i64,
        request: &Value,
        started_at: DateTime<Utc>,
    ) -> Self {
        Self::build(Some(store), account_id, request, started_at)
    }

    /// 创建只解析响应、不写计费表的诊断观察器。
    /// @param request 实际上游请求的临时参数。
    /// @return 独立的有界响应观察器。
    pub(crate) fn observer(request: &Value) -> Self {
        Self::build(None, 0, request, Utc::now())
    }

    fn build(
        store: Option<Arc<UsageStore>>,
        account_id: i64,
        request: &Value,
        started_at: DateTime<Utc>,
    ) -> Self {
        Self {
            store,
            record: UsageRecord {
                record_id: Uuid::new_v4().to_string(),
                account_id,
                started_at,
                model: request["model"].as_str().unwrap_or("").to_owned(),
                usage: MessageTelemetryUsage::default(),
                cost_usd: None,
                partial: false,
            },
            speed: request["speed"].as_str().unwrap_or("").to_owned(),
            buffer: Vec::new(),
            is_sse: None,
            discard_line: false,
            saw_input: false,
            saw_output: false,
            saw_end: false,
            response_model: None,
            message_id: None,
            stop_reason: None,
            error_type: None,
            observation_error: None,
            tokens: [None; 6],
        }
    }

    /// 消费原始未压缩响应片段，兼容跨帧 SSE 和非流式 JSON。
    /// @param bytes 当前响应片段，不保存到数据库。
    /// @return 无返回值，更新本次调用摘要。
    pub fn observe(&mut self, bytes: &[u8]) {
        if self.is_sse.is_none() {
            self.is_sse = bytes
                .iter()
                .find(|byte| !byte.is_ascii_whitespace())
                .map(|byte| *byte != b'{');
        }
        if self.is_sse != Some(true) {
            if self.buffer.len().saturating_add(bytes.len()) > USAGE_BUFFER_LIMIT {
                self.mark_partial();
                self.discard_line = true;
                self.buffer.clear();
            } else if !self.discard_line {
                self.buffer.extend_from_slice(bytes);
            }
            return;
        }
        for part in bytes.split_inclusive(|byte| *byte == b'\n') {
            if self.buffer.len().saturating_add(part.len()) > USAGE_BUFFER_LIMIT {
                self.mark_partial();
                self.discard_line = true;
                self.buffer.clear();
            }
            if !self.discard_line {
                self.buffer.extend_from_slice(part);
            }
            if part.last() == Some(&b'\n') {
                if !self.discard_line {
                    self.parse_line();
                }
                self.discard_line = false;
                self.buffer.clear();
            }
        }
    }

    /// 标记解码失败或采样超限，不把缺失数据记成精确零费用。
    /// @return 无返回值。
    pub fn mark_partial(&mut self) {
        self.mark_observation_error("sampling_limit");
    }

    fn mark_observation_error(&mut self, reason: &'static str) {
        self.record.partial = true;
        self.observation_error.get_or_insert(reason);
    }

    /// 限长解码压缩旁路副本，原始响应继续由网关直接转发。
    /// @param bytes 已有旁路采样字节。
    /// @param codings 响应声明的内容编码。
    /// @return 无返回值；不支持、损坏或超限时标记为部分数据。
    pub fn observe_encoded(&mut self, bytes: &[u8], codings: &[String]) {
        match decode_usage_body(bytes, codings) {
            Ok(decoded) => self.observe(&decoded),
            Err(_) => self.mark_observation_error("decode_error"),
        }
    }

    fn parse_line(&mut self) {
        let line = self
            .buffer
            .strip_prefix(b"data:")
            .map(|line| line.trim_ascii());
        if let Some(line) = line {
            if line == b"[DONE]" {
                self.saw_end = true;
            } else if let Ok(value) = serde_json::from_slice::<Value>(line) {
                self.observe_value(&value);
            } else {
                self.mark_observation_error("invalid_payload");
            }
        }
    }

    fn observe_value(&mut self, value: &Value) {
        if let Some(model) = value
            .get("model")
            .or_else(|| value.pointer("/message/model"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            self.record.model = model.to_owned();
            self.response_model = Some(model.to_owned());
        }
        if let Some(id) = value
            .get("id")
            .or_else(|| value.pointer("/message/id"))
            .and_then(Value::as_str)
        {
            // ID 只在临时观察器中存在，持久化前由诊断层散列。
            if id.len() <= 256 {
                self.message_id = Some(id.to_owned());
            }
        }
        if let Some(reason) = value
            .get("stop_reason")
            .or_else(|| value.pointer("/delta/stop_reason"))
            .and_then(Value::as_str)
        {
            if [
                "end_turn",
                "max_tokens",
                "stop_sequence",
                "tool_use",
                "pause_turn",
                "refusal",
                "model_context_window_exceeded",
            ]
            .contains(&reason)
            {
                self.stop_reason = Some(reason.to_owned());
            }
        }
        if value["type"].as_str() == Some("error") {
            self.error_type = Some(
                match value.pointer("/error/type").and_then(Value::as_str) {
                    Some(
                        kind @ ("invalid_request_error"
                        | "authentication_error"
                        | "permission_error"
                        | "not_found_error"
                        | "rate_limit_error"
                        | "api_error"
                        | "overloaded_error"
                        | "billing_error"
                        | "timeout_error"),
                    ) => kind,
                    _ => "unknown_upstream_error",
                }
                .to_owned(),
            );
        }
        if value["type"].as_str() == Some("message_stop") {
            self.saw_end = true;
        }
        for usage in [
            value.get("usage"),
            value.pointer("/message/usage"),
            value.pointer("/delta/usage"),
        ]
        .into_iter()
        .flatten()
        {
            if !usage.is_object() {
                self.mark_observation_error("invalid_payload");
                continue;
            }
            self.saw_input |= usage["input_tokens"].as_u64().is_some();
            self.saw_output |= usage["output_tokens"].as_u64().is_some();
            for field in [
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_creation_input_tokens",
            ] {
                if usage
                    .get(field)
                    .is_some_and(|value| value.as_u64().is_none())
                {
                    self.mark_observation_error("invalid_payload");
                }
            }
            for field in ["ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens"] {
                if usage
                    .get("cache_creation")
                    .and_then(|value| value.get(field))
                    .is_some_and(|value| value.as_u64().is_none())
                {
                    self.mark_observation_error("invalid_payload");
                }
            }
            merge_message_usage(&mut self.record.usage, usage);
            for (index, field) in [
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_creation_input_tokens",
            ]
            .iter()
            .enumerate()
            {
                if let Some(value) = usage.get(*field).and_then(Value::as_u64) {
                    self.tokens[index] = Some(self.tokens[index].unwrap_or(0).max(value));
                }
            }
            for (index, field) in ["ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens"]
                .iter()
                .enumerate()
            {
                if let Some(value) = usage
                    .get("cache_creation")
                    .and_then(|v| v.get(*field))
                    .and_then(Value::as_u64)
                {
                    self.tokens[index + 4] = Some(self.tokens[index + 4].unwrap_or(0).max(value));
                }
            }
            if let Some(speed) = usage
                .get("speed")
                .or_else(|| usage.get("service_tier"))
                .and_then(Value::as_str)
            {
                self.speed = speed.to_owned();
            }
        }
    }

    fn complete_observation(&mut self, complete: bool) {
        if self.is_sse == Some(true) {
            if !self.buffer.is_empty() && !self.discard_line {
                self.parse_line();
            }
        } else if !self.discard_line {
            match serde_json::from_slice::<Value>(&self.buffer) {
                Ok(value) => self.observe_value(&value),
                Err(_) => self.mark_observation_error("invalid_payload"),
            }
        }
        self.record.partial |= !complete
            || !self.saw_input
            || !self.saw_output
            || (self.is_sse == Some(true) && !self.saw_end);
        if self.store.is_some() && (self.saw_input || self.saw_output) {
            self.record.cost_usd =
                calculate_cost(&self.record.model, &self.speed, self.record.usage);
        }
    }

    fn into_record(mut self, complete: bool) -> UsageRecord {
        self.complete_observation(complete);
        self.record
    }

    /// 结束诊断旁路解析，不触发费用写入。
    /// @param complete 是否读到正常的传输结束。
    /// @return 已观察到的元信息与部分数据标记。
    pub(crate) fn observation(mut self, complete: bool) -> MessageObservation {
        self.complete_observation(complete);
        MessageObservation {
            response_model: self.response_model,
            message_id: self.message_id,
            stop_reason: self.stop_reason,
            error_type: self.error_type,
            observation_error: self.observation_error,
            tokens: self.tokens,
            partial: self.record.partial,
        }
    }

    /// 完成本次观察并异步落库；由调用方 take 保证只执行一次。
    /// @param complete 是否正常读到响应结束。
    /// @return 无返回值，不阻塞响应转发。
    pub fn finish(self, complete: bool) {
        let store = self.store.clone();
        let record = self.into_record(complete);
        if let (Some(store), Ok(runtime)) = (store, Handle::try_current()) {
            runtime.spawn(async move {
                if let Err(error) = store.record(&record).await {
                    warn!(
                        account_id = record.account_id,
                        "网关用量记录失败: {}", error
                    );
                }
            });
        }
    }
}

fn read_limited(reader: impl Read) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(USAGE_BUFFER_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > USAGE_BUFFER_LIMIT {
        return Err(io::Error::other("用量解码超过采样上限"));
    }
    Ok(bytes)
}

fn decode_usage_body(bytes: &[u8], codings: &[String]) -> io::Result<Vec<u8>> {
    if bytes.len() > USAGE_BUFFER_LIMIT {
        return Err(io::Error::other("用量采样超过上限"));
    }
    let mut decoded = bytes.to_vec();
    for coding in codings.iter().rev() {
        decoded = match coding.as_str() {
            "gzip" | "x-gzip" => read_limited(GzDecoder::new(decoded.as_slice()))?,
            "deflate" => read_limited(ZlibDecoder::new(decoded.as_slice()))
                .or_else(|_| read_limited(DeflateDecoder::new(decoded.as_slice())))?,
            "br" => read_limited(Decompressor::new(decoded.as_slice(), 4096))?,
            "zstd" => read_limited(ZstdDecoder::new(decoded.as_slice())?)?,
            "identity" => decoded,
            _ => return Err(io::Error::other("不支持的用量内容编码")),
        };
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use serde_json::json;
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    use std::io::Write;

    fn test_capture() -> UsageCapture {
        install_default_drivers();
        let pool = AnyPoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        UsageCapture::new(
            Arc::new(UsageStore::new(pool)),
            1,
            &json!({"model":"claude-sonnet-5"}),
            Utc::now(),
        )
    }

    #[tokio::test]
    async fn split_sse_merges_cumulative_usage_without_double_counting() {
        let source = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-fable-5-1\",\"usage\":{\"input_tokens\":1000,\"output_tokens\":0,\"cache_read_input_tokens\":500}}}\n\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"合成内容\"}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":100}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":100}}\n\ndata: {\"type\":\"message_stop\"}\n\n";
        for size in [1, 7, 29, source.len()] {
            let mut capture = test_capture();
            for part in source.as_bytes().chunks(size) {
                capture.observe(part);
            }
            let record = capture.into_record(true);
            assert_eq!(record.usage.input_tokens, 1000);
            assert_eq!(record.usage.output_tokens, 100);
            assert_eq!(record.usage.cache_read_input_tokens, 500);
            assert!(!record.partial);
            assert!((record.cost_usd.unwrap() - 0.015125).abs() < 1e-10);
        }
    }

    #[tokio::test]
    async fn buffered_json_uses_actual_response_model_and_speed() {
        let mut capture = test_capture();
        capture.speed = "fast".into();
        let response=br#"{"type":"message","model":"claude-opus-5-5","usage":{"input_tokens":1000000,"output_tokens":0,"speed":"standard"}}"#;
        for part in response.chunks(11) {
            capture.observe(part);
        }
        let record = capture.into_record(true);
        assert_eq!(record.cost_usd, Some(4.0));
        assert!(!record.partial);
    }

    #[tokio::test]
    async fn compressed_usage_is_bounded_and_never_turns_missing_data_into_zero_cost() {
        let response = br#"{"usage":{"input_tokens":1000000,"output_tokens":0}}"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(response).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut capture = test_capture();
        capture.observe_encoded(&compressed, &["gzip".into()]);
        let record = capture.into_record(true);
        assert_eq!(record.cost_usd, Some(3.0));
        assert!(!record.partial);

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&vec![b' '; USAGE_BUFFER_LIMIT + 1])
            .unwrap();
        let compressed = encoder.finish().unwrap();
        let mut capture = test_capture();
        capture.observe_encoded(&compressed, &["gzip".into()]);
        let record = capture.into_record(true);
        assert!(record.partial);
        assert!(record.cost_usd.is_none());
    }

    #[tokio::test]
    async fn interrupted_and_malformed_responses_keep_only_observed_usage() {
        let mut partial = test_capture();
        partial.observe(b"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":100,\"output_tokens\":0}}}\n\n");
        let record = partial.into_record(false);
        assert!(record.partial);
        assert_eq!(record.usage.input_tokens, 100);
        assert!(record.cost_usd.unwrap() > 0.0);

        let mut invalid = test_capture();
        invalid.observe(br#"{"usage":{"input_tokens":-100,"output_tokens":2}}"#);
        assert!(invalid.into_record(true).partial);
        let mut unknown = test_capture();
        unknown.observe(
            br#"{"model":"claude-unknown","usage":{"input_tokens":100,"output_tokens":2}}"#,
        );
        assert!(unknown.into_record(true).cost_usd.is_none());
        assert!(test_capture().into_record(true).cost_usd.is_none());
    }
}
