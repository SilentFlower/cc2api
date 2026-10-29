use std::io::{self, Write};
use std::mem::take;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::runtime::Handle;
use tracing::warn;
use uuid::Uuid;

use crate::service::response_body_codec::{ResponseBodyDecoder, ResponseDecodeError};
use crate::service::telemetry::{MessageTelemetryUsage, merge_message_usage};
use crate::service::usage_pricing::calculate_cost;
use crate::store::usage_store::{UsageRecord, UsageStore};

/// 单行、单个 SSE data 事件或完整 JSON 的采样上限，不限制 SSE 累计长度。
pub(crate) const USAGE_BUFFER_LIMIT: usize = 1024 * 1024;

/// 一次真实调用的用量观察器；只保存有界解析状态与安全摘要。
pub struct UsageCapture {
    decoder: ResponseBodyDecoder<UsageParser>,
}

/// 解码链末端拥有解析状态，避免跨块借用或自引用回调。
struct UsageParser {
    store: Option<Arc<UsageStore>>,
    record: UsageRecord,
    speed: String,
    buffer: Vec<u8>,
    is_sse: Option<bool>,
    prefix: Vec<u8>,
    bom_checked: bool,
    event_data: Vec<u8>,
    event_has_data: bool,
    discard_line: bool,
    discard_event: bool,
    skip_lf: bool,
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
            decoder: ResponseBodyDecoder::new(UsageParser::build(
                store, account_id, request, started_at,
            )),
        }
    }

    /// 首块响应之前配置编码，原始网络响应继续由已有 Body 转发。
    /// @param codings 既有响应头解析得到的有序编码列表。
    /// @return 无返回值；配置失败只标记观察缺口。
    pub(crate) fn configure_codings(&mut self, codings: &[String]) {
        if let Err(error) = self.decoder.configure(codings) {
            self.decode_failed(error);
        }
    }

    /// 增量消费原始响应片段；未配置编码时兼容明文调用。
    /// @param bytes 当前原始响应片段，不持久化正文。
    /// @return 无返回值，更新本次调用摘要。
    pub fn observe(&mut self, bytes: &[u8]) {
        if let Err(error) = self.decoder.feed(bytes) {
            self.decode_failed(error);
        }
    }

    /// 标记采样超限，不把缺失数据记成精确零费用。
    /// @return 无返回值。
    pub fn mark_partial(&mut self) {
        if let Some(parser) = self.decoder.sink_mut() {
            parser.mark_observation_error("sampling_limit");
        }
    }

    fn decode_failed(&mut self, error: ResponseDecodeError) {
        if let Some(parser) = self.decoder.sink_mut() {
            parser.mark_observation_error(match error {
                ResponseDecodeError::InvalidData => "decode_error",
                ResponseDecodeError::SamplingLimit => "sampling_limit",
            });
        }
    }

    /// 复用既有缓冲分支的原始字节，仍使用同一增量观察及结束校验。
    /// @param bytes 既有分支已读取的响应，不再额外读网络。
    /// @param codings 响应声明的内容编码。
    /// @return 无返回值；不支持、损坏或超限只影响统计完整性。
    pub fn observe_encoded(&mut self, bytes: &[u8], codings: &[String]) {
        self.configure_codings(codings);
        self.observe(bytes);
    }

    fn completed_parser(mut self, complete: bool) -> UsageParser {
        if let Err(error) = self.decoder.finish() {
            self.decode_failed(error);
        }
        // 先取走末端状态，codec 的隐式 Drop 即使 flush 也不能再次修改摘要。
        let mut parser = self.decoder.take_sink().expect("响应观察器仅终结一次");
        parser.complete_observation(complete);
        parser
    }

    fn into_record(self, complete: bool) -> UsageRecord {
        self.completed_parser(complete).record
    }

    /// 结束诊断旁路解析，不触发费用写入。
    /// @param complete 是否读到正常的传输结束。
    /// @return 已观察到的元信息与部分数据标记。
    pub(crate) fn observation(self, complete: bool) -> MessageObservation {
        let parser = self.completed_parser(complete);
        MessageObservation {
            response_model: parser.response_model,
            message_id: parser.message_id,
            stop_reason: parser.stop_reason,
            error_type: parser.error_type,
            observation_error: parser.observation_error,
            tokens: parser.tokens,
            partial: parser.record.partial,
        }
    }

    /// 完成本次观察并异步落库；由调用方 take 保证只执行一次。
    /// @param complete 是否正常读到响应结束。
    /// @return 无返回值，不等待数据库写入。
    pub fn finish(mut self, complete: bool) {
        let store = self
            .decoder
            .sink_mut()
            .and_then(|parser| parser.store.clone());
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

impl UsageParser {
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
            prefix: Vec::with_capacity(3),
            bom_checked: false,
            event_data: Vec::new(),
            event_has_data: false,
            discard_line: false,
            discard_event: false,
            skip_lf: false,
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

    fn observe(&mut self, mut bytes: &[u8]) {
        while !self.bom_checked && !bytes.is_empty() {
            self.prefix.push(bytes[0]);
            bytes = &bytes[1..];
            if b"\xef\xbb\xbf".starts_with(&self.prefix) {
                if self.prefix.len() < 3 {
                    continue;
                }
                self.prefix.clear();
            } else {
                let prefix = take(&mut self.prefix);
                self.observe_content(&prefix);
            }
            self.bom_checked = true;
        }
        self.observe_content(bytes);
    }

    fn observe_content(&mut self, bytes: &[u8]) {
        if self.is_sse.is_none() {
            self.is_sse = bytes
                .iter()
                .find(|byte| !byte.is_ascii_whitespace())
                .map(|byte| *byte != b'{');
            if self.is_sse == Some(true) {
                let prefix = take(&mut self.buffer);
                self.observe_sse(&prefix);
            }
        }
        if self.is_sse == Some(true) {
            self.observe_sse(bytes);
        } else if !self.discard_line {
            if self.buffer.len().saturating_add(bytes.len()) > USAGE_BUFFER_LIMIT {
                self.mark_observation_error("sampling_limit");
                self.discard_line = true;
                self.buffer.clear();
            } else {
                extend_bounded(&mut self.buffer, bytes);
            }
        }
    }

    fn observe_sse(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if byte == b'\n' || byte == b'\r' {
                if !self.discard_line {
                    let mut line = take(&mut self.buffer);
                    self.parse_sse_line(&line);
                    line.clear();
                    self.buffer = line;
                }
                self.discard_line = false;
                self.skip_lf = byte == b'\r';
            } else if !self.discard_line {
                if self.buffer.len() == USAGE_BUFFER_LIMIT {
                    self.mark_observation_error("sampling_limit");
                    self.buffer.clear();
                    self.event_data.clear();
                    self.discard_line = true;
                    self.discard_event = true;
                } else {
                    extend_bounded(&mut self.buffer, &[byte]);
                }
            }
        }
    }

    fn parse_sse_line(&mut self, line: &[u8]) {
        if line.is_empty() {
            if self.event_has_data && !self.discard_event {
                let data = take(&mut self.event_data);
                if data.trim_ascii() == b"[DONE]" {
                    self.saw_end = true;
                } else if let Ok(value) = serde_json::from_slice::<Value>(&data) {
                    self.observe_value(&value);
                } else {
                    self.mark_observation_error("invalid_payload");
                }
                self.event_data = data;
            }
            self.event_data.clear();
            self.event_has_data = false;
            self.discard_event = false;
            return;
        }
        if self.discard_event || line.starts_with(b":") {
            return;
        }
        let (field, value) = match line.iter().position(|byte| *byte == b':') {
            Some(index) => {
                let value = &line[index + 1..];
                (&line[..index], value.strip_prefix(b" ").unwrap_or(value))
            }
            None => (line, &b""[..]),
        };
        if field != b"data" {
            return;
        }
        let separator = usize::from(self.event_has_data);
        if self
            .event_data
            .len()
            .saturating_add(separator)
            .saturating_add(value.len())
            > USAGE_BUFFER_LIMIT
        {
            self.mark_observation_error("sampling_limit");
            self.event_data.clear();
            self.discard_event = true;
            return;
        }
        if self.event_has_data {
            extend_bounded(&mut self.event_data, b"\n");
        }
        extend_bounded(&mut self.event_data, value);
        self.event_has_data = true;
    }

    fn mark_observation_error(&mut self, reason: &'static str) {
        self.record.partial = true;
        self.observation_error.get_or_insert(reason);
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
        if !self.bom_checked && !self.prefix.is_empty() {
            let prefix = take(&mut self.prefix);
            self.observe_content(&prefix);
        }
        if self.is_sse == Some(true) {
            // EOF 不派发残缺事件；已经出现的 message_stop 也不能证明尾部 data 完整。
            let pending_data = self.buffer == b"data"
                || self.buffer.starts_with(b"data:")
                || self.event_has_data
                || self.discard_line
                || self.discard_event;
            self.record.partial |= pending_data;
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
}

impl Write for UsageParser {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.observe(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn extend_bounded(buffer: &mut Vec<u8>, bytes: &[u8]) {
    let needed = buffer.len() + bytes.len();
    if needed > buffer.capacity() {
        // Vec 默认倍增可能越过业务上限；保留几何增长但把申请容量封顶。
        let capacity = buffer
            .capacity()
            .saturating_mul(2)
            .max(needed)
            .min(USAGE_BUFFER_LIMIT);
        buffer.reserve_exact(capacity - buffer.len());
    }
    buffer.extend_from_slice(bytes);
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
        capture.decoder.sink_mut().unwrap().speed = "fast".into();
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

    fn events(multiline: bool, newline: &str) -> Vec<u8> {
        let values = [
            json!({"type":"message_start","message":{"id":"msg_synthetic","model":"claude-sonnet-5","usage":{"input_tokens":1000,"output_tokens":0,"cache_read_input_tokens":500,"cache_creation_input_tokens":60,"cache_creation":{"ephemeral_5m_input_tokens":40,"ephemeral_1h_input_tokens":20}}}}),
            json!({"type":"future_synthetic_event","data":"合成正文秘密"}),
            json!({"type":"message_delta","usage":{"output_tokens":100},"delta":{"stop_reason":"end_turn"}}),
            json!({"type":"message_delta","usage":{"output_tokens":99}}),
            json!({"type":"message_stop"}),
        ];
        let mut result = String::from(
            "\u{feff}: 合成注释\n\nid: ignored\nretry: 100\nData: {\"usage\":{\"input_tokens\":9999}}\n\n",
        );
        for value in values {
            let value = if multiline {
                serde_json::to_string_pretty(&value).unwrap()
            } else {
                value.to_string()
            };
            result.push_str("event: synthetic\n");
            for line in value.lines() {
                result.push_str("data: ");
                result.push_str(line);
                result.push('\n');
            }
            result.push('\n');
        }
        result.replace('\n', newline).into_bytes()
    }

    fn observe_parts(source: &[u8], size: usize) -> MessageObservation {
        let mut capture = UsageCapture::observer(&json!({"model":"claude-sonnet-5"}));
        for bytes in source.chunks(size) {
            capture.observe(bytes);
            let parser = capture.decoder.sink_mut().unwrap();
            assert!(parser.buffer.capacity() <= USAGE_BUFFER_LIMIT);
            assert!(parser.event_data.capacity() <= USAGE_BUFFER_LIMIT);
        }
        capture.observation(true)
    }

    #[test]
    fn sse_events_follow_all_line_endings_and_merge_multiple_data_fields() {
        for newline in ["\n", "\r\n", "\r"] {
            for multiline in [false, true] {
                let source = events(multiline, newline);
                for size in [1, 2, 7, 29, source.len()] {
                    let observed = observe_parts(&source, size);
                    assert_eq!(
                        observed.tokens,
                        [
                            Some(1000),
                            Some(100),
                            Some(500),
                            Some(60),
                            Some(40),
                            Some(20)
                        ]
                    );
                    assert_eq!(observed.response_model.as_deref(), Some("claude-sonnet-5"));
                    assert_eq!(observed.message_id.as_deref(), Some("msg_synthetic"));
                    assert_eq!(observed.stop_reason.as_deref(), Some("end_turn"));
                    assert!(!observed.partial, "{newline:?}/{multiline}/{size}");
                    assert!(observed.observation_error.is_none());
                }
            }
        }
    }

    #[test]
    fn eof_does_not_dispatch_unterminated_data_or_hide_it_after_message_stop() {
        let source = events(false, "\n");
        let pending = b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":9999}}\n";
        for bytes in [
            pending.to_vec(),
            [source.as_slice(), pending].concat(),
            source[..source.len() - 1].to_vec(),
        ] {
            let observed = observe_parts(&bytes, 1);
            assert!(observed.partial);
            assert!(observed.tokens[1].is_none_or(|value| value == 100));
        }
        let observed = observe_parts(&[source.as_slice(), b": unfinished comment"].concat(), 7);
        assert!(!observed.partial);
        assert_eq!(observed.tokens[1], Some(100));
    }

    #[test]
    fn parser_bounds_json_lines_and_combined_events_then_resynchronizes() {
        let prefix = br#"{"usage":{"input_tokens":999,"output_tokens":999}}"#;
        for size in [USAGE_BUFFER_LIMIT, USAGE_BUFFER_LIMIT + 1] {
            let mut json = prefix.to_vec();
            json.resize(size, b' ');
            let observed = observe_parts(&json, 4093);
            assert_eq!(observed.partial, size > USAGE_BUFFER_LIMIT);
            assert_eq!(
                observed.tokens[0],
                if size == USAGE_BUFFER_LIMIT {
                    Some(999)
                } else {
                    None
                }
            );
        }
        let end = b"data: {\"usage\":{\"input_tokens\":3,\"output_tokens\":4}}\n\ndata: {\"type\":\"message_stop\"}\n\n";
        for size in [USAGE_BUFFER_LIMIT, USAGE_BUFFER_LIMIT + 1] {
            let mut data = prefix.to_vec();
            data.resize(size - 1, b' ');
            let middle = data.len() / 2;
            let mut source = b"data: ".to_vec();
            source.extend_from_slice(&data[..middle]);
            source.extend_from_slice(b"\ndata: ");
            source.extend_from_slice(&data[middle..]);
            source.extend_from_slice(b"\n\n");
            source.extend_from_slice(end);
            let observed = observe_parts(&source, 8191);
            assert_eq!(observed.partial, size > USAGE_BUFFER_LIMIT);
            assert_eq!(
                observed.tokens[0],
                Some(if size == USAGE_BUFFER_LIMIT { 999 } else { 3 })
            );
            if size > USAGE_BUFFER_LIMIT {
                assert_eq!(observed.observation_error, Some("sampling_limit"));
            }
        }
        let mut oversized_line = b"data: ".to_vec();
        oversized_line.resize(USAGE_BUFFER_LIMIT + 1, b' ');
        oversized_line.extend_from_slice(
            b"\ndata: {\"usage\":{\"input_tokens\":9999,\"output_tokens\":9999}}\n\n",
        );
        oversized_line.extend_from_slice(end);
        let observed = observe_parts(&oversized_line, 8191);
        assert_eq!(observed.tokens[..2], [Some(3), Some(4)]);
        assert_eq!(observed.observation_error, Some("sampling_limit"));
        assert!(observed.partial);
    }

    #[test]
    fn real_zero_invalid_usage_and_gzip_footer_keep_distinct_completeness() {
        let zero = b"data: {\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}\n\ndata: {\"type\":\"message_stop\"}\n\n";
        let observed = observe_parts(zero, 1);
        assert_eq!(observed.tokens[..2], [Some(0), Some(0)]);
        assert!(!observed.partial);
        let invalid = b"data: {\"usage\":{\"input_tokens\":-1,\"output_tokens\":2}}\n\ndata: {\"type\":\"message_stop\"}\n\n";
        let observed = observe_parts(invalid, 7);
        assert_eq!(observed.tokens[..2], [None, Some(2)]);
        assert_eq!(observed.observation_error, Some("invalid_payload"));
        let source = events(true, "\r");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&source).unwrap();
        let encoded = encoder.finish().unwrap();
        for wire in [encoded.clone(), encoded[..encoded.len() - 1].to_vec()] {
            let mut capture = UsageCapture::observer(&json!({}));
            capture.configure_codings(&["gzip".into()]);
            for part in wire.chunks(7) {
                capture.observe(part);
            }
            let observed = capture.observation(true);
            assert_eq!(observed.tokens[..2], [Some(1000), Some(100)]);
            assert_eq!(observed.partial, wire.len() != encoded.len());
        }
    }
}
