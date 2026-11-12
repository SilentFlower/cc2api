//! 传输异常只投影稳定类型，避免库错误中的 URL、凭据及正文进入普通日志。

use std::error::Error;
use std::io;

/// 错误链的有界类型摘要；未知错误保留未知语义。
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct StreamErrorSummary {
    pub timeout: bool,
    pub connect: bool,
    pub body: bool,
    pub decode: bool,
    pub http_transport: bool,
    pub incomplete: bool,
    pub canceled: bool,
    pub closed: bool,
    pub io_kind: Option<io::ErrorKind>,
    pub os_code: Option<i32>,
    pub chain_depth: usize,
    pub chain_truncated: bool,
}

/// 从类型化错误链提取诊断标志，不读取异常原文。
/// @param error 本层实际收到的传输错误。
/// @return 最多遍历八层的受控摘要。
pub(crate) fn summarize(error: &(dyn Error + 'static)) -> StreamErrorSummary {
    let mut summary = StreamErrorSummary::default();
    let mut current = Some(error);
    while let Some(error) = current {
        if summary.chain_depth == 8 {
            summary.chain_truncated = true;
            break;
        }
        summary.chain_depth += 1;
        if let Some(error) = error.downcast_ref::<reqwest::Error>() {
            summary.timeout |= error.is_timeout();
            summary.connect |= error.is_connect();
            summary.body |= error.is_body();
            summary.decode |= error.is_decode();
        }
        if let Some(error) = error.downcast_ref::<hyper::Error>() {
            summary.http_transport = true;
            summary.timeout |= error.is_timeout();
            summary.incomplete |= error.is_incomplete_message();
            summary.canceled |= error.is_canceled();
            summary.closed |= error.is_closed();
        }
        current = if let Some(error) = error.downcast_ref::<io::Error>() {
            summary.timeout |= error.kind() == io::ErrorKind::TimedOut;
            summary.io_kind = Some(error.kind());
            summary.os_code = summary.os_code.or(error.raw_os_error());
            // io::Error 的 source 可能直接越过包装层；显式访问内部错误保留类型。
            error.get_ref().map(|inner| inner as &(dyn Error + 'static))
        } else {
            error.source()
        };
    }
    summary
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::subscriber::Interest;
    use tracing::{Event, Metadata, Subscriber};

    struct LogCollector(Arc<Mutex<Vec<u8>>>);

    struct Fields(String);

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.0.push_str(&format!(" {}={value:?}", field.name()));
        }
    }

    impl Subscriber for LogCollector {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
            Interest::always()
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            fields.0.push('\n');
            self.0
                .lock()
                .unwrap()
                .extend_from_slice(fields.0.as_bytes());
        }
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }

    /// 按实际字段收集日志，避免并行测试的格式化过滤状态影响断言。
    /// @return 订阅器及共享输出缓冲。
    pub(crate) fn capture() -> (impl Subscriber + Send + Sync, Arc<Mutex<Vec<u8>>>) {
        let output = Arc::new(Mutex::new(Vec::new()));
        (LogCollector(output.clone()), output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::time::{Duration, sleep};

    #[test]
    fn error_summary_preserves_typed_causes_without_error_text() {
        let secret = "Bearer synthetic-secret@example.test/private?token=secret";
        let error = io::Error::other(io::Error::new(io::ErrorKind::ConnectionReset, secret));
        let summary = summarize(&error);
        assert_eq!(summary.chain_depth, 3);
        assert_eq!(summary.io_kind, Some(io::ErrorKind::ConnectionReset));
        assert!(!format!("{summary:?}").contains(secret));
        let error = io::Error::from_raw_os_error(libc::ECONNRESET);
        assert_eq!(summarize(&error).os_code, Some(libc::ECONNRESET));
        assert_eq!(
            summarize(&error).io_kind,
            Some(io::ErrorKind::ConnectionReset)
        );
        let mut nested = io::Error::other(secret);
        for _ in 0..10 {
            nested = io::Error::other(nested);
        }
        assert!(summarize(&nested).chain_truncated);
    }

    #[tokio::test]
    async fn real_http_truncation_and_timeout_have_distinct_safe_summaries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial")
                .await
                .unwrap();
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let error = client
            .get(format!("http://{address}/?token=synthetic-secret"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap_err();
        let summary = summarize(&error);
        assert!(summary.decode || summary.body);
        assert!(summary.http_transport, "{summary:?}");
        assert!(!summary.timeout);
        assert!(!format!("{summary:?}").contains("synthetic-secret"));
        server.await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            sleep(Duration::from_secs(5)).await;
        });
        let error = client
            .get(format!("http://{address}/"))
            .timeout(Duration::from_millis(30))
            .send()
            .await
            .unwrap_err();
        assert!(summarize(&error).timeout);
        assert!(!summarize(&error).incomplete);
        server.abort();
    }
}
