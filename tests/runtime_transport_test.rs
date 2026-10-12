//! v3 传输保真、长流和取消验证；只使用本地合成服务。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use claude_code_gateway::service::runtime_transport::{
    PreparedRequest, SenderConfig, TransportBinding, sender_router,
};
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use uuid::Uuid;

/// 按 Content-Length 接收完整原始请求，保留大小写、顺序和 body。
async fn request_bytes(socket: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 65536);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&bytes[..end]);
            let length = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                .unwrap_or(0);
            if bytes.len() >= end + 4 + length {
                return bytes;
            }
        }
    }
}

/// 创建独立账号发送实例；每个实例拥有自己的配置和进程内活动计数。
async fn sender(origin: &str) -> (String, SenderConfig, String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let config = SenderConfig {
        binding: TransportBinding {
            account_id: 7,
            environment_id: Uuid::new_v4().to_string(),
            generation: 4,
            endpoint: endpoint.clone(),
            secret: "synthetic-secret-".repeat(4),
            release_id: "test".into(),
        },
        proxy_url: String::new(),
        allowed_origin: origin.into(),
        evidence_file: None,
        quota_identity: String::new(),
    };
    let path = format!("/tmp/v3-sender-test-{}.json", Uuid::new_v4());
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let app = sender_router(path.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (endpoint, config, path, handle)
}

fn prepared(config: &SenderConfig, url: String) -> PreparedRequest {
    PreparedRequest {
        account_id: config.binding.account_id,
        environment_id: config.binding.environment_id.clone(),
        generation: config.binding.generation,
        request_id: Uuid::new_v4().to_string(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 30_000,
        method: "POST".into(),
        target_url: url,
        preserve_header_casing: true,
        headers: vec![
            ("Authorization".into(), "Bearer synthetic-upstream".into()),
            ("Content-Type".into(), "application/json".into()),
            ("X-Order-A".into(), "one".into()),
            ("X-Order-B".into(), "two".into()),
        ],
    }
}

fn post(
    client: &Client,
    endpoint: &str,
    config: &SenderConfig,
    request: &PreparedRequest,
    body: Vec<u8>,
) -> reqwest::RequestBuilder {
    client
        .post(format!("{endpoint}/internal/send"))
        .bearer_auth(&config.binding.secret)
        .header(
            "x-v3-request",
            STANDARD.encode(serde_json::to_vec(request).unwrap()),
        )
        .body(body)
}

#[tokio::test]
async fn raw_body_header_order_and_upstream_error_survive_bridge() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let raw = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let raw = request_bytes(&mut socket).await;
        socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 7\r\nX-Test: preserved\r\nConnection: close\r\n\r\nblocked").await.unwrap();
        raw
    });
    let (endpoint, config, path, handle) = sender(&origin).await;
    let client = Client::builder().no_proxy().build().unwrap();
    let body = b"{ \"messages\" : [\"\xe4\xb8\xad\xe6\x96\x87\"] }\n".to_vec();
    let response = post(
        &client,
        &endpoint,
        &config,
        &prepared(&config, format!("{origin}/v1/messages?beta=true")),
        body.clone(),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status().as_u16(), 429);
    assert_eq!(response.headers()["x-test"], "preserved");
    assert!(!response.headers().contains_key("x-v3-transport-error"));
    assert_eq!(response.bytes().await.unwrap(), &b"blocked"[..]);
    let wire = timeout(Duration::from_secs(3), raw).await.unwrap().unwrap();
    let end = wire
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap()
        + 4;
    assert_eq!(&wire[end..], body);
    let headers = String::from_utf8_lossy(&wire[..end]);
    assert!(headers.starts_with("POST /v1/messages?beta=true HTTP/1.1\r\n"));
    assert!(headers.find("Authorization:").unwrap() < headers.find("Content-Type:").unwrap());
    assert!(headers.find("X-Order-A:").unwrap() < headers.find("X-Order-B:").unwrap());
    assert!(!headers.contains("x-v3-request") && !headers.contains(&config.binding.secret));
    handle.abort();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn sse_is_streamed_and_disconnect_closes_upstream_without_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request_bytes(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n9\r\ndata: a\n\n\r\n").await.unwrap();
        let mut buffer = [0; 1];
        let result = timeout(Duration::from_secs(5), socket.read(&mut buffer))
            .await
            .unwrap();
        assert!(
            matches!(result, Ok(0) | Err(_)),
            "取消后应关闭尚未完成的上游响应"
        );
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "不能重新提交"
        );
    });
    let (endpoint, config, path, handle) = sender(&origin).await;
    let client = Client::builder().no_proxy().build().unwrap();
    let mut response = timeout(
        Duration::from_secs(2),
        post(
            &client,
            &endpoint,
            &config,
            &prepared(&config, format!("{origin}/v1/messages")),
            b"{}".to_vec(),
        )
        .send(),
    )
    .await
    .unwrap()
    .unwrap();
    let chunk = timeout(Duration::from_secs(2), response.chunk())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(chunk, &b"data: a\n\n"[..]);
    let active: serde_json::Value = client
        .get(format!("{endpoint}/internal/status"))
        .bearer_auth(&config.binding.secret)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(active["active"], 1);
    drop(response);
    upstream.await.unwrap();
    let active: serde_json::Value = client
        .get(format!("{endpoint}/internal/status"))
        .bearer_auth(&config.binding.secret)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(active["active"], 0);
    handle.abort();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn stale_generation_is_rejected_before_contacting_upstream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (endpoint, config, path, handle) = sender(&origin).await;
    let mut request = prepared(&config, format!("{origin}/v1/messages"));
    request.generation -= 1;
    let client = Client::builder().no_proxy().build().unwrap();
    let response = post(&client, &endpoint, &config, &request, b"{}".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(response.headers()["x-v3-transport-error"], "before-send");
    assert!(
        timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    handle.abort();
    std::fs::remove_file(path).unwrap();
}
