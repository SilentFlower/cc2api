use std::collections::HashMap;
use std::iter::once;
use std::time::Duration;

use claude_code_gateway::service::rewriter::ordered_anthropic_headers;
use claude_code_gateway::tlsfp::make_request_client;
use hyper::Request;
use reqwest::{Body, Client, Request as ClientRequest, RequestBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// 接收原始 HTTP/1 字节，避免 HTTP 框架在断言之前把名字转成小写。
async fn raw_server(responses: Vec<String>) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        timeout(Duration::from_secs(10), async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buf = [0; 4096];
                    let count = socket.read(&mut buf).await.unwrap();
                    assert!(count > 0, "请求尚未完整就断开连接");
                    bytes.extend_from_slice(&buf[..count]);
                    assert!(bytes.len() < 64 * 1024, "合成测试请求超过上限");
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&bytes[..end]);
                        let length = head
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        })
        .await
        .expect("原始 HTTP 接收超时")
    });
    (url, task)
}

fn response() -> String {
    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".into()
}

fn headers() -> HashMap<String, String> {
    HashMap::from([
        ("Accept".into(), "application/json".into()),
        ("Authorization".into(), "Bearer synthetic".into()),
        ("content-type".into(), "application/json".into()),
        (
            "User-Agent".into(),
            "claude-cli/2.1.280 (external, cli)".into(),
        ),
        ("X-Stainless-OS".into(), "Linux".into()),
        ("anthropic-beta".into(), "oauth-2025-04-20".into()),
        ("x-claude-code-request-class".into(), "main".into()),
        ("accept-encoding".into(), "gzip, deflate, br, zstd".into()),
    ])
}

fn request(client: &Client, url: &str, preserve: bool) -> RequestBuilder {
    let ordered = ordered_anthropic_headers("/v1/messages", &headers());
    let mut builder = client.post(url);
    if preserve {
        builder = builder.http1_header_casing(
            ordered
                .iter()
                .map(|(name, _)| name.as_str())
                .chain(once("Content-Length")),
        );
    }
    for (name, value) in ordered {
        builder = builder.header(name, value);
    }
    builder.body("{\"text\":\"测试\"}")
}

fn assert_profile(raw: &str) {
    let expected = [
        "Accept",
        "Authorization",
        "Content-Type",
        "User-Agent",
        "X-Stainless-OS",
        "anthropic-beta",
        "x-claude-code-request-class",
        "Connection",
        "Host",
        "Accept-Encoding",
        "Content-Length",
    ];
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let names: Vec<_> = head
        .lines()
        .skip(1)
        .map(|line| line.split_once(':').unwrap().0)
        .collect();
    assert_eq!(names, expected);
    assert!(head.contains(&format!("Content-Length: {}", body.len())));
    assert_eq!(body, "{\"text\":\"测试\"}");
}

#[tokio::test]
async fn explicit_casing_survives_clone_and_conversion_without_changing_default() {
    let (url, server) = raw_server(vec![response(), response(), response()]).await;
    let client = make_request_client("").unwrap();
    let built = request(&client, &url, true).build().unwrap();
    let cloned = built.try_clone().unwrap();
    let converted: Request<Body> = built.try_into().unwrap();
    let restored = ClientRequest::try_from(converted).unwrap();
    for req in [cloned, restored] {
        assert_eq!(
            client.execute(req).await.unwrap().text().await.unwrap(),
            "ok"
        );
    }
    request(&client, &url, false)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let requests = server.await.unwrap();
    assert_profile(&requests[0]);
    assert_profile(&requests[1]);
    for line in requests[2]
        .split("\r\n\r\n")
        .next()
        .unwrap()
        .lines()
        .skip(1)
    {
        let name = line.split_once(':').unwrap().0;
        assert_eq!(name, name.to_ascii_lowercase(), "未启用时沿用原发送行为");
    }
}

#[tokio::test]
async fn explicit_casing_survives_redirect_and_keeps_sensitive_header_removal() {
    let (destination, second) = raw_server(vec![response()]).await;
    let redirect = format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {destination}/next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let (url, first) = raw_server(vec![redirect]).await;
    let client = make_request_client("").unwrap();
    assert_eq!(
        request(&client, &url, true)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "ok"
    );
    assert_profile(&first.await.unwrap()[0]);
    let redirected = &second.await.unwrap()[0];
    assert!(redirected.contains("\r\nUser-Agent: claude-cli/"));
    assert!(redirected.contains("\r\nanthropic-beta: oauth-2025-04-20\r\n"));
    assert!(redirected.contains("\r\nContent-Length: "));
    assert!(!redirected.to_ascii_lowercase().contains("authorization:"));
}

#[tokio::test]
async fn explicit_casing_reaches_configured_http_proxy() {
    let (proxy_url, server) = raw_server(vec![response()]).await;
    let client = make_request_client(&proxy_url).unwrap();
    request(
        &client,
        "http://upstream.invalid/v1/messages?beta=true",
        true,
    )
    .send()
    .await
    .unwrap()
    .bytes()
    .await
    .unwrap();
    let requests = server.await.unwrap();
    assert!(
        requests[0].starts_with("POST http://upstream.invalid/v1/messages?beta=true HTTP/1.1\r\n")
    );
    assert_profile(&requests[0]);
}

#[test]
fn explicit_casing_rejects_invalid_names_before_network_io() {
    let client = make_request_client("").unwrap();
    for name in ["X-Test\r\nInjected: yes", "X Bad", ""] {
        assert!(
            client
                .get("http://localhost/")
                .http1_header_casing([name])
                .build()
                .is_err()
        );
    }
}
