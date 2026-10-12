//! 固定账号容器中的 HTTPS 发送进程，沿用网关 crate 的 HTTP/TLS 实现。

use claude_code_gateway::service::runtime_transport::sender_router;

#[tokio::main]
async fn main() {
    let path = std::env::var("V3_SENDER_CONFIG").unwrap_or_else(|_| "/run/v3/sender.json".into());
    let address = std::env::var("V3_SENDER_LISTEN").unwrap_or_else(|_| "0.0.0.0:8091".into());
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("绑定账号内部端口失败");
    axum::serve(listener, sender_router(path))
        .await
        .expect("账号发送器退出");
}
