use once_cell::sync::Lazy;
use rustls::craft::{
    CraftExtension, ExtensionSpec, Fingerprint, GreaseOrCipher, GreaseOrCurve, GreaseOrVersion,
    KeepExtension, OCSP_REQ,
};
use rustls::crypto::{ActiveKeyExchange, SharedSecret, SupportedKxGroup};
use rustls::internal::msgs::base::Payload;
use rustls::internal::msgs::enums::{ECPointFormat, ExtensionType, PSKKeyExchangeMode};
use rustls::internal::msgs::handshake::{ClientExtension, ClientSessionTicket};
use rustls::{
    CipherSuite, ClientConfig, Error, NamedGroup, ProtocolVersion, RootCertStore, SignatureScheme,
};
#[cfg(any(test, feature = "v3-verification"))]
use rustls_pki_types::CertificateDer;
use static_init::dynamic;
use std::collections::HashMap;
#[cfg(feature = "v3-verification")]
use std::fs::read;
#[cfg(test)]
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// ---------------------------------------------------------------------------
// X25519MLKEM768 混合密钥交换（真实实现）
// 按 draft-ietf-tls-ecdhe-mlkem：
//   client key_share = ML-KEM encaps key (1184) || X25519 pub (32) = 1216 bytes
//   server key_share = ML-KEM ciphertext (1088) || X25519 pub (32) = 1120 bytes
//   shared_secret    = ML-KEM shared secret (32) || X25519 shared (32) = 64 bytes
// ---------------------------------------------------------------------------
const X25519MLKEM768_GROUP: NamedGroup = NamedGroup::Unknown(0x11EC);

#[derive(Debug)]
struct X25519Mlkem768KxGroup;

impl SupportedKxGroup for X25519Mlkem768KxGroup {
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, Error> {
        use ml_kem::{EncodedSizeUser, KemCore, MlKem768};

        let mut rng = rand::thread_rng();

        // ML-KEM-768 keypair
        let (dk, ek) = MlKem768::generate(&mut rng);
        let ek_bytes = &ek.as_bytes();

        // X25519 keypair
        let x25519_secret = x25519_dalek::StaticSecret::random_from_rng(&mut rng);
        let x25519_public = x25519_dalek::PublicKey::from(&x25519_secret);

        // client key_share = ek (1184) || x25519_pub (32)
        let mut pub_key = Vec::with_capacity(1216);
        pub_key.extend_from_slice(ek_bytes);
        pub_key.extend_from_slice(x25519_public.as_bytes());

        Ok(Box::new(X25519Mlkem768ActiveKx {
            dk,
            x25519_secret,
            pub_key,
        }))
    }

    fn name(&self) -> NamedGroup {
        X25519MLKEM768_GROUP
    }
}

struct X25519Mlkem768ActiveKx {
    dk: ml_kem::kem::DecapsulationKey<ml_kem::MlKem768Params>,
    x25519_secret: x25519_dalek::StaticSecret,
    pub_key: Vec<u8>,
}

impl std::fmt::Debug for X25519Mlkem768ActiveKx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("X25519Mlkem768ActiveKx").finish()
    }
}

impl ActiveKeyExchange for X25519Mlkem768ActiveKx {
    fn complete(self: Box<Self>, peer_pub_key: &[u8]) -> Result<SharedSecret, Error> {
        use ml_kem::kem::Decapsulate as _;

        // server key_share = ciphertext (1088) || x25519_pub (32) = 1120 bytes
        if peer_pub_key.len() != 1120 {
            return Err(Error::General(format!(
                "X25519MLKEM768: invalid server key_share length {}",
                peer_pub_key.len()
            )));
        }

        let (ct_bytes, x25519_peer) = peer_pub_key.split_at(1088);

        // ML-KEM decapsulation
        let ct: ml_kem::Ciphertext<ml_kem::MlKem768> = ct_bytes
            .try_into()
            .map_err(|_| Error::General("ML-KEM: invalid ciphertext".into()))?;
        let mlkem_ss = self
            .dk
            .decapsulate(&ct)
            .map_err(|_| Error::General("ML-KEM decapsulation failed".into()))?;

        // X25519 DH
        let x25519_peer_key: [u8; 32] = x25519_peer
            .try_into()
            .map_err(|_| Error::General("X25519: invalid peer key".into()))?;
        let x25519_peer_pub = x25519_dalek::PublicKey::from(x25519_peer_key);
        let x25519_ss = self.x25519_secret.diffie_hellman(&x25519_peer_pub);
        // 混合 KEM 不能掩盖低阶 X25519 输入；全零秘密必须按协议拒绝。
        if !x25519_ss.was_contributory() {
            return Err(Error::General(
                "X25519MLKEM768: invalid X25519 peer key".into(),
            ));
        }

        // shared_secret = mlkem_ss (32) || x25519_ss (32)
        let mut shared = Vec::with_capacity(64);
        shared.extend_from_slice(mlkem_ss.as_ref());
        shared.extend_from_slice(x25519_ss.as_bytes());

        Ok(SharedSecret::from(&shared[..]))
    }

    fn pub_key(&self) -> &[u8] {
        &self.pub_key
    }

    fn group(&self) -> NamedGroup {
        X25519MLKEM768_GROUP
    }
}

static X25519MLKEM768_KX: X25519Mlkem768KxGroup = X25519Mlkem768KxGroup;
static REQUEST_CLIENT_POOL_ENABLED: AtomicBool = AtomicBool::new(true);
static REQUEST_CLIENT_CACHE: Lazy<RwLock<HashMap<String, reqwest::Client>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

macro_rules! static_ref {
    ($val:expr, $type:ty) => {{
        static X: $type = $val;
        X
    }};
}

/// 原版 Claude Code 2.1.293 主请求的有序 cipher offer，依据正式握手与无 MITM 对照。
#[dynamic]
pub static CLAUDE_CODE_293_CIPHERS: Vec<GreaseOrCipher> = vec![
    4865, 4866, 4867, 49195, 49199, 49196, 49200, 52393, 52392, 49161, 49171, 49162, 49172, 156,
    157, 47, 53,
]
.into_iter()
.map(|value| GreaseOrCipher::T(CipherSuite::from(value)))
.collect();

/// 原版 293 主请求的扩展顺序；恢复材料只接受 rustls 本次真实生成的值。
#[dynamic]
pub static CLAUDE_CODE_293_EXTENSIONS: Vec<ExtensionSpec> = {
    use ExtensionSpec::*;
    use KeepExtension::*;
    vec![
        Keep(Must(ExtensionType::ServerName)),
        Rustls(ClientExtension::ExtendedMasterSecretRequest),
        Craft(CraftExtension::RenegotiationInfo),
        Rustls(ClientExtension::NamedGroups(vec![
            X25519MLKEM768_GROUP,
            NamedGroup::X25519,
            NamedGroup::secp256r1,
            NamedGroup::secp384r1,
        ])),
        Rustls(ClientExtension::EcPointFormats(vec![
            ECPointFormat::Uncompressed,
        ])),
        Keep(OrDefault(
            ExtensionType::SessionTicket,
            ClientExtension::SessionTicket(ClientSessionTicket::Offer(Payload(vec![]))),
        )),
        Craft(CraftExtension::Protocols(&[b"http/1.1"])),
        Rustls(ClientExtension::CertificateStatusRequest(OCSP_REQ.clone())),
        Rustls(ClientExtension::SignatureAlgorithms(
            vec![1027, 2052, 1025, 1283, 2053, 1281, 2054, 1537, 513]
                .into_iter()
                .map(SignatureScheme::from)
                .collect(),
        )),
        Craft(CraftExtension::SignedCertificateTimestamp),
        Craft(CraftExtension::KeyShare(static_ref!(
            &[
                GreaseOrCurve::T(X25519MLKEM768_GROUP),
                GreaseOrCurve::T(NamedGroup::X25519)
            ],
            &[GreaseOrCurve]
        ))),
        Rustls(ClientExtension::PresharedKeyModes(vec![
            PSKKeyExchangeMode::PSK_DHE_KE,
        ])),
        Craft(CraftExtension::SupportedVersions(static_ref!(
            &[
                GreaseOrVersion::T(ProtocolVersion::TLSv1_3),
                GreaseOrVersion::T(ProtocolVersion::TLSv1_2)
            ],
            &[GreaseOrVersion]
        ))),
        // Cookie 仅在真实 HRR 时出现；PSK 必须是最后扩展，binder 由 rustls 计算。
        Keep(Optional(ExtensionType::Cookie)),
        Keep(Optional(ExtensionType::PreSharedKey)),
    ]
};

/// 统一出站模板目前以原版 293 主 messages 为基线，辅助接口及其他版本另需实测。
#[dynamic]
pub static CLAUDE_CODE_293_FINGERPRINT: Fingerprint = Fingerprint {
    extensions: &CLAUDE_CODE_293_EXTENSIONS,
    cipher: &CLAUDE_CODE_293_CIPHERS,
    shuffle_extensions: false,
};

/// 构建原生 293 主请求 TLS 配置。@return 保持生产信任根与证书校验的配置。
fn build_tls_config() -> ClientConfig {
    let root_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    #[cfg(feature = "v3-verification")]
    let root_store = {
        let mut roots = root_store;
        // 隔离上游使用临时 CA，仍由 rustls 校验签名和主机名；此分支不进入正式二进制。
        let certificate =
            read("/run/v3/verification-ca.der").expect("隔离验证构建需要专用 CA 文件");
        roots
            .add(CertificateDer::from(certificate))
            .expect("隔离验证 CA 无效");
        roots
    };
    build_tls_config_with_roots(root_store)
}

/// 使用指定信任根构建同一生产画像。@param root_store 信任根。@return TLS 配置。
fn build_tls_config_with_roots(root_store: RootCertStore) -> ClientConfig {
    let mut config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
        .with_fingerprint(CLAUDE_CODE_293_FINGERPRINT.builder());

    // 只保留实际可完成交换的组，避免 HRR 选中只声明却无法执行的占位实现。
    let mut provider = config.provider.as_ref().clone();
    provider.kx_groups.insert(0, &X25519MLKEM768_KX);
    provider.kx_groups.retain(|group| {
        matches!(
            group.name(),
            X25519MLKEM768_GROUP
                | NamedGroup::X25519
                | NamedGroup::secp256r1
                | NamedGroup::secp384r1
        )
    });
    config.provider = Arc::new(provider);

    config
}

/// 设置带代理的 reqwest 客户端连接池是否启用。
///
/// # 参数
/// - `enabled`: `true` 表示复用同一代理的缓存客户端,`false` 表示每次请求新建客户端。
pub fn set_request_client_pool_enabled(enabled: bool) {
    REQUEST_CLIENT_POOL_ENABLED.store(enabled, Ordering::Relaxed);
    if !enabled {
        REQUEST_CLIENT_CACHE
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// 读取当前 reqwest 客户端连接池开关状态。
///
/// # 返回
/// 返回 `true` 表示当前会复用缓存客户端,返回 `false` 表示每次请求新建客户端。
pub fn request_client_pool_enabled() -> bool {
    REQUEST_CLIENT_POOL_ENABLED.load(Ordering::Relaxed)
}

/// 获取带 TLS 指纹伪装的缓存 reqwest 客户端。
///
/// 相同代理地址返回同一个内部连接池的 clone；不同代理地址使用独立客户端，避免代理配置串用。
///
/// # 参数
/// - `proxy_url`: 代理地址。空字符串表示直连，非空时沿用 reqwest 支持的 HTTP/SOCKS 代理格式。
///
/// # 返回
/// 成功时返回可直接发起请求的 `reqwest::Client` clone；代理配置或客户端构建失败时返回错误。
pub fn get_request_client(proxy_url: &str) -> Result<reqwest::Client, reqwest::Error> {
    if !request_client_pool_enabled() {
        return make_request_client(proxy_url);
    }

    let cache_key = proxy_url.to_string();
    {
        let cache = REQUEST_CLIENT_CACHE
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(client) = cache.get(&cache_key) {
            return Ok(client.clone());
        }
    }

    let client = make_request_client(proxy_url)?;
    let mut cache = REQUEST_CLIENT_CACHE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = cache.get(&cache_key) {
        return Ok(existing.clone());
    }
    cache.insert(cache_key, client.clone());
    Ok(client)
}

/// 创建带 TLS 指纹伪装的 reqwest 客户端。
///
/// 支持直连和代理（HTTP/SOCKS5）。
///
/// # 参数
/// - `proxy_url`: 代理地址。空字符串表示直连，非空时沿用 reqwest 支持的 HTTP/SOCKS 代理格式。
///
/// # 返回
/// 成功时返回新建的 `reqwest::Client`；代理配置或客户端构建失败时返回错误。
pub fn make_request_client(proxy_url: &str) -> Result<reqwest::Client, reqwest::Error> {
    let tls_config = build_tls_config();

    // 不设整体 timeout，也不用 read_timeout（reqwest 0.12.4 不支持该 API）：
    // 整体超时会误杀健康长流（Opus 扩展思考可持续数十分钟）；
    // 卡死连接的检测统一放到 gateway 层（tokio::time::timeout 包 send() 与 bytes_stream()）。
    //
    // 保活: tcp_keepalive OS 层 SO_KEEPALIVE + KEEPIDLE=30s,idle 30s 后发 TCP 探测包。
    // HTTP/2 PING 因 craftls 指纹 ALPN 只声明 http/1.1,永远协商不出 h2,故不配置。
    let mut builder = request_client_builder(tls_config);

    if !proxy_url.is_empty() {
        // 非空代理必须成功解析并真正挂到客户端上，不能因配置错误退回直连。
        builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
    }

    builder.build()
}

fn request_client_builder(tls_config: ClientConfig) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .use_preconfigured_tls(tls_config)
        .connect_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .no_proxy()
}

/// 本地 TLS 测试复用生产指纹和客户端设置；额外信任根仅编译进测试。
/// @param certificate 测试 CA；None 时使用生产公有根。
/// @param host 仅解析到本地的测试域名。
/// @param address 本地 TLS 服务地址。
/// @return 保持证书和主机名校验的测试客户端。
#[cfg(test)]
pub(crate) fn test_request_client(
    certificate: Option<CertificateDer<'static>>,
    host: &str,
    address: SocketAddr,
) -> Result<reqwest::Client, reqwest::Error> {
    let mut roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    if let Some(certificate) = certificate {
        roots.add(certificate).unwrap();
    }
    request_client_builder(build_tls_config_with_roots(roots))
        .resolve(host, address)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ml_kem::kem::{Encapsulate, EncapsulationKey};
    use ml_kem::{EncodedSizeUser, MlKem768Params};
    use rand::thread_rng;
    use rustls::client::{ClientSessionMemoryCache, ClientSessionStore, Resumption};
    use rustls::{ClientConnection, StreamOwned};
    use rustls_pki_types::ServerName;
    use serde_json::{Value, from_slice, from_str, json};
    use std::env::temp_dir;
    use std::fs;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{Ipv4Addr, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::Mutex;
    use uuid::Uuid;
    use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret as X25519Secret};

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn clear_request_client_cache_for_test() {
        REQUEST_CLIENT_CACHE
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    fn request_client_cache_len_for_test() -> usize {
        REQUEST_CLIENT_CACHE
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    struct RequestClientPoolStateGuard;

    impl RequestClientPoolStateGuard {
        fn new() -> Self {
            set_request_client_pool_enabled(true);
            clear_request_client_cache_for_test();
            Self
        }
    }

    impl Drop for RequestClientPoolStateGuard {
        fn drop(&mut self) {
            set_request_client_pool_enabled(true);
            clear_request_client_cache_for_test();
        }
    }

    #[test]
    fn same_proxy_reuses_cached_client_entry() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _state_guard = RequestClientPoolStateGuard::new();

        let proxy_url = "socks5h://127.0.0.1:65530";

        let _first = get_request_client(proxy_url).unwrap();
        assert_eq!(request_client_cache_len_for_test(), 1);

        let _second = get_request_client(proxy_url).unwrap();
        assert_eq!(request_client_cache_len_for_test(), 1);
    }

    #[test]
    fn different_proxy_uses_different_cached_client_entry() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _state_guard = RequestClientPoolStateGuard::new();

        let _first = get_request_client("socks5h://127.0.0.1:65530").unwrap();
        let _second = get_request_client("socks5h://127.0.0.1:65531").unwrap();

        assert_eq!(request_client_cache_len_for_test(), 2);
    }

    #[test]
    fn disabled_pool_bypasses_cached_client_entry() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _state_guard = RequestClientPoolStateGuard::new();

        set_request_client_pool_enabled(false);
        assert!(!request_client_pool_enabled());

        let proxy_url = "socks5h://127.0.0.1:65530";

        let _first = get_request_client(proxy_url).unwrap();
        assert_eq!(request_client_cache_len_for_test(), 0);

        let _second = get_request_client(proxy_url).unwrap();
        assert_eq!(request_client_cache_len_for_test(), 0);
    }

    #[test]
    fn invalid_proxy_is_rejected_without_caching_direct_client() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _state_guard = RequestClientPoolStateGuard::new();

        assert!(get_request_client("://invalid-proxy").is_err());
        assert_eq!(request_client_cache_len_for_test(), 0);
    }

    #[test]
    fn clienthello_wire_matches_native_293_even_after_cached_group_hint() {
        let cache = Arc::new(ClientSessionMemoryCache::new(256));
        let mut config = build_tls_config_with_roots(RootCertStore::empty());
        config.resumption = Resumption::store(cache.clone());
        let config = Arc::new(config);
        for group in [
            X25519MLKEM768_GROUP,
            NamedGroup::X25519,
            NamedGroup::secp256r1,
        ] {
            let hostname = ServerName::try_from("api.anthropic.com")
                .unwrap()
                .to_owned();
            cache.set_kx_hint(hostname.clone(), group);
            assert_eq!(cache.kx_hint(&hostname), Some(group));
            let mut connection = ClientConnection::new(config.clone(), hostname).unwrap();
            let mut wire = Vec::new();
            connection.write_tls(&mut wire).unwrap();
            let mut child = Command::new("python3")
                .arg("-B")
                .arg(
                    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls_clienthello.py"),
                )
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(&wire).unwrap();
            let result = child.wait_with_output().unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let value: Value = from_slice(&result.stdout).unwrap();
            assert_eq!(value["ja3"]["md5"], "1523504b38f0fae0d881d4b6554aac1b");
            assert_eq!(value["fields"]["sni"], "api.anthropic.com");
        }
    }

    #[test]
    fn explicit_keyshare_override_disabled_preserves_cached_group_hint() {
        let mut config = build_tls_config_with_roots(RootCertStore::empty()).with_fingerprint(
            CLAUDE_CODE_293_FINGERPRINT
                .builder()
                .dangerous_disable_override_keyshare(),
        );
        // 只关闭测试配置中的 share 覆盖，验证未启用的新分支仍沿原缓存提示机制。
        let cache = Arc::new(ClientSessionMemoryCache::new(256));
        cache.set_kx_hint(
            ServerName::try_from("api.anthropic.com")
                .unwrap()
                .to_owned(),
            NamedGroup::secp256r1,
        );
        assert_eq!(
            cache.kx_hint(&ServerName::try_from("api.anthropic.com").unwrap()),
            Some(NamedGroup::secp256r1)
        );
        config.resumption = Resumption::store(cache);
        assert!(
            config
                .provider
                .kx_groups
                .iter()
                .any(|group| group.name() == NamedGroup::secp256r1)
        );
        let mut connection = ClientConnection::new(
            Arc::new(config),
            ServerName::try_from("api.anthropic.com")
                .unwrap()
                .to_owned(),
        )
        .unwrap();
        let mut wire = Vec::new();
        connection.write_tls(&mut wire).unwrap();
        // 独立解析结果由 Python 返回，避免从模板常量直接推断实际发送组。
        let program = "import json,sys; from pathlib import Path; sys.path.insert(0,sys.argv[1]); from tls_clienthello import parse_body; data=sys.stdin.buffer.read(); print(json.dumps(parse_body(data[9:])['key_share']))";
        let mut child = Command::new("python3")
            .arg("-B")
            .arg("-c")
            .arg(program)
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&wire).unwrap();
        let result = child.wait_with_output().unwrap();
        assert!(result.status.success());
        let shares: Value = from_slice(&result.stdout).unwrap();
        assert_eq!(shares, json!([{"group":23,"key_exchange_length":65}]));
    }

    #[test]
    fn hybrid_exchange_matches_independent_server_and_rejects_invalid_peers() {
        let client = X25519MLKEM768_KX.start().unwrap();
        assert_eq!(client.pub_key().len(), 1216);
        let encoded = client.pub_key()[..1184].try_into().unwrap();
        let peer_key = EncapsulationKey::<MlKem768Params>::from_bytes(&encoded);
        let mut rng = thread_rng();
        let (ciphertext, kem_secret) = peer_key.encapsulate(&mut rng).unwrap();
        let server_secret = X25519Secret::random_from_rng(&mut rng);
        let server_public = X25519PublicKey::from(&server_secret);
        let client_public =
            X25519PublicKey::from(<[u8; 32]>::try_from(&client.pub_key()[1184..]).unwrap());
        let dh_secret = server_secret.diffie_hellman(&client_public);
        let mut peer = ciphertext.to_vec();
        peer.extend_from_slice(server_public.as_bytes());
        assert_eq!(peer.len(), 1120);
        let actual = client.complete(&peer).unwrap();
        let mut expected = kem_secret.to_vec();
        expected.extend_from_slice(dh_secret.as_bytes());
        assert_eq!(actual.secret_bytes(), expected);
        for size in [0, 1119, 1121] {
            assert!(
                X25519MLKEM768_KX
                    .start()
                    .unwrap()
                    .complete(&vec![0; size])
                    .is_err()
            );
        }
        let client = X25519MLKEM768_KX.start().unwrap();
        let encoded = client.pub_key()[..1184].try_into().unwrap();
        let (ciphertext, _) = EncapsulationKey::<MlKem768Params>::from_bytes(&encoded)
            .encapsulate(&mut rng)
            .unwrap();
        let mut invalid = ciphertext.to_vec();
        invalid.extend_from_slice(&[0; 32]);
        assert!(client.complete(&invalid).is_err());
    }

    /// 临时本地HTTPS服务器，退出时定向收口子进程与合成资料。
    struct FingerprintServer {
        root: PathBuf,
        child: Child,
        address: SocketAddr,
    }

    impl FingerprintServer {
        /// 创建指定TLS验证模式。@param mode TLS1.2、TLS1.3或HRR。@return 就绪服务器。
        fn start(mode: &str) -> Self {
            let root = temp_dir().join(format!("cc2api-tls-fingerprint-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            }
            let child = Command::new("python3")
                .arg("-B")
                .arg(
                    Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/fixtures/tls_fingerprint_server.py"),
                )
                .arg(&root)
                .arg(mode)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut fixture = Self {
                root,
                child,
                address: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            };
            let mut line = String::new();
            BufReader::new(fixture.child.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            let value: Value = from_str(&line).unwrap();
            fixture
                .address
                .set_port(value["port"].as_u64().unwrap().try_into().unwrap());
            fixture
        }

        /// 建立实际生产TLS配置的回环连接。@param config 同一会话配置。@return 可读写TLS流。
        fn connect(&self, config: Arc<ClientConfig>) -> StreamOwned<ClientConnection, TcpStream> {
            let socket = TcpStream::connect_timeout(&self.address, Duration::from_secs(5)).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            StreamOwned::new(
                ClientConnection::new(
                    config,
                    ServerName::try_from("api.anthropic.com")
                        .unwrap()
                        .to_owned(),
                )
                .unwrap(),
                socket,
            )
        }

        /// 发送并读取一条完整合成响应。@param stream TLS流。@param path 测试路径。@return None。
        fn request(&self, stream: &mut StreamOwned<ClientConnection, TcpStream>, path: &str) {
            write!(
                stream,
                "GET {path} HTTP/1.1\r\nHost: api.anthropic.com\r\nConnection: keep-alive\r\n\r\n"
            )
            .unwrap();
            stream.flush().unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            assert!(line.starts_with("HTTP/1.1 200"), "{line}");
            let mut length = None;
            loop {
                line.clear();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let mut body = vec![0; length.unwrap()];
            reader.read_exact(&mut body).unwrap();
            assert_eq!(body, b"{\"synthetic\":true}");
        }

        /// 读取仅含公开字段的夹具结果。@param name 文件。@return JSON行。
        fn records(&self, name: &str) -> Vec<Value> {
            fs::read_to_string(self.root.join(name))
                .unwrap()
                .lines()
                .map(|line| from_str(line).unwrap())
                .collect()
        }
    }

    impl Drop for FingerprintServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn local_https_native_fingerprint_tls12_tls13_resumption_and_hrr() {
        let expected: Value = from_str(include_str!(
            "../../tests/fixtures/claude_code_293_tls.json"
        ))
        .unwrap();
        for mode in ["tls12", "tls13", "hrr"] {
            let fixture = FingerprintServer::start(mode);
            let mut roots = RootCertStore::empty();
            roots
                .add(CertificateDer::from(
                    fs::read(fixture.root.join("ca.der")).unwrap(),
                ))
                .unwrap();
            let config = Arc::new(build_tls_config_with_roots(roots));
            let mut connection = fixture.connect(config.clone());
            fixture.request(&mut connection, "/first");
            fixture.request(&mut connection, "/keepalive");
            drop(connection);
            let mut connection = fixture.connect(config);
            fixture.request(&mut connection, "/second-connection");
            drop(connection);
            let requests = fixture.records("requests.jsonl");
            assert_eq!(requests.len(), 3);
            assert_eq!(
                requests[0]["tls"],
                if mode == "tls12" {
                    "TLSv1.2"
                } else {
                    "TLSv1.3"
                }
            );
            assert_eq!(requests[0]["session_reused"], false);
            assert_eq!(requests[1]["session_reused"], false);
            assert_eq!(requests[2]["session_reused"], true, "{mode}");
            assert!(requests.iter().all(|value| value["alpn"] == "http/1.1"));
            let hellos = fixture.records("hellos.jsonl");
            assert_eq!(hellos[0]["ja3"], expected["fingerprint"]["ja3"]);
            assert_eq!(hellos[0]["key_share"], expected["fingerprint"]["key_share"]);
            if mode == "hrr" {
                assert_eq!(hellos.len(), 4);
                for hello in [&hellos[1], &hellos[3]] {
                    assert_eq!(
                        hello["key_share"],
                        json!([{"group":23,"key_exchange_length":65}])
                    );
                }
            } else {
                assert_eq!(hellos.len(), 2);
                assert_eq!(hellos[1]["key_share"], expected["fingerprint"]["key_share"]);
            }
            if mode == "tls13" {
                assert_eq!(hellos[1]["ja3"]["md5"], expected["resumption_ja3_md5"]);
                assert_eq!(
                    hellos[1]["extensions"].as_array().unwrap().last().unwrap()["type"],
                    41
                );
            }
        }
    }
}
