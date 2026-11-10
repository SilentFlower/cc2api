//! 按账号保存有界的 CC 组装后快照，完整内容只通过管理员接口读取。

use std::cmp::Reverse;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use regex::Regex;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::model::request_diagnostics::RequestLogMode;
use crate::service::native_contract::NativeSubmission;

const BODY_LIMIT: usize = 2 * 1024 * 1024;
const HEADER_LIMIT: usize = 128 * 1024;
const FILE_LIMIT: usize = 50;
const STORAGE_LIMIT: u64 = 100 * 1024 * 1024;
static WRITERS: Semaphore = Semaphore::const_new(2);
static STORAGE: Mutex<()> = Mutex::new(());
static TOKEN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"sk-ant-[A-Za-z0-9_-]+").unwrap());
static SECRET_STRING: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r#"(?i)("[^"\\]*(?:authorization|cookie|api[-_]?key|token|password|secret|credential)[^"\\]*"\s*:\s*)"(?:\\.|[^"\\])*""#
).unwrap()
});
static SECRET_SCALAR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r#"(?i)("(?:authorization|cookie|api[-_]?key|access[-_]?token|refresh[-_]?token|password|secret|credential|token)"\s*:\s*)(?:-?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null)"#
).unwrap()
});
static PROXY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"://[^\s/:]+:[^\s/@]+@").unwrap());

fn sensitive(name: &str) -> bool {
    let name = name.to_ascii_lowercase().replace('-', "_");
    [
        "authorization",
        "cookie",
        "api_key",
        "access_token",
        "refresh_token",
        "password",
        "secret",
        "credential",
    ]
    .iter()
    .any(|part| name.contains(part))
        || name == "token"
        || name.ends_with("_token")
}

fn collect(value: &Value, secrets: &mut Vec<String>) {
    match value {
        Value::Object(fields) => {
            for (name, value) in fields {
                if sensitive(name) {
                    if let Some(value) = value.as_str() {
                        add_secret(value, secrets);
                    }
                }
                collect(value, secrets);
            }
        }
        Value::Array(values) => {
            // 原生输入以有序二元组保存请求头；过滤后的出站头已无法补齐这些入站秘密。
            if let [Value::String(name), Value::String(value)] = values.as_slice() {
                if sensitive(name) {
                    add_secret(value, secrets);
                }
            }
            for value in values {
                collect(value, secrets);
            }
        }
        _ => {}
    }
}

fn add_secret(value: &str, secrets: &mut Vec<String>) {
    if value.len() >= 4 {
        secrets.push(value.to_owned());
    }
    if let Some((scheme, token)) = value.split_once(' ') {
        if scheme.eq_ignore_ascii_case("bearer") && token.len() >= 4 {
            secrets.push(token.to_owned());
        }
    }
}

fn text(value: &str, secrets: &[String]) -> String {
    let mut value = value.to_owned();
    for secret in secrets {
        value = value.replace(secret, "[REDACTED]");
    }
    PROXY
        .replace_all(&TOKEN.replace_all(&value, "[REDACTED]"), "://[REDACTED]@")
        .into_owned()
}

fn prefix(value: &str, limit: usize) -> (&str, bool) {
    let mut end = value.len().min(limit);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (&value[..end], end < value.len())
}

fn snapshot(submission: &NativeSubmission, payload: &Value) -> io::Result<Value> {
    let pairs: Vec<(String, String)> = serde_json::from_value(payload["headers"].clone())?;
    let body: Value =
        serde_json::from_str(payload["body"].as_str().ok_or(io::ErrorKind::InvalidData)?)?;
    let mut secrets = vec![submission.bearer.clone()];
    collect(&submission.input, &mut secrets);
    collect(&body, &mut secrets);
    for (name, value) in &pairs {
        if sensitive(name) {
            add_secret(value, &mut secrets);
        }
    }
    secrets.retain(|value| value.len() >= 4);
    secrets.sort_by_key(|value| Reverse(value.len()));
    secrets.dedup();
    // 保留组装正文的顺序和空白，仅替换秘密，避免把重新序列化的结果称为原始快照。
    let original = payload["body"].as_str().ok_or(io::ErrorKind::InvalidData)?;
    let escaped: Vec<String> = secrets
        .iter()
        .map(|secret| {
            serde_json::to_string(secret).map(|encoded| encoded[1..encoded.len() - 1].to_owned())
        })
        .collect::<Result<_, _>>()?;
    let body = text(&text(original, &escaped), &secrets);
    let body = SECRET_STRING.replace_all(&body, "${1}\"[REDACTED]\"");
    let body = SECRET_SCALAR.replace_all(&body, "${1}\"[REDACTED]\"");
    let (body, truncated) = prefix(&body, BODY_LIMIT);
    let mut headers = Vec::new();
    let mut header_bytes = 0;
    let mut headers_truncated = false;
    for (name, value) in pairs {
        let name = text(&name, &secrets);
        let value = if sensitive(&name) {
            "[REDACTED]".into()
        } else {
            text(&value, &secrets)
        };
        header_bytes += name.len() + value.len();
        if header_bytes > HEADER_LIMIT {
            headers_truncated = true;
            break;
        }
        headers.push((name, value));
    }
    Ok(
        json!({"schema_version":1,"stage":"cc_prepared", "account_id":submission.request.account_id,
        "attempt_id":submission.request.request_id,"captured_at_ms":SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
        "headers":headers,"body":body,"body_truncated":truncated,"headers_truncated":headers_truncated,
        "original_body_bytes":payload["body_bytes"],"original_body_sha256":payload["body_sha256"]}),
    )
}

fn save(directory: &Path, submission: &NativeSubmission, payload: &Value) -> io::Result<()> {
    let id =
        Uuid::parse_str(&submission.request.request_id).map_err(|_| io::ErrorKind::InvalidInput)?;
    let bytes = serde_json::to_vec(&snapshot(submission, payload)?)?;
    let _guard = STORAGE.lock().map_err(|_| io::ErrorKind::Other)?;
    fs::create_dir_all(directory)?;
    if fs::symlink_metadata(directory)?.file_type().is_symlink() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    #[cfg(unix)]
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "tmp")
            && path
                .file_stem()
                .and_then(|name| name.to_str())
                .is_some_and(|name| Uuid::parse_str(name).is_ok())
        {
            // 同一把锁覆盖所有写入，遗留临时文件来自中断的旧写入。
            fs::remove_file(&path)?;
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "json")
            && path
                .file_stem()
                .and_then(|name| name.to_str())
                .is_some_and(|name| Uuid::parse_str(name).is_ok())
        {
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if !metadata.is_file() {
                return Err(io::ErrorKind::InvalidData.into());
            }
            files.push((metadata.modified()?, path, metadata.len()));
        }
    }
    files.sort_by_key(|(modified, _, _)| *modified);
    let mut total: u64 = files.iter().map(|(_, _, length)| length).sum();
    let mut count = files.len();
    // 写入前腾出空间，保证并发快照共享同一数量及字节上限。
    for (_, path, length) in files {
        if count < FILE_LIMIT && total + bytes.len() as u64 <= STORAGE_LIMIT {
            break;
        }
        // 独立的按天清理可能已回收该旧文件，缺失仍代表空间已释放。
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        count -= 1;
        total -= length;
    }
    let temporary = directory.join(format!("{id}.tmp"));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let result = (|| {
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        fs::rename(&temporary, directory.join(format!("{id}.json")))
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

/// 有界保存已验证事件；失败只影响采集状态。
/// @param directory 账号配置的私有目录；省略时关闭。
/// @param submission 当前尝试的归属与已知凭据。
/// @param payload 二进制输出的组装快照。
/// @return disabled、saved、dropped、timeout 或 failed，供诊断详情显示。
pub(crate) async fn record(
    directory: Option<&str>,
    submission: &NativeSubmission,
    payload: &Value,
) -> String {
    // 在克隆正文和申请磁盘写入之前执行同次模式门控。
    if submission.log_mode != RequestLogMode::Debug {
        return "disabled".into();
    }
    let Some(directory) = directory else {
        return "disabled".into();
    };
    let Ok(permit) = WRITERS.try_acquire() else {
        return "dropped".into();
    };
    let directory = directory.to_owned();
    let submission = submission.clone();
    let payload = payload.clone();
    let write = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        save(Path::new(&directory), &submission, &payload)
    });
    // 磁盘阻塞时及时交还模型响应，后台写入仍持有有界许可。
    let result = tokio::time::timeout(Duration::from_millis(250), write).await;
    if result.is_err() {
        return "timeout".into();
    }
    if matches!(result, Ok(Ok(Ok(())))) {
        "saved"
    } else {
        "failed"
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn info_short_circuits_before_creating_capture_files() {
        let mut submission = submission();
        submission.log_mode = RequestLogMode::Info;
        let root = std::env::temp_dir().join(format!("info-capture-{}", Uuid::new_v4()));
        assert_eq!(
            record(root.to_str(), &submission, &json!({"body":"无效采集输入"})).await,
            "disabled"
        );
        assert!(!root.exists());
        let mut value = serde_json::to_value(&submission).unwrap();
        value.as_object_mut().unwrap().remove("log_mode");
        assert_eq!(
            serde_json::from_value::<NativeSubmission>(value.clone())
                .unwrap()
                .log_mode,
            RequestLogMode::Info
        );
        value["log_mode"] = json!("trace");
        assert!(serde_json::from_value::<NativeSubmission>(value).is_err());
    }

    fn submission() -> NativeSubmission {
        serde_json::from_value(json!({
            "request":{"account_id":7,"environment_id":"synthetic","generation":1,"request_id":Uuid::new_v4().to_string(),
                "deadline_unix_ms":1,"method":"POST","target_url":"https://api.anthropic.com/v1/messages","headers":[],"preserve_header_casing":false},
            "log_mode":"debug","input":{"body":{"metadata":{"access_token":"echo-secret"}}},"bearer":"synthetic-bearer",
            "device_id":"a".repeat(64),"account_uuid":null,"organization_uuid":null,"email":"fixture@example.test","concurrency":2
        })).unwrap()
    }

    #[test]
    fn snapshot_preserves_duplicate_headers_and_masks_credentials_before_clipping() {
        let submission = submission();
        let body = json!({"model":"claude-sonnet-4-6","max_tokens":64,"messages":[{"role":"user","content":"synthetic-bearer echo-secret cookie-secret sk-ant-test"}],"metadata":{"password":"private-password"}}).to_string();
        let payload = json!({"headers":[["Authorization","Bearer synthetic-bearer"],["Cookie","cookie-secret"],["x-custom","first"],["x-custom","second"]],"body":body,"body_bytes":body.len(),"body_sha256":"a".repeat(64)});
        let value = snapshot(&submission, &payload).unwrap();
        let serialized = value.to_string();
        for secret in [
            "synthetic-bearer",
            "echo-secret",
            "cookie-secret",
            "private-password",
            "sk-ant-test",
        ] {
            assert!(!serialized.contains(secret));
        }
        assert_eq!(value["headers"][2], json!(["x-custom", "first"]));
        assert_eq!(value["headers"][3], json!(["x-custom", "second"]));
        let body: Value = serde_json::from_str(value["body"].as_str().unwrap()).unwrap();
        assert_eq!(body["max_tokens"], 64);
        assert_eq!(value["stage"], "cc_prepared");
        assert_eq!(value["body_truncated"], false);
        let original = "{ \"z\": 1, \"a\": 2, \"password\": 1234, \"api_key\": \"x\" }";
        let ordered = snapshot(&submission, &json!({"headers":[],"body":original})).unwrap();
        assert_eq!(
            ordered["body"],
            "{ \"z\": 1, \"a\": 2, \"password\": \"[REDACTED]\", \"api_key\": \"[REDACTED]\" }"
        );
        let payload =
            json!({"headers":[],"body":json!({"content":"中".repeat(BODY_LIMIT)}).to_string()});
        let clipped = snapshot(&submission, &payload).unwrap();
        assert_eq!(clipped["body_truncated"], true);
        assert!(clipped["body"].as_str().unwrap().len() <= BODY_LIMIT);
    }

    /// 核对被过滤的入站凭据在正文和扩展头中重复出现时仍先脱敏。
    /// @return 私有文件、普通头顺序及转发输入均符合合同。
    #[tokio::test]
    async fn incoming_header_credentials_are_redacted_across_persisted_content() {
        let directory = std::env::temp_dir().join(format!("native-capture-{}", Uuid::new_v4()));
        let mut submission = submission();
        submission.input["headers"] = json!([
            ["aUtHoRiZaTiOn", "bEaReR synthetic-client-key"],
            ["Cookie", "synthetic-client-cookie"],
            ["X-API-Key", "synthetic-client-api-key"],
            ["x-custom", "ordinary-value"]
        ]);
        let original = "{ \"z\": 1, \"messages\": [{\"role\":\"user\",\"content\":\"synthetic-client-key synthetic-client-cookie synthetic-client-api-key ordinary-value\"}], \"max_tokens\":64 }";
        let payload = json!({"headers":[
            ["authorization","Bearer synthetic-bearer"],
            ["x-reference","synthetic-client-key synthetic-client-cookie synthetic-client-api-key"],
            ["x-custom","first"],["x-custom","second"]
        ],"body":original,"body_bytes":original.len(),"body_sha256":"a".repeat(64)});
        let input_before = submission.input.clone();
        let payload_before = payload.clone();
        assert_eq!(
            record(directory.to_str(), &submission, &payload).await,
            "saved"
        );
        let stored =
            fs::read_to_string(directory.join(format!("{}.json", submission.request.request_id)))
                .unwrap();
        for secret in [
            "synthetic-client-key",
            "synthetic-client-cookie",
            "synthetic-client-api-key",
            "synthetic-bearer",
        ] {
            assert!(!stored.contains(secret));
        }
        let value: Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(value["headers"][1][1], "[REDACTED] [REDACTED] [REDACTED]");
        assert_eq!(value["headers"][2], json!(["x-custom", "first"]));
        assert_eq!(value["headers"][3], json!(["x-custom", "second"]));
        assert_eq!(
            value["body"],
            original
                .replace("synthetic-client-key", "[REDACTED]")
                .replace("synthetic-client-cookie", "[REDACTED]")
                .replace("synthetic-client-api-key", "[REDACTED]")
        );
        assert_eq!(value["original_body_bytes"], original.len());
        assert_eq!(submission.input, input_before);
        assert_eq!(payload, payload_before);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn retention_is_bounded_and_storage_failure_is_visible() {
        let directory = std::env::temp_dir().join(format!("native-capture-{}", Uuid::new_v4()));
        let mut submission = submission();
        let payload = json!({"headers":[],"body":"{}","body_bytes":2,"body_sha256":"a".repeat(64)});
        assert_eq!(record(None, &submission, &payload).await, "disabled");
        for _ in 0..51 {
            submission.request.request_id = Uuid::new_v4().to_string();
            save(&directory, &submission, &payload).unwrap();
        }
        assert_eq!(fs::read_dir(&directory).unwrap().count(), FILE_LIMIT);
        let metadata =
            fs::metadata(directory.join(format!("{}.json", submission.request.request_id)))
                .unwrap();
        #[cfg(unix)]
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        // 稀疏文件覆盖总量上限，避免测试实际写入大正文。
        for _ in 0..2 {
            let path = directory.join(format!("{}.json", Uuid::new_v4()));
            fs::File::create(path)
                .unwrap()
                .set_len(60 * 1024 * 1024)
                .unwrap();
        }
        submission.request.request_id = Uuid::new_v4().to_string();
        save(&directory, &submission, &payload).unwrap();
        let total: u64 = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum();
        assert!(total <= STORAGE_LIMIT);
        let invalid = directory.join("ordinary-file");
        fs::write(&invalid, b"blocked").unwrap();
        assert_eq!(
            record(invalid.to_str(), &submission, &payload).await,
            "failed"
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
