//! 部署级维护准入。流响应持有活动证明直到 EOF 或断连。

#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
use std::task::{Context, Poll};

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Error as AxumError, Json};
use http_body::{Body as HttpBody, Frame, SizeHint};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::error::AppError;
use crate::service::runtime_bridge::{update_admitted, update_control_admitted};

static ACTIVITIES: LazyLock<Mutex<usize>> = LazyLock::new(|| Mutex::new(0));

/// 由 root 更新服务原子发布的双方共用门禁。
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentGate {
    schema_version: u32,
    deployment: String,
    operation_id: Option<String>,
    epoch: String,
    phase: String,
    target_release: Option<String>,
    revision: String,
    created_at: String,
}

/// 一项真实在途活动的 RAII 证明。
pub struct UpdateActivity;

struct TrackedBody {
    inner: Body,
    activity: Option<UpdateActivity>,
}

impl HttpBody for TrackedBody {
    type Data = Bytes;
    type Error = AxumError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_frame(cx);
        if matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            this.activity.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for UpdateActivity {
    fn drop(&mut self) {
        let mut active = ACTIVITIES.lock().unwrap_or_else(|error| error.into_inner());
        *active = active.saturating_sub(1);
    }
}

#[cfg(unix)]
fn read_gate(path: &Path, deployment: &str) -> Result<DeploymentGate, AppError> {
    let error = || AppError::ServiceUnavailable("平台维护状态需要对账".into());
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| error())?;
    let info = file.metadata().map_err(|_| error())?;
    if !info.is_file()
        || info.len() > 8192
        || info.mode() & 0o077 != 0
        || info.nlink() != 1
        || info.uid() != unsafe { libc::geteuid() }
    {
        return Err(error());
    }
    let mut raw = Vec::new();
    file.by_ref()
        .take(8193)
        .read_to_end(&mut raw)
        .map_err(|_| error())?;
    if raw.len() > 8192 {
        return Err(error());
    }
    let gate: DeploymentGate = serde_json::from_slice(&raw).map_err(|_| error())?;
    if gate.schema_version != 1
        || deployment.is_empty()
        || gate.deployment != deployment
        || !matches!(gate.phase.as_str(), "idle" | "blocked")
        || Uuid::parse_str(&gate.epoch).is_err()
        || Uuid::parse_str(&gate.revision).is_err()
        || (gate.phase == "blocked"
            && gate
                .operation_id
                .as_ref()
                .is_none_or(|value| Uuid::parse_str(value).is_err()))
        || (gate.phase == "idle" && (gate.operation_id.is_some() || gate.target_release.is_some()))
    {
        return Err(error());
    }
    Ok(gate)
}

#[cfg(not(unix))]
fn read_gate(_path: &Path, _deployment: &str) -> Result<DeploymentGate, AppError> {
    Err(AppError::ServiceUnavailable(
        "当前平台未接入 Linux 系统更新底座".into(),
    ))
}

fn gate() -> Result<Option<DeploymentGate>, AppError> {
    let directory = std::env::var("V3_UPDATE_GATE_DIR").unwrap_or_default();
    if directory.is_empty() {
        return Ok(None);
    }
    read_gate(
        &Path::new(&directory).join("admission.json"),
        &std::env::var("V3_DEPLOYMENT_ID").unwrap_or_default(),
    )
    .map(Some)
}

/// 数据库初始化前核对底座维护记录，未知门禁禁止继续启动。
/// @return 已明确识别启用状态或门禁错误。
pub fn validate_startup() -> Result<(), AppError> {
    gate().map(|_| ())
}

/// 在同一锁内核验准入并登记活动，后台调用亦使用该入口。
/// @return 活动证明；维护中拒绝新工作。
pub fn admission() -> Result<UpdateActivity, AppError> {
    let mut active = ACTIVITIES.lock().unwrap_or_else(|error| error.into_inner());
    if gate()?.is_some_and(|value| value.phase != "idle") {
        return Err(AppError::ServiceUnavailable(
            "平台正在维护，请稍后重试".into(),
        ));
    }
    *active += 1;
    Ok(UpdateActivity)
}

/// 允许原在途账号任务完成必要凭据解析，和 ACK 共用登记锁。
/// @param admitted 只读复核原账号仍持有活动或运行租约。
/// @return 被跟踪的有限续接活动。
pub fn continuation(admitted: impl FnOnce() -> bool) -> Result<UpdateActivity, AppError> {
    let mut active = ACTIVITIES.lock().unwrap_or_else(|error| error.into_inner());
    if gate()?.is_some_and(|value| value.phase != "idle") && !admitted() {
        return Err(AppError::ServiceUnavailable(
            "平台维护中，账号没有在途工作".into(),
        ));
    }
    *active += 1;
    Ok(UpdateActivity)
}

/// 登记有限内部认领后核对原运行作业，防止暂停时打断原 CLI 准备。
/// @param account_id 原账号。
/// @param owner 原作业来源。
/// @return 已跟踪且有实际登记的续接活动。
pub async fn cli_admission(account_id: i64, owner: &str) -> Result<UpdateActivity, AppError> {
    let (activity, blocked) = {
        let mut active = ACTIVITIES.lock().unwrap_or_else(|error| error.into_inner());
        let blocked = gate()?.is_some_and(|value| value.phase != "idle");
        *active += 1;
        (UpdateActivity, blocked)
    };
    if blocked && !update_admitted(account_id, owner).await? {
        return Err(AppError::ServiceUnavailable(
            "平台维护中，原任务登记已经失效".into(),
        ));
    }
    Ok(activity)
}

/// 在实际准入锁内返回 ACK 和活动计数，未知状态保持明确失败。
/// @return 当前门禁及进程活动。
pub fn snapshot() -> Result<Value, AppError> {
    let active = ACTIVITIES.lock().unwrap_or_else(|error| error.into_inner());
    let Some(gate) = gate()? else {
        return Ok(json!({"enabled":false,"phase":"idle","epoch":null,"active":*active}));
    };
    let mut value =
        serde_json::to_value(gate).map_err(|_| AppError::Internal("维护状态编码失败".into()))?;
    value["active"] = json!(*active);
    value["enabled"] = json!(true);
    Ok(value)
}

fn exempt(method: &Method, path: &str) -> bool {
    path == "/admin/runtime/system-update/status"
        || path == "/admin/runtime/system-update/ack"
        || path.starts_with("/runtime/api/system-updates/")
        || path == "/runtime/logout"
        || path == "/admin/runtime/session"
        || (path.starts_with("/runtime/api/accounts/login/")
            && (path.ends_with("/code") || path.ends_with("/commit") || *method == Method::DELETE))
        || (path.starts_with("/admin/runtime/accounts/") && path.ends_with("/lease"))
        || (path.starts_with("/admin/accounts/") && path.ends_with("/oauth-credentials/resolve"))
        || (matches!(*method, Method::GET | Method::HEAD)
            && (path == "/"
                || path == "/login"
                || path == "/updates"
                || path == "/environments"
                || path.starts_with("/assets/")
                || path.starts_with("/admin/")
                || path.starts_with("/runtime/")))
}

/// 在 HTTP 边界登记完整请求和响应流，管理员观察与受证恢复独立准入。
/// @param request 当前 HTTP 请求。
/// @param next 原处理链。
/// @return 原始响应或维护错误，业务字节保持原样。
pub async fn middleware(request: Request, next: Next) -> Response {
    if exempt(request.method(), request.uri().path()) {
        return next.run(request).await;
    }
    let activity = match admission() {
        Ok(activity) => activity,
        Err(_) => {
            let admission_id = request
                .headers()
                .get("x-v3-update-admission")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            let supplied = request
                .headers()
                .get("x-v3-service-token")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            let expected = std::env::var("V3_INTERNAL_TOKEN").unwrap_or_default();
            let valid = admission_id.len() == 32
                && admission_id.bytes().all(|value| value.is_ascii_hexdigit())
                && expected.len() >= 32
                && bool::from(expected.as_bytes().ct_eq(supplied.as_bytes()));
            let continuation = if valid {
                let activity = {
                    let mut active = ACTIVITIES.lock().unwrap_or_else(|error| error.into_inner());
                    if gate().is_ok() {
                        *active += 1;
                        Some(UpdateActivity)
                    } else {
                        None
                    }
                };
                if activity.is_some()
                    && update_control_admitted(admission_id).await.unwrap_or(false)
                {
                    activity
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(activity) = continuation {
                let response = next.run(request).await;
                let (parts, body) = response.into_parts();
                return Response::from_parts(
                    parts,
                    Body::new(TrackedBody {
                        inner: body,
                        activity: Some(activity),
                    }),
                );
            }
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [("retry-after", "5")],
                Json(json!({
            "type":"error", "error":{"type":"overloaded_error","code":"system_maintenance",
            "message":"平台正在维护，请稍后重试"}})),
            )
                .into_response();
        }
    };
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(TrackedBody {
            inner: body,
            activity: Some(activity),
        }),
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::{ACTIVITIES, TrackedBody, UpdateActivity, read_gate};
    use axum::body::{Body, Bytes};
    use futures_util::stream;
    use http_body_util::BodyExt;
    use serde_json::json;
    use std::env::temp_dir;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn response_guard_remains_until_body_eof_or_error() {
        let mut body = {
            *ACTIVITIES.lock().unwrap() += 1;
            TrackedBody {
                inner: Body::from_stream(stream::iter([
                    Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
                    Ok(Bytes::from_static(b"last")),
                ])),
                activity: Some(UpdateActivity),
            }
        };
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "first"
        );
        assert!(body.activity.is_some());
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "last"
        );
        assert!(body.activity.is_some());
        assert!(body.frame().await.is_none());
        assert!(body.activity.is_none());
        *ACTIVITIES.lock().unwrap() += 1;
        let mut failed = TrackedBody {
            inner: Body::from_stream(stream::iter([Err::<Bytes, _>(std::io::Error::other(
                "合成断流",
            ))])),
            activity: Some(UpdateActivity),
        };
        assert!(failed.frame().await.unwrap().is_err());
        assert!(failed.activity.is_none());
    }

    #[test]
    fn gate_contract_rejects_missing_unknown_and_bad_owner() {
        let path = temp_dir().join(format!("v3-update-gate-{}.json", uuid::Uuid::new_v4()));
        assert!(read_gate(&path, "test").is_err());
        let mut value = json!({"schema_version":1,"deployment":"test","operation_id":null,
            "epoch":uuid::Uuid::new_v4().to_string(),"phase":"idle","target_release":null,
            "revision":uuid::Uuid::new_v4().to_string(),"created_at":"2026-11-11T00:00:00Z"});
        fs::write(&path, value.to_string()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(read_gate(&path, "test").is_ok());
        assert!(read_gate(&path, "other").is_err());
        value["phase"] = json!("unknown");
        fs::write(&path, value.to_string()).unwrap();
        assert!(read_gate(&path, "test").is_err());
        value["phase"] = json!("idle");
        value["extra"] = json!(true);
        fs::write(&path, value.to_string()).unwrap();
        assert!(read_gate(&path, "test").is_err());
        fs::remove_file(path).unwrap();
    }
}
