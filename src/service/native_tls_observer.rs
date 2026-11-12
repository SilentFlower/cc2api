//! 原生 TLS 旁路帮助进程的有界私有协议，采集失败独立于模型执行。

use std::env::var;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, from_slice, from_value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::task::AbortHandle;
use tokio::time::timeout;
use uuid::Uuid;

use crate::service::native_contract::NativeSubmission;

const FRAME_LIMIT: u64 = 6 * 1024 * 1024;

struct SlotState {
    generation: u64,
    starting: bool,
    consumed: bool,
    can_prepare: bool,
    ready: Option<NativeTlsObserver>,
    reason: &'static str,
    closed: bool,
    startup: Option<AbortHandle>,
}

/// 物理worker的后台单次探针预备槽，业务取用始终立即返回。
#[derive(Clone)]
pub(super) struct NativeTlsSlot {
    state: Arc<Mutex<SlotState>>,
    pid: u32,
    worker_id: String,
    image: String,
    path: Option<String>,
}

impl NativeTlsSlot {
    /// 只读核对当前采集槽。@return 实际状态和受控缺口；不启动帮助进程。
    pub(super) fn status(&self) -> (&'static str, Option<&'static str>) {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return ("unavailable", Some("WORKER_UNAVAILABLE"));
        }
        if let Some(observer) = state.ready.as_mut() {
            return if matches!(observer.child.try_wait(), Ok(None)) {
                ("ready", None)
            } else {
                ("unavailable", Some("OBSERVER_FAILED"))
            };
        }
        if state.starting || state.reason == "CAPTURE_PREPARING" {
            ("preparing", Some("CAPTURE_PREPARING"))
        } else {
            ("unavailable", Some(state.reason))
        }
    }

    /// 绑定已验证物理进程。@param pid 实际PID。@param worker_id 物理代次。@param image 精确SHA。@return 后台预备槽。
    pub(super) fn new(pid: u32, worker_id: &str, image: &str) -> Self {
        let path = var("CCRB_TLS_OBSERVER").ok();
        Self {
            state: Arc::new(Mutex::new(SlotState {
                generation: 0,
                starting: false,
                consumed: false,
                can_prepare: true,
                ready: None,
                reason: if path.is_some() {
                    "CAPTURE_PREPARING"
                } else {
                    "OBSERVER_NOT_CONFIGURED"
                },
                closed: false,
                startup: None,
            })),
            pid,
            worker_id: worker_id.into(),
            image: image.into(),
            path,
        }
    }

    /// 合并重复后台预备；不等待启动。@return 无。
    pub(super) fn warm(&self) {
        let Some(path) = self.path.clone() else {
            return;
        };
        let mut state = self.state.lock().unwrap();
        state.can_prepare = true;
        if state.closed || state.starting || state.ready.is_some() {
            return;
        }
        state.generation += 1;
        let generation = state.generation;
        state.starting = true;
        state.consumed = false;
        drop(state);
        let slot = self.clone();
        let handle = tokio::spawn(async move {
            let result = NativeTlsObserver::start(
                slot.pid,
                &slot.worker_id,
                &Uuid::new_v4().to_string(),
                &slot.image,
                &path,
            )
            .await;
            let mut state = slot.state.lock().unwrap();
            if state.closed || state.generation != generation {
                return;
            }
            state.starting = false;
            state.startup = None;
            if state.consumed {
                if let Err(reason) = result {
                    state.reason = reason;
                    return;
                }
                state.reason = "CAPTURE_PREPARING";
                let rearm = state.can_prepare;
                drop(state);
                drop(result);
                if rearm {
                    slot.warm();
                }
                return;
            }
            match result {
                Ok(observer) => state.ready = Some(observer),
                Err(reason) => state.reason = reason,
            }
        });
        let mut state = self.state.lock().unwrap();
        if !state.closed && state.generation == generation && state.starting {
            state.startup = Some(handle.abort_handle());
        } else {
            handle.abort();
        }
    }

    fn take(&self) -> Result<NativeTlsObserver, &'static str> {
        let mut state = self.state.lock().unwrap();
        state.can_prepare = false;
        state.consumed |= state.starting;
        state.ready.take().ok_or(state.reason)
    }

    /// 只读当前预备是否就绪，供隔离验收使用。@return 当前就绪状态。
    #[cfg(test)]
    pub(super) fn is_ready(&self) -> bool {
        self.state.lock().unwrap().ready.is_some()
    }

    /// 清除本轮不用的预备并使迟到启动失效。@return 无。
    pub(super) fn clear(&self) {
        let _ = self.take();
    }

    /// 物理worker退役后关闭预备及在途结果。@return 无。
    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.generation += 1;
        state.ready = None;
        if let Some(startup) = state.startup.take() {
            startup.abort();
        }
    }
}

/// 同一作业的收口守卫，取消执行future时在后台保存已经取得的副本。
pub(super) struct NativeTlsCapture {
    observer: Option<NativeTlsObserver>,
    reason: &'static str,
    directory: String,
    submission: NativeSubmission,
    worker_id: String,
    prepared: Option<Value>,
    finalize_on_drop: bool,
    slot: Option<NativeTlsSlot>,
}

impl NativeTlsCapture {
    /// 即时取得已就绪旁路。@param directory 私有目录。@param submission 已鉴权作业。@param worker_id 物理代次。@param slot 后台预备槽。@return 采集守卫。
    pub(super) fn take(
        directory: &str,
        submission: &NativeSubmission,
        worker_id: &str,
        slot: Option<&NativeTlsSlot>,
    ) -> Self {
        let result = slot
            .ok_or("OBSERVER_NOT_CONFIGURED")
            .and_then(NativeTlsSlot::take);
        let (observer, reason) = match result {
            Ok(observer) => (Some(observer), "CAPTURE_INTERRUPTED"),
            Err(reason) => (None, reason),
        };
        Self {
            observer,
            reason,
            directory: directory.into(),
            submission: submission.clone(),
            worker_id: worker_id.into(),
            prepared: None,
            finalize_on_drop: true,
            slot: slot.cloned(),
        }
    }

    /// 绑定同次组装快照。@param payload 组装事件。@param policy 同次决策。@return 无。
    pub(super) fn prepared(&mut self, payload: &Value, policy: Option<&Value>) {
        if self.observer.is_none() {
            return;
        }
        let mut payload = payload.clone();
        if let Some(policy) = policy {
            payload["_header_policy"] = policy.clone();
        }
        self.prepared = Some(payload);
    }

    /// 正常收口并保存。@return 有界持久状态。
    pub(super) async fn finish(&mut self) -> String {
        let Some(mut payload) = self.prepared.clone() else {
            return "disabled".into();
        };
        payload["_wire_final"] = match self.observer.take() {
            Some(observer) => observer.finish(&payload["headers"]).await,
            None => missing(self.reason),
        };
        payload["_wire_final"]["worker_id"] = json!(self.worker_id);
        payload["_wire_final"]["request_id"] = json!(self.submission.request.request_id);
        payload["_wire_final"]["binding_generation"] = json!(self.submission.request.generation);
        if payload["_wire_final"]["complete"] == true {
            if let Some(slot) = &self.slot {
                slot.warm();
            }
        }
        let result =
            super::native_capture::record(Some(&self.directory), &self.submission, &payload, None)
                .await;
        if !matches!(result.as_str(), "saved" | "disabled") {
            tracing::warn!(request_id = %self.submission.request.request_id, worker_id = %self.worker_id,
                layer = "native_tls_capture", reason = result.as_str(), "原生TLS后台证据保存结果待核对");
        }
        self.prepared = None;
        result
    }

    /// 后台收口TLS副本，独立于响应交付。@return 无。
    pub(super) fn background(mut self) {
        if self.prepared.is_none() {
            return;
        }
        self.finalize_on_drop = false;
        tokio::spawn(async move {
            let _ = self.finish().await;
        });
    }

    /// 为初始快照提供明确进度。@return 同次缺证或采集中状态。
    pub(super) fn pending(&self) -> Value {
        let mut value = missing(if self.observer.is_some() {
            "CAPTURE_PENDING"
        } else {
            self.reason
        });
        value["worker_id"] = json!(self.worker_id);
        value["request_id"] = json!(self.submission.request.request_id);
        value["binding_generation"] = json!(self.submission.request.generation);
        value
    }
}

impl Drop for NativeTlsCapture {
    fn drop(&mut self) {
        if !self.finalize_on_drop || self.prepared.is_none() {
            return;
        }
        let mut pending = Self {
            observer: self.observer.take(),
            reason: self.reason,
            directory: self.directory.clone(),
            submission: self.submission.clone(),
            worker_id: self.worker_id.clone(),
            prepared: self.prepared.take(),
            // 运行时关闭可能取消后台任务；后台副本的Drop只释放资源，避免递归派生任务。
            finalize_on_drop: false,
            slot: self.slot.take(),
        };
        tokio::spawn(async move {
            let _ = pending.finish().await;
        });
    }
}

/// 单次作业帮助进程；Drop关闭管道并终止进程，内核随FD释放探针。
pub(super) struct NativeTlsObserver {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    capture_id: String,
    worker_id: String,
    worker_pid: u32,
}

/// 生成固定原因的缺证对象。@param reason 受控原因码。@return 私有阶段状态。
pub(super) fn missing(reason: &str) -> Value {
    json!({"source":"native_tls_uprobe","representation":"http_entity_from_tls_accepted_write",
        "status":"unavailable","reason":reason,"headers":[],"body":"","body_truncated":false,
        "headers_truncated":false,"observed_body_bytes":null,"body_sha256":null,"complete":false,"flushed":null})
}

impl NativeTlsObserver {
    /// 后台绑定同一worker。@param pid 已验证PID。@param worker_id 物理代次。@param capture_id 独立采集代次。@param image 已验证执行体SHA。@param path 冻结帮助进程路径。@return 有界就绪实例或固定原因。
    pub(super) async fn start(
        pid: u32,
        worker_id: &str,
        capture_id: &str,
        image: &str,
        path: &str,
    ) -> Result<Self, &'static str> {
        if !Path::new(path).is_absolute() {
            return Err("OBSERVER_CONFIGURATION");
        }
        let mut child = Command::new("python3")
            .arg(path)
            .args([
                "--pid",
                &pid.to_string(),
                "--worker",
                worker_id,
                "--capture",
                capture_id,
                "--image",
                image,
            ])
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("LANG", "C.UTF-8")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| "OBSERVER_START_FAILED")?;
        let input = child.stdin.take().ok_or("OBSERVER_CHANNEL")?;
        let output = BufReader::new(child.stdout.take().ok_or("OBSERVER_CHANNEL")?);
        let mut observer = Self {
            child,
            input,
            output,
            capture_id: capture_id.into(),
            worker_id: worker_id.into(),
            worker_pid: pid,
        };
        let ready = timeout(Duration::from_secs(2), observer.read())
            .await
            .map_err(|_| "OBSERVER_START_TIMEOUT")??;
        if ready["ready"] != true
            || ready["capture_id"] != capture_id
            || ready["worker_id"] != worker_id
        {
            return Err(match ready["reason"].as_str() {
                Some("IMAGE_NOT_ADMITTED") => "IMAGE_NOT_ADMITTED",
                Some("IMAGE_MISMATCH") => "IMAGE_MISMATCH",
                Some("BPF_LOAD_FAILED") => "BPF_LOAD_FAILED",
                Some("PROBE_ATTACH_FAILED") => "PROBE_ATTACH_FAILED",
                Some("PROBE_ARTIFACT") => "PROBE_ARTIFACT",
                Some("WORKER_OWNERSHIP") => "WORKER_OWNERSHIP",
                _ => "OBSERVER_UNAVAILABLE",
            });
        }
        Ok(observer)
    }

    async fn read(&mut self) -> Result<Value, &'static str> {
        let mut bytes = Vec::new();
        AsyncReadExt::take(&mut self.output, FRAME_LIMIT + 1)
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|_| "OBSERVER_CHANNEL")?;
        if bytes.len() as u64 > FRAME_LIMIT || bytes.last() != Some(&b'\n') {
            return Err("OBSERVER_FRAME_INVALID");
        }
        let value: Value = from_slice(&bytes).map_err(|_| "OBSERVER_FRAME_INVALID")?;
        if value["schema_version"] != 1 {
            return Err("OBSERVER_FRAME_INVALID");
        }
        Ok(value)
    }

    /// 以实际组装头绑定并收口。@param headers 同次cc_prepared有序头。@return 最终证据或固定缺证。
    pub(super) async fn finish(mut self, headers: &Value) -> Value {
        let operation = async {
            let pairs: Vec<(String, String)> =
                from_value(headers.clone()).map_err(|_| "OBSERVER_BINDING")?;
            let ids: Vec<_> = pairs
                .iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("x-client-request-id"))
                .collect();
            if ids.len() != 1 {
                return Err("OBSERVER_BINDING");
            }
            let command = json!({"capture_id":self.capture_id,"client_request_id":ids[0].1})
                .to_string()
                + "\n";
            self.input
                .write_all(command.as_bytes())
                .await
                .map_err(|_| "OBSERVER_CHANNEL")?;
            let value = self.read().await?;
            if value["capture_id"] != self.capture_id
                || value["worker_id"] != self.worker_id
                || value["worker_pid"].as_u64() != Some(self.worker_pid as u64)
                || value["source"] != "native_tls_uprobe"
                || value["representation"] != "http_entity_from_tls_accepted_write"
                || !matches!(
                    value["status"].as_str(),
                    Some("complete" | "incomplete" | "unavailable")
                )
                || !value["flushed"].is_null()
            {
                return Err("OBSERVER_FRAME_INVALID");
            }
            Ok(value)
        };
        let result = timeout(Duration::from_secs(2), operation).await;
        let _ = self.child.start_kill();
        // 回收等待也保持有界，模型请求的生命周期由现有worker独立控制。
        let _ = timeout(Duration::from_millis(25), self.child.wait()).await;
        match result {
            Ok(Ok(value)) => value,
            Ok(Err(reason)) => missing(reason),
            Err(_) => missing("OBSERVER_FINISH_TIMEOUT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::to_string;
    use std::env::temp_dir;
    use std::fs;
    use tokio::runtime::Builder;
    use tokio::time::sleep;
    use uuid::Uuid;

    /// 能力状态核对帮助进程实际存活，退出后旧ready立即失效。@return 无。
    #[tokio::test]
    async fn status_checks_live_observer_and_closed_slot() {
        let directory = temp_dir().join(format!("native-status-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let script = directory.join("observer.py");
        fs::write(&script, "import json,sys\nprint(json.dumps({'schema_version':1,'ready':True,'capture_id':'capture','worker_id':'worker'}),flush=True)\nsys.stdin.buffer.read()\n").unwrap();
        let slot = NativeTlsSlot::new(1, "worker", "image");
        let observer =
            NativeTlsObserver::start(1, "worker", "capture", "image", script.to_str().unwrap())
                .await
                .unwrap();
        slot.state.lock().unwrap().ready = Some(observer);
        assert_eq!(slot.status(), ("ready", None));
        let mut observer = slot.state.lock().unwrap().ready.take().unwrap();
        observer.child.kill().await.unwrap();
        observer.child.wait().await.unwrap();
        slot.state.lock().unwrap().ready = Some(observer);
        assert_eq!(slot.status(), ("unavailable", Some("OBSERVER_FAILED")));
        slot.close();
        assert_eq!(slot.status(), ("unavailable", Some("WORKER_UNAVAILABLE")));
        fs::remove_dir_all(directory).unwrap();
    }

    /// 慢启动期间业务即时取用，反复请求合并为每worker一个在途启动。@return 无。
    #[tokio::test]
    async fn slow_prepare_is_nonblocking_bounded_and_stale_safe() {
        let directory = temp_dir().join(format!("native-slot-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let script = directory.join("observer.py");
        let marker = directory.join("starts");
        let release = directory.join("release");
        fs::write(&script, format!(r#"import sys,time,json,os
with open({},'a') as output: output.write(str(os.getpid())+'\n')
while not os.path.exists({}): time.sleep(0.01)
arguments=sys.argv
print(json.dumps({{'schema_version':1,'ready':True,'capture_id':arguments[arguments.index('--capture')+1],'worker_id':arguments[arguments.index('--worker')+1]}}),flush=True)
sys.stdin.buffer.read()
"#, to_string(&marker.to_string_lossy()).unwrap(), to_string(&release.to_string_lossy()).unwrap())).unwrap();
        let mut slot = NativeTlsSlot::new(1, "synthetic-worker", "synthetic-image");
        slot.path = Some(script.to_string_lossy().into_owned());
        slot.state.lock().unwrap().reason = "CAPTURE_PREPARING";
        slot.warm();
        timeout(Duration::from_secs(2), async {
            while !marker.exists() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        for _ in 0..100 {
            assert!(matches!(slot.take(), Err("CAPTURE_PREPARING")));
            slot.warm();
        }
        // 最后一次取用标记在途结果失效，后台完成后只释放原进程。
        assert!(matches!(slot.take(), Err("CAPTURE_PREPARING")));
        fs::write(&release, "ready").unwrap();
        timeout(Duration::from_secs(2), async {
            while slot.state.lock().unwrap().starting {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(fs::read_to_string(&marker).unwrap().lines().count(), 1);
        assert!(!slot.state.lock().unwrap().starting);
        assert!(slot.state.lock().unwrap().ready.is_none());
        slot.warm();
        slot.close();
        sleep(Duration::from_millis(700)).await;
        assert!(slot.state.lock().unwrap().ready.is_none());
        assert!(slot.state.lock().unwrap().closed);
        fs::remove_dir_all(directory).unwrap();
    }

    fn capture(directory: &Path) -> NativeTlsCapture {
        let submission = from_value(json!({
            "request":{"account_id":7,"environment_id":"synthetic","generation":9,"request_id":Uuid::new_v4().to_string(),
                "deadline_unix_ms":1,"method":"POST","target_url":"https://api.anthropic.com/v1/messages","headers":[],"preserve_header_casing":false},
            "log_mode":"debug","input":{},"bearer":"synthetic-guard-bearer","device_id":"a".repeat(64),
            "account_uuid":null,"organization_uuid":null,"email":"fixture@example.test","concurrency":2
        })).unwrap();
        NativeTlsCapture {
            observer: None,
            reason: "CAPTURE_INTERRUPTED",
            directory: directory.to_string_lossy().into_owned(),
            submission,
            worker_id: Uuid::new_v4().to_string(),
            slot: None,
            prepared: Some(json!({"body":"{}","headers":[],"body_bytes":2,
                "body_sha256":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"})),
            finalize_on_drop: true,
        }
    }

    /// 取消收口保持旧作业归属及显式缺证。@return 无。
    #[tokio::test]
    async fn dropped_capture_finishes_same_request() {
        let directory = temp_dir().join(format!("native-cancel-{}", Uuid::new_v4()));
        let guard = capture(&directory);
        let id = guard.submission.request.request_id.clone();
        let file = directory.join(format!("{id}.json"));
        drop(guard);
        timeout(Duration::from_secs(2), async {
            while !file.exists() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let value: Value = from_slice(&fs::read(file).unwrap()).unwrap();
        assert_eq!(value["wire_final"]["request_id"], id);
        assert_eq!(value["wire_final"]["binding_generation"], 9);
        assert_eq!(value["wire_final"]["reason"], "CAPTURE_INTERRUPTED");
        assert!(value["wire_final"]["body_sha256"].is_null());
        fs::remove_dir_all(directory).unwrap();
    }

    /// 运行时取消尚未开始的后台收口时，Drop只释放一次。@return 无。
    #[test]
    fn shutdown_does_not_recursively_spawn_capture() {
        let directory = temp_dir().join(format!("native-shutdown-{}", Uuid::new_v4()));
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            drop(capture(&directory));
        });
        runtime.shutdown_timeout(Duration::from_millis(30));
        let _ = fs::remove_dir_all(directory);
    }
}
