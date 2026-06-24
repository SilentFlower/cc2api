use chrono::{DateTime, Utc};
use rand::Rng;
use serde_json::Value;

use super::account::{Account, CanonicalEnvData, CanonicalProcessData, CanonicalPromptEnvData};
use crate::service::version_profile::{
    DEFAULT_CLAUDE_CODE_BUILD_TIME, DEFAULT_CLAUDE_CODE_VERSION, DEFAULT_CLAUDE_CODE_VERSION_BASE,
};

const DEFAULT_ACCOUNT_ID_SEED: &str = "account";
const UUID_SEED_NAMESPACE: &str = "cc2api-identity-profile";

#[derive(Clone)]
struct EnvPreset {
    env: CanonicalEnvData,
    prompt: CanonicalPromptEnvData,
    process: CanonicalProcessData,
}

fn profile_presets() -> Vec<EnvPreset> {
    vec![
        // --- darwin arm64 (8 presets) ---
        dprof(
            "arm64",
            "v22.15.0",
            "iTerm.app",
            "zsh",
            "Darwin 24.4.0",
            "/Users/user/projects",
            "npm,pnpm",
        ),
        dprof(
            "arm64",
            "v24.3.0",
            "Apple_Terminal",
            "zsh",
            "Darwin 24.5.0",
            "/Users/user/code",
            "npm,yarn",
        ),
        dprof(
            "arm64",
            "v22.15.0",
            "vscode",
            "zsh",
            "Darwin 24.4.0",
            "/Users/user/workspace",
            "npm,pnpm",
        ),
        dprof(
            "arm64",
            "v24.3.0",
            "WarpTerminal",
            "zsh",
            "Darwin 24.5.0",
            "/Users/user/dev",
            "npm",
        ),
        dprof(
            "arm64",
            "v22.15.0",
            "kitty",
            "zsh",
            "Darwin 24.4.0",
            "/Users/user/src",
            "npm,yarn,pnpm",
        ),
        dprof(
            "arm64",
            "v24.3.0",
            "iTerm.app",
            "zsh",
            "Darwin 24.5.0",
            "/Users/user/projects",
            "npm",
        ),
        dprof(
            "arm64",
            "v22.15.0",
            "tmux",
            "zsh",
            "Darwin 24.4.0",
            "/Users/user/repo",
            "npm,pnpm",
        ),
        dprof(
            "arm64",
            "v24.3.0",
            "ghostty",
            "zsh",
            "Darwin 24.5.0",
            "/Users/user/work",
            "npm,yarn",
        ),
        // --- darwin x64 (4 presets) ---
        dprof(
            "x64",
            "v22.15.0",
            "iTerm.app",
            "zsh",
            "Darwin 23.6.0",
            "/Users/user/projects",
            "npm,yarn",
        ),
        dprof(
            "x64",
            "v24.3.0",
            "Apple_Terminal",
            "zsh",
            "Darwin 23.6.0",
            "/Users/user/code",
            "npm,pnpm",
        ),
        dprof(
            "x64",
            "v22.15.0",
            "vscode",
            "zsh",
            "Darwin 23.6.0",
            "/Users/user/workspace",
            "npm",
        ),
        dprof(
            "x64",
            "v24.3.0",
            "iTerm.app",
            "zsh",
            "Darwin 23.6.0",
            "/Users/user/dev",
            "npm,pnpm",
        ),
        // --- linux (6 presets) ---
        lprof(
            "v22.15.0",
            "gnome-terminal",
            "bash",
            "Linux 6.8.0-60-generic",
            "/home/user/projects",
            "npm,pnpm",
            "ubuntu",
            "24.04",
            "6.8.0-60-generic",
        ),
        lprof(
            "v24.3.0",
            "ssh-session",
            "bash",
            "Linux 6.5.0-1025-aws",
            "/home/user/work",
            "npm",
            "ubuntu",
            "22.04",
            "6.5.0-1025-aws",
        ),
        lprof(
            "v22.15.0",
            "xterm-256color",
            "zsh",
            "Linux 6.6.32",
            "/home/user/src",
            "npm,yarn",
            "debian",
            "12",
            "6.6.32",
        ),
        lprof(
            "v24.3.0",
            "vscode",
            "bash",
            "Linux 6.8.0-60-generic",
            "/home/user/workspace",
            "npm,pnpm",
            "ubuntu",
            "24.04",
            "6.8.0-60-generic",
        ),
        lprof(
            "v22.15.0",
            "tmux",
            "bash",
            "Linux 6.1.0-21-amd64",
            "/home/user/repo",
            "npm",
            "debian",
            "12",
            "6.1.0-21-amd64",
        ),
        lprof(
            "v24.3.0",
            "alacritty",
            "zsh",
            "Linux 6.9.3-arch1-1",
            "/home/user/dev",
            "npm,yarn",
            "arch",
            "rolling",
            "6.9.3-arch1-1",
        ),
        // --- win32 (4 presets) ---
        wprof(
            "v22.15.0",
            "windows-terminal",
            "Windows 10 Pro 10.0.19045",
            "/c/Users/user/projects",
            "npm,pnpm",
        ),
        wprof(
            "v24.3.0",
            "vscode",
            "Windows 11 Pro 10.0.22631",
            "/c/Users/user/workspace",
            "npm,yarn",
        ),
        wprof(
            "v22.15.0",
            "mingw64",
            "Windows 10 Pro 10.0.19045",
            "/c/Users/user/src",
            "npm",
        ),
        wprof(
            "v24.3.0",
            "windows-terminal",
            "Windows 11 Pro 10.0.22631",
            "/c/Users/user/dev",
            "npm,pnpm",
        ),
    ]
}

/// 构造 darwin 平台预设。
fn dp(arch: &str, node: &str, term: &str, pm: &str) -> CanonicalEnvData {
    CanonicalEnvData {
        platform: "darwin".into(),
        platform_raw: "darwin".into(),
        arch: arch.into(),
        node_version: node.into(),
        terminal: term.into(),
        package_managers: pm.into(),
        runtimes: "node".into(),
        is_claude_ai_auth: true,
        version: DEFAULT_CLAUDE_CODE_VERSION.into(),
        version_base: DEFAULT_CLAUDE_CODE_VERSION_BASE.into(),
        build_time: DEFAULT_CLAUDE_CODE_BUILD_TIME.into(),
        deployment_environment: "unknown-darwin".into(),
        vcs: "git".into(),
        ..Default::default()
    }
}

fn dprof(
    arch: &str,
    node: &str,
    term: &str,
    shell: &str,
    os_version: &str,
    working_dir: &str,
    pm: &str,
) -> EnvPreset {
    EnvPreset {
        env: dp(arch, node, term, pm),
        prompt: CanonicalPromptEnvData {
            platform: "darwin".into(),
            shell: shell.into(),
            os_version: os_version.into(),
            working_dir: working_dir.into(),
        },
        process: process_profile(360_000_000, 620_000_000, 58_000_000, 112_000_000),
    }
}

/// 构造 linux 平台预设。
fn lp(node: &str, term: &str, pm: &str) -> CanonicalEnvData {
    CanonicalEnvData {
        platform: "linux".into(),
        platform_raw: "linux".into(),
        arch: "x64".into(),
        node_version: node.into(),
        terminal: term.into(),
        package_managers: pm.into(),
        runtimes: "node".into(),
        is_claude_ai_auth: true,
        version: DEFAULT_CLAUDE_CODE_VERSION.into(),
        version_base: DEFAULT_CLAUDE_CODE_VERSION_BASE.into(),
        build_time: DEFAULT_CLAUDE_CODE_BUILD_TIME.into(),
        deployment_environment: "unknown-linux".into(),
        vcs: "git".into(),
        ..Default::default()
    }
}

fn lprof(
    node: &str,
    term: &str,
    shell: &str,
    os_version: &str,
    working_dir: &str,
    pm: &str,
    distro_id: &str,
    distro_version: &str,
    kernel: &str,
) -> EnvPreset {
    let mut env = lp(node, term, pm);
    env.linux_distro_id = distro_id.into();
    env.linux_distro_version = distro_version.into();
    env.linux_kernel = kernel.into();
    EnvPreset {
        env,
        prompt: CanonicalPromptEnvData {
            platform: "linux".into(),
            shell: shell.into(),
            os_version: os_version.into(),
            working_dir: working_dir.into(),
        },
        process: process_profile(300_000_000, 520_000_000, 48_000_000, 96_000_000),
    }
}

/// 构造 win32 平台预设。
fn wp(node: &str, term: &str, pm: &str) -> CanonicalEnvData {
    CanonicalEnvData {
        platform: "win32".into(),
        platform_raw: "win32".into(),
        arch: "x64".into(),
        node_version: node.into(),
        terminal: term.into(),
        package_managers: pm.into(),
        runtimes: "node".into(),
        is_claude_ai_auth: true,
        version: DEFAULT_CLAUDE_CODE_VERSION.into(),
        version_base: DEFAULT_CLAUDE_CODE_VERSION_BASE.into(),
        build_time: DEFAULT_CLAUDE_CODE_BUILD_TIME.into(),
        deployment_environment: "unknown-win32".into(),
        vcs: "git".into(),
        ..Default::default()
    }
}

fn wprof(node: &str, term: &str, os_version: &str, working_dir: &str, pm: &str) -> EnvPreset {
    EnvPreset {
        env: wp(node, term, pm),
        prompt: CanonicalPromptEnvData {
            platform: "win32".into(),
            shell: "bash (use Unix shell syntax, not Windows - e.g., /dev/null not NUL, forward slashes in paths)".into(),
            os_version: os_version.into(),
            working_dir: working_dir.into(),
        },
        process: process_profile(420_000_000, 680_000_000, 64_000_000, 128_000_000),
    }
}

fn process_profile(
    rss_min: i64,
    rss_max: i64,
    heap_min: i64,
    heap_max: i64,
) -> CanonicalProcessData {
    CanonicalProcessData {
        constrained_memory: 0,
        rss_range: [rss_min, rss_max],
        heap_total_range: [heap_min, heap_max],
        heap_used_range: [heap_min / 2, heap_max - 1_000_000],
        external_range: [1_000_000, 3_000_000],
        array_buffers_range: [10_000, 50_000],
    }
}

static MEMORY_PRESETS: &[i64] = &[
    0, // process.constrainedMemory() 在非容器化环境返回 0
];

/// 生成随机的 64 字符十六进制字符串。
pub fn generate_device_id() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill(&mut bytes);
    hex::encode(bytes)
}

/// 为新账号生成全部规范化身份字段。
pub fn generate_canonical_identity() -> (String, Value, Value, Value) {
    let device_id = generate_device_id();
    let mut rng = rand::thread_rng();

    let presets = profile_presets();
    let preset = &presets[rng.gen_range(0..presets.len())];
    let mut process = preset.process.clone();
    process.constrained_memory = MEMORY_PRESETS[rng.gen_range(0..MEMORY_PRESETS.len())];
    let env_json = serde_json::to_value(&preset.env).expect("env preset serialize");
    let prompt_json = serde_json::to_value(&preset.prompt).expect("prompt preset serialize");
    let process_json = serde_json::to_value(&process).expect("process serialize");

    (device_id, env_json, prompt_json, process_json)
}

/// 构造 proto schema 完整的 env JSON（含所有 ~30 个字段）。
/// 供 rewriter 和 telemetry 共用，避免重复定义。
pub fn build_full_env_json(env: &CanonicalEnvData) -> Value {
    serde_json::json!({
        "platform": env.platform,
        "platform_raw": env.platform_raw,
        "arch": env.arch,
        "node_version": env.node_version,
        "terminal": env.terminal,
        "package_managers": env.package_managers,
        "runtimes": env.runtimes,
        "is_running_with_bun": env.is_running_with_bun,
        "is_ci": false,
        "is_claubbit": false,
        "is_claude_code_remote": false,
        "is_local_agent_mode": false,
        "is_conductor": false,
        "is_github_action": false,
        "is_claude_code_action": false,
        "is_claude_ai_auth": env.is_claude_ai_auth,
        "version": env.version,
        "version_base": env.version_base,
        "build_time": env.build_time,
        "deployment_environment": env.deployment_environment,
        "vcs": env.vcs,
        "github_event_name": "",
        "github_actions_runner_environment": "",
        "github_actions_runner_os": "",
        "github_action_ref": "",
        "wsl_version": "",
        "remote_environment_type": "",
        "claude_code_container_id": "",
        "claude_code_remote_session_id": "",
        "tags": [],
        "coworker_type": "",
        "linux_distro_id": env.linux_distro_id,
        "linux_distro_version": env.linux_distro_version,
        "linux_kernel": env.linux_kernel,
    })
}

/// 账号级设备画像，集中承载稳定身份字段。
#[derive(Debug, Clone)]
pub struct DeviceProfile {
    pub device_id: String,
    pub email: String,
    pub account_uuid: String,
    pub organization_uuid: Option<String>,
    pub subscription_type: Option<String>,
    pub env: CanonicalEnvData,
    pub prompt: CanonicalPromptEnvData,
    pub process: CanonicalProcessData,
}

/// 单次运行画像，承载一次会话内稳定的派生字段。
#[derive(Debug, Clone)]
pub struct RunProfile {
    pub started_at: DateTime<Utc>,
    pub session_id: String,
    pub growthbook_session_id: String,
}

/// 单次请求画像，承载请求级别的唯一标识。
#[derive(Debug, Clone)]
pub struct RequestProfile {
    pub session_id: String,
    pub client_request_id: String,
}

/// 进程指标快照。
#[derive(Debug, Clone)]
pub struct ProcessSnapshot {
    pub uptime: f64,
    pub rss: i64,
    pub heap_total: i64,
    pub heap_used: i64,
    pub external: i64,
    pub array_buffers: i64,
    pub constrained_memory: i64,
    pub cpu_user: i64,
    pub cpu_system: i64,
    pub cpu_percent: f64,
}

/// 从账号构建设备画像，缺失字段按旧账号兼容策略补齐。
pub fn device_profile(account: &Account) -> DeviceProfile {
    let env =
        normalize_env(serde_json::from_value(account.canonical_env.clone()).unwrap_or_default());
    let prompt = normalize_prompt(
        serde_json::from_value(account.canonical_prompt.clone()).unwrap_or_default(),
        &env,
    );
    let process = normalize_process(
        serde_json::from_value(account.canonical_process.clone()).unwrap_or_default(),
    );
    DeviceProfile {
        device_id: derive_device_id(account),
        email: account.email.clone(),
        account_uuid: derive_account_uuid(account),
        organization_uuid: account.organization_uuid.clone(),
        subscription_type: account.subscription_type.clone(),
        env,
        prompt,
        process,
    }
}

/// 为一次运行创建稳定运行画像。
pub fn run_profile(account: &Account, started_at: DateTime<Utc>) -> RunProfile {
    let seed = account_seed(account);
    RunProfile {
        started_at,
        session_id: deterministic_uuid(&format!("{}:run:{}", seed, started_at.timestamp())),
        growthbook_session_id: deterministic_uuid(&format!(
            "{}:growthbook:{}",
            seed,
            started_at.timestamp()
        )),
    }
}

/// 为单次请求创建请求画像。
pub fn request_profile(account: &Account, session_id: Option<String>) -> RequestProfile {
    let session_id = session_id.unwrap_or_else(|| random_uuid());
    let client_request_id = deterministic_uuid(&format!(
        "{}:request:{}",
        account_seed(account),
        random_uuid()
    ));
    RequestProfile {
        session_id,
        client_request_id,
    }
}

/// 派生账号 UUID，优先使用 OAuth 账号字段，旧账号缺失时用稳定 hash 补齐。
pub fn derive_account_uuid(account: &Account) -> String {
    account.account_uuid.clone().unwrap_or_else(|| {
        let seed = if account.email.is_empty() {
            format!("{}-{}", DEFAULT_ACCOUNT_ID_SEED, account.id)
        } else {
            account.email.clone()
        };
        deterministic_uuid(&seed)
    })
}

/// 派生设备 ID，旧账号缺失时用稳定 hash 补齐为 64 位十六进制。
pub fn derive_device_id(account: &Account) -> String {
    if !account.device_id.is_empty() {
        return account.device_id.clone();
    }
    hex::encode(stable_hash_bytes(&account_seed(account)))
}

/// 生成 RFC 4122 v4 形态的随机 UUID。
pub fn random_uuid() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill(&mut b);
    uuid_from_bytes(b)
}

/// 根据进程配置和 uptime 生成平滑进程快照。
pub fn process_snapshot(
    proc: &CanonicalProcessData,
    account_seed: &str,
    uptime_secs: f64,
) -> ProcessSnapshot {
    let tick = uptime_secs.max(0.0).round() as i64;
    let rss = smooth_value(&proc.rss_range, account_seed, "rss", tick, 7);
    let heap_total = smooth_value(&proc.heap_total_range, account_seed, "heap_total", tick, 11);
    let heap_used = smooth_value(&proc.heap_used_range, account_seed, "heap_used", tick, 5)
        .min(heap_total.saturating_sub(1))
        .max(1);
    let external = smooth_value(&proc.external_range, account_seed, "external", tick, 13);
    let array_buffers = smooth_value(
        &proc.array_buffers_range,
        account_seed,
        "array_buffers",
        tick,
        17,
    );
    let cpu_user = 50_000
        + ((stable_hash(&format!("{}:cpu_user", account_seed)) % 450_000) as i64)
        + tick.saturating_mul(127);
    let cpu_system = 15_000
        + ((stable_hash(&format!("{}:cpu_system", account_seed)) % 135_000) as i64)
        + tick.saturating_mul(43);
    let cpu_percent = 0.5
        + ((stable_hash(&format!("{}:cpu_percent:{}", account_seed, tick / 10)) % 350) as f64)
            / 100.0;
    ProcessSnapshot {
        uptime: uptime_secs,
        rss,
        heap_total,
        heap_used,
        external,
        array_buffers,
        constrained_memory: proc.constrained_memory,
        cpu_user,
        cpu_system,
        cpu_percent,
    }
}

/// 将进程快照转换为 telemetry 使用的 JSON。
pub fn process_snapshot_json(snapshot: &ProcessSnapshot) -> Value {
    serde_json::json!({
        "uptime": snapshot.uptime,
        "rss": snapshot.rss,
        "heapTotal": snapshot.heap_total,
        "heapUsed": snapshot.heap_used,
        "external": snapshot.external,
        "arrayBuffers": snapshot.array_buffers,
        "constrainedMemory": snapshot.constrained_memory,
        "cpuUsage": {
            "user": snapshot.cpu_user,
            "system": snapshot.cpu_system,
        },
        "cpuPercent": snapshot.cpu_percent,
    })
}

/// 旧账号兼容补齐环境字段，不覆盖已有非空值。
pub fn normalize_env(mut env: CanonicalEnvData) -> CanonicalEnvData {
    env.is_running_with_bun = true;
    if env.platform.is_empty() {
        env.platform = "linux".into();
    }
    if env.platform_raw.is_empty() {
        env.platform_raw = env.platform.clone();
    }
    if env.arch.is_empty() {
        env.arch = if env.platform == "darwin" {
            "arm64".into()
        } else {
            "x64".into()
        };
    }
    if env.node_version.is_empty() {
        env.node_version = "v24.3.0".into();
    }
    if env.terminal.is_empty() {
        env.terminal = match env.platform.as_str() {
            "darwin" => "iTerm.app",
            "win32" => "windows-terminal",
            _ => "ssh-session",
        }
        .into();
    }
    if env.package_managers.is_empty() {
        env.package_managers = "npm".into();
    }
    if env.runtimes.is_empty() {
        env.runtimes = "node".into();
    }
    if env.version.is_empty() {
        env.version = DEFAULT_CLAUDE_CODE_VERSION.into();
    }
    if env.version_base.is_empty() {
        env.version_base = DEFAULT_CLAUDE_CODE_VERSION_BASE.into();
    }
    if env.build_time.is_empty() {
        env.build_time = DEFAULT_CLAUDE_CODE_BUILD_TIME.into();
    }
    if env.deployment_environment.is_empty() {
        env.deployment_environment = format!("unknown-{}", env.platform);
    }
    if env.vcs.is_empty() {
        env.vcs = "git".into();
    }
    if env.platform == "linux" {
        if env.linux_distro_id.is_empty() {
            env.linux_distro_id = "ubuntu".into();
        }
        if env.linux_distro_version.is_empty() {
            env.linux_distro_version = "24.04".into();
        }
        if env.linux_kernel.is_empty() {
            env.linux_kernel = "6.8.0-60-generic".into();
        }
    }
    env
}

/// 旧账号兼容补齐 system prompt 环境字段。
pub fn normalize_prompt(
    mut prompt: CanonicalPromptEnvData,
    env: &CanonicalEnvData,
) -> CanonicalPromptEnvData {
    if prompt.platform.is_empty() {
        prompt.platform = env.platform.clone();
    }
    if prompt.shell.is_empty() {
        prompt.shell = match env.platform.as_str() {
            "darwin" => "zsh".into(),
            "win32" => "bash (use Unix shell syntax, not Windows - e.g., /dev/null not NUL, forward slashes in paths)".into(),
            _ => "bash".into(),
        };
    }
    if prompt.os_version.is_empty() {
        prompt.os_version = match env.platform.as_str() {
            "darwin" => "Darwin 24.4.0".into(),
            "win32" => "Windows 11 Pro 10.0.22631".into(),
            _ => format!("Linux {}", env.linux_kernel),
        };
    }
    if prompt.working_dir.is_empty() {
        prompt.working_dir = match env.platform.as_str() {
            "darwin" => "/Users/user/projects",
            "win32" => "/c/Users/user/projects",
            _ => "/home/user/projects",
        }
        .into();
    }
    prompt
}

/// 旧账号兼容补齐进程范围字段。
pub fn normalize_process(mut process: CanonicalProcessData) -> CanonicalProcessData {
    if invalid_range(process.rss_range) {
        process.rss_range = [300_000_000, 520_000_000];
    }
    if invalid_range(process.heap_total_range) {
        process.heap_total_range = [48_000_000, 96_000_000];
    }
    if invalid_range(process.heap_used_range) {
        process.heap_used_range = [24_000_000, 80_000_000];
    }
    if process.heap_used_range[1] >= process.heap_total_range[1] {
        // heapUsed 必须低于 heapTotal，否则 telemetry 会暴露出明显不可能的运行画像。
        process.heap_used_range = [
            (process.heap_total_range[0] / 2).max(1),
            process.heap_total_range[1].saturating_sub(1).max(2),
        ];
    }
    if invalid_range(process.external_range) {
        process.external_range = [1_000_000, 3_000_000];
    }
    if invalid_range(process.array_buffers_range) {
        process.array_buffers_range = [10_000, 50_000];
    }
    process
}

fn invalid_range(range: [i64; 2]) -> bool {
    range[0] <= 0 || range[1] <= range[0]
}

fn account_seed(account: &Account) -> String {
    if !account.device_id.is_empty() {
        account.device_id.clone()
    } else if !account.email.is_empty() {
        account.email.clone()
    } else {
        format!("{}-{}", DEFAULT_ACCOUNT_ID_SEED, account.id)
    }
}

fn deterministic_uuid(seed: &str) -> String {
    let hash = stable_hash_bytes(&format!("{}:{}", UUID_SEED_NAMESPACE, seed));
    let mut b = [0u8; 16];
    b.copy_from_slice(&hash[..16]);
    uuid_from_bytes(b)
}

fn uuid_from_bytes(mut b: [u8; 16]) -> String {
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{}-{}-{}-{}-{}",
        hex::encode(&b[0..4]),
        hex::encode(&b[4..6]),
        hex::encode(&b[6..8]),
        hex::encode(&b[8..10]),
        hex::encode(&b[10..16])
    )
}

fn smooth_value(range: &[i64; 2], seed: &str, label: &str, tick: i64, period: i64) -> i64 {
    if range[1] <= range[0] {
        return range[0].max(0);
    }
    let bucket = if period <= 0 { tick } else { tick / period };
    let width = (range[1] - range[0]) as u64;
    let hash = stable_hash(&format!("{}:{}:{}", seed, label, bucket));
    range[0] + (hash % width) as i64
}

fn stable_hash(seed: &str) -> u64 {
    let bytes = stable_hash_bytes(seed);
    u64::from_be_bytes(bytes[..8].try_into().expect("hash prefix"))
}

fn stable_hash_bytes(seed: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(seed.as_bytes()).to_vec()
}

#[cfg(test)]
mod tests {
    use super::{
        derive_account_uuid, derive_device_id, generate_canonical_identity, normalize_env,
        normalize_process, process_snapshot, profile_presets, run_profile,
    };
    use crate::model::account::{
        Account, AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData,
        CanonicalProcessData, CanonicalPromptEnvData,
    };
    use crate::service::version_profile::{
        DEFAULT_CLAUDE_CODE_BUILD_TIME, DEFAULT_CLAUDE_CODE_VERSION,
        DEFAULT_CLAUDE_CODE_VERSION_BASE,
    };
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    fn legacy_account() -> Account {
        Account {
            id: 42,
            name: "测试账号".into(),
            email: "user@example.com".into(),
            status: AccountStatus::Active,
            auth_type: AccountAuthType::Oauth,
            setup_token: String::new(),
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_at: None,
            oauth_refreshed_at: None,
            auth_error: String::new(),
            proxy_url: String::new(),
            device_id: String::new(),
            canonical_env: json!({}),
            canonical_prompt: serde_json::to_value(CanonicalPromptEnvData::default()).unwrap(),
            canonical_process: serde_json::to_value(CanonicalProcessData::default()).unwrap(),
            billing_mode: BillingMode::Strip,
            account_uuid: None,
            organization_uuid: None,
            subscription_type: None,
            concurrency: 3,
            priority: 50,
            rpm_limit: 0,
            rate_limited_at: None,
            rate_limit_reset_at: None,
            disable_reason: String::new(),
            auto_telemetry: false,
            auto_poll_usage: false,
            allow_1m_models: "opus".into(),
            telemetry_count: 0,
            usage_data: json!({}),
            usage_fetched_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn profile_presets_are_platform_consistent() {
        let presets = profile_presets();
        assert!(presets.iter().any(|p| p.env.platform == "linux"));
        assert!(presets.iter().any(|p| p.env.platform == "darwin"));
        assert!(presets.iter().any(|p| p.env.platform == "win32"));

        for preset in presets {
            assert_eq!(preset.env.platform, preset.env.platform_raw);
            assert_eq!(preset.prompt.platform, preset.env.platform);
            assert_eq!(preset.env.version, DEFAULT_CLAUDE_CODE_VERSION);
            assert_eq!(preset.env.version_base, DEFAULT_CLAUDE_CODE_VERSION_BASE);
            assert_eq!(preset.env.build_time, DEFAULT_CLAUDE_CODE_BUILD_TIME);
            assert!(preset.env.is_running_with_bun);
            assert!(preset.process.heap_used_range[1] < preset.process.heap_total_range[1]);

            match preset.env.platform.as_str() {
                "linux" => {
                    assert!(preset.prompt.working_dir.starts_with("/home/"));
                    assert!(preset.prompt.os_version.starts_with("Linux "));
                    assert!(!preset.env.linux_kernel.is_empty());
                }
                "darwin" => {
                    assert!(preset.prompt.working_dir.starts_with("/Users/"));
                    assert!(preset.prompt.os_version.starts_with("Darwin "));
                    assert_eq!(preset.prompt.shell, "zsh");
                }
                "win32" => {
                    assert!(preset.prompt.working_dir.starts_with("/c/Users/"));
                    assert!(preset.prompt.os_version.starts_with("Windows "));
                    assert!(preset.prompt.shell.contains("Unix shell syntax"));
                }
                other => panic!("未知平台预设: {}", other),
            }
        }
    }

    #[test]
    fn generated_identity_uses_consistent_profile() {
        for _ in 0..20 {
            let (device_id, env_json, prompt_json, process_json) = generate_canonical_identity();
            let env: CanonicalEnvData = serde_json::from_value(env_json).unwrap();
            let prompt: CanonicalPromptEnvData = serde_json::from_value(prompt_json).unwrap();
            let process: CanonicalProcessData = serde_json::from_value(process_json).unwrap();

            assert_eq!(device_id.len(), 64);
            assert_eq!(prompt.platform, env.platform);
            assert!(env.is_running_with_bun);
            assert!(process.heap_used_range[1] < process.heap_total_range[1]);
        }
    }

    #[test]
    fn legacy_env_and_process_are_normalized_without_storage_writeback() {
        let env = normalize_env(CanonicalEnvData::default());
        assert_eq!(env.platform, "linux");
        assert_eq!(env.node_version, "v24.3.0");
        assert_eq!(env.version, DEFAULT_CLAUDE_CODE_VERSION);
        assert!(env.is_running_with_bun);
        assert_eq!(env.linux_distro_id, "ubuntu");
        assert_eq!(env.linux_kernel, "6.8.0-60-generic");

        let process = normalize_process(CanonicalProcessData {
            heap_total_range: [48_000_000, 96_000_000],
            heap_used_range: [120_000_000, 220_000_000],
            ..Default::default()
        });
        assert!(process.heap_used_range[1] < process.heap_total_range[1]);
        assert!(process.rss_range[1] > process.rss_range[0]);
    }

    #[test]
    fn normalize_env_aligns_legacy_false_bun_to_current_profile() {
        let env = normalize_env(CanonicalEnvData {
            is_running_with_bun: false,
            ..Default::default()
        });

        assert!(env.is_running_with_bun);
    }

    #[test]
    fn account_and_device_fallback_ids_are_stable() {
        let account = legacy_account();
        assert_eq!(derive_account_uuid(&account), derive_account_uuid(&account));
        assert_eq!(derive_device_id(&account), derive_device_id(&account));
        assert_eq!(derive_device_id(&account).len(), 64);
    }

    #[test]
    fn run_profile_and_process_snapshot_are_stable_and_bounded() {
        let account = legacy_account();
        let started_at = Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap();
        let run_a = run_profile(&account, started_at);
        let run_b = run_profile(&account, started_at);
        assert_eq!(run_a.session_id, run_b.session_id);
        assert_eq!(run_a.growthbook_session_id, run_b.growthbook_session_id);

        let process = normalize_process(CanonicalProcessData::default());
        let first = process_snapshot(&process, "device-1", 12.0);
        let second = process_snapshot(&process, "device-1", 12.0);
        let later = process_snapshot(&process, "device-1", 24.0);
        assert_eq!(first.rss, second.rss);
        assert!(first.heap_used <= first.heap_total);
        assert!(later.cpu_user > first.cpu_user);
        assert!(later.cpu_system > first.cpu_system);
    }
}
