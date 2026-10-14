use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::model::account::{AccountAuthType, AccountStatus};
use crate::service::account::AccountService;

/// 账户间隔,避免瞬间打爆 Anthropic usage 端点。
const PER_ACCOUNT_GAP: Duration = Duration::from_millis(500);

/// 后台定时拉取所有 OAuth 账户用量数据的服务。
pub struct UsagePollerService {
    account_svc: Arc<AccountService>,
    interval: Duration,
    enabled: bool,
}

impl UsagePollerService {
    /// 构造可由部署模式关闭的后台用量服务。
    /// @param account_svc 既有账号服务。
    /// @param interval 旧实例轮询间隔。
    /// @param enabled 是否允许后台主动查询；v3 必须为 false。
    /// @return 后台服务实例，不修改账号的历史轮询开关。
    pub fn new(account_svc: Arc<AccountService>, interval: Duration, enabled: bool) -> Self {
        Self {
            account_svc,
            interval,
            enabled,
        }
    }

    /// 启动后台循环；被动部署直接返回。
    /// @return 无返回值，启用时持续轮询。
    pub async fn run(self: Arc<Self>) {
        if !self.enabled {
            return;
        }
        info!("usage poller: started, interval = {:?}", self.interval);
        // 启动后先等一个 interval,避免和应用冷启动抢资源
        sleep(self.interval).await;
        loop {
            self.tick().await;
            sleep(self.interval).await;
        }
    }

    async fn tick(&self) {
        let accounts = match self.account_svc.list_accounts().await {
            Ok(list) => list,
            Err(e) => {
                warn!("usage poller: list accounts failed: {}", e);
                return;
            }
        };

        let targets: Vec<i64> = accounts
            .iter()
            .filter(|a| {
                a.auth_type == AccountAuthType::Oauth
                    && a.status == AccountStatus::Active
                    && a.auto_poll_usage
            })
            .map(|a| a.id)
            .collect();

        debug!("usage poller: polling {} oauth accounts", targets.len());

        for id in targets {
            match self.account_svc.refresh_usage(id).await {
                Ok(_) => debug!("usage poller: refreshed account {}", id),
                Err(e) => warn!("usage poller: account {} failed: {}", id, e),
            }
            sleep(PER_ACCOUNT_GAP).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UsagePollerService;
    use crate::service::account::AccountService;
    use crate::store::account_store::AccountStore;
    use crate::store::db;
    use crate::store::memory::MemoryStore;
    use crate::store::settings_store::SettingsStore;
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    /// 同一真实轮询账号在 v3 禁用时零连接，在旧模式仍能请求所配置代理。
    /// @return 测试成功无返回值。
    #[tokio::test]
    async fn disabled_mode_does_not_poll_account_but_legacy_mode_does() {
        install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool");
        db::migrate(&pool, "sqlite").await.expect("schema");
        let proxy = TcpListener::bind("127.0.0.1:0").await.expect("proxy");
        let url = format!("http://{}", proxy.local_addr().expect("address"));
        sqlx::query("INSERT INTO accounts(name,email,token,device_id,auth_type,access_token,refresh_token,oauth_expires_at,proxy_url,auto_poll_usage) VALUES('fixture','fixture@example.invalid','synthetic','synthetic-device','oauth','synthetic','synthetic-refresh','2050-01-01T00:00:00Z',$1,1)")
            .bind(url)
            .execute(&pool)
            .await
            .expect("account");
        let account_svc = Arc::new(AccountService::new(
            Arc::new(AccountStore::new(pool.clone(), "sqlite".into())),
            Arc::new(MemoryStore::new()),
            Arc::new(SettingsStore::new(pool)),
        ));
        let account = account_svc
            .list_accounts()
            .await
            .expect("accounts")
            .remove(0);
        assert!(account.auto_poll_usage, "fixture must enable polling");
        assert!(
            account.expires_at.is_some(),
            "fixture token must have expiry"
        );
        let disabled = Arc::new(UsagePollerService::new(
            account_svc.clone(),
            Duration::from_millis(1),
            false,
        ));
        timeout(Duration::from_millis(100), disabled.run())
            .await
            .expect("disabled service must return");
        assert!(
            timeout(Duration::from_millis(25), proxy.accept())
                .await
                .is_err()
        );
        let enabled = Arc::new(UsagePollerService::new(
            account_svc.clone(),
            Duration::from_millis(1),
            true,
        ));
        let task = tokio::spawn(enabled.run());
        let connection = timeout(Duration::from_secs(3), proxy.accept()).await;
        task.abort();
        let _ = task.await;
        assert!(
            connection.is_ok(),
            "legacy polling must still use its proxy"
        );
    }
}
