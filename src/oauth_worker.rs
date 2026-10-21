use crate::db::Database;
use crate::oauth;
use reqwest::Client;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

/// Фоновый демон превентивного обновления токенов Google OAuth.
/// Обновляет токены за 5 минут до их истечения, исключая лаги при запросах пользователей.
pub struct OAuthWorker {
    db: Database,
    client: Client,
    client_id: String,
    client_secret: String,
}

impl OAuthWorker {
    pub fn new(db: Database, client: Client, client_id: String, client_secret: String) -> Self {
        Self {
            db,
            client,
            client_id,
            client_secret,
        }
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            log::info!("[OAUTH:WORKER] Background token refresh worker started");
            loop {
                if let Err(e) = self.refresh_expiring_tokens().await {
                    log::warn!("[OAUTH:WORKER] Periodic check failed: {}", e);
                }
                // Проверка каждые 60 секунд
                sleep(Duration::from_secs(60)).await;
            }
        });
    }

    pub async fn refresh_expiring_tokens(&self) -> anyhow::Result<()> {
        let accounts = self.db.load_accounts().await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
        let refresh_threshold = now + 300.0; // 5 минут до истечения

        for acc in accounts {
            if !acc.enabled {
                continue;
            }

            let needs_refresh = match &acc.access_token {
                Some(token) if !token.is_empty() => acc.expires_at < refresh_threshold,
                _ => true,
            };

            if needs_refresh {
                let email = acc.email.as_deref().unwrap_or("anon");
                log::info!(
                    "[OAUTH:WORKER] Refreshing expiring token for Acc #{} ({})",
                    acc.id,
                    email
                );

                match oauth::refresh_access_token(
                    &self.client,
                    &acc.refresh_token,
                    &self.client_id,
                    &self.client_secret,
                )
                .await
                {
                    Ok((new_token, exp)) => {
                        self.db.update_tokens(acc.id, &new_token, exp).await?;
                        log::info!(
                            "[OAUTH:WORKER] Token refreshed successfully for Acc #{} (TTL: {:.0}s)",
                            acc.id,
                            exp - now
                        );
                    }
                    Err(e) => {
                        log::error!("[OAUTH:WORKER] Failed to refresh Acc #{}: {}", acc.id, e);
                    }
                }
            }
        }
        Ok(())
    }
}
