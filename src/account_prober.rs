use crate::client::UpstreamClient;
use crate::db::Database;
use crate::types::Account;
use crate::{oauth, quota};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub id: i64,
    pub email: String,
    pub is_banned: bool,
    pub ban_reason: Option<String>,
    pub status: String, // "healthy", "banned", "unbanned", "error"
    pub message: String,
}

pub struct AccountProber {
    db: Database,
    client: UpstreamClient,
    client_id: String,
    client_secret: String,
    interval_sec: u64,
}

impl AccountProber {
    pub fn new(
        db: Database,
        client: UpstreamClient,
        client_id: String,
        client_secret: String,
        interval_sec: u64,
    ) -> Self {
        Self {
            db,
            client,
            client_id,
            client_secret,
            interval_sec,
        }
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            log::info!(
                "[ACCOUNT:PROBER] Background periodic probe worker active (interval: {}s)",
                self.interval_sec
            );
            // 5с задержка при старте шлюза перед первым опросом
            sleep(Duration::from_secs(5)).await;
            loop {
                let _ = self.probe_all_accounts().await;
                sleep(Duration::from_secs(self.interval_sec)).await;
            }
        });
    }

    pub async fn probe_all_accounts(&self) -> Vec<ProbeResult> {
        let accounts = match self.db.load_all_accounts().await {
            Ok(accs) => accs,
            Err(e) => {
                log::error!("[ACCOUNT:PROBER] Failed to load accounts for probing: {}", e);
                return Vec::new();
            }
        };

        let mut results = Vec::with_capacity(accounts.len());
        for acc in accounts {
            let res = self.probe_single_account(&acc).await;
            results.push(res);
            sleep(Duration::from_millis(250)).await;
        }
        results
    }

    pub async fn probe_single_account(&self, acc: &Account) -> ProbeResult {
        let email = acc.email.clone().unwrap_or_else(|| "anon".to_string());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        // 1. Проверка/обновление OAuth токена (direct с откатом на proxy)
        let token_res = if acc.refresh_token.starts_with("ya29.") {
            Ok((acc.refresh_token.clone(), now + 3500.0))
        } else {
            match oauth::refresh_access_token(
                self.client.direct(),
                &acc.refresh_token,
                &self.client_id,
                &self.client_secret,
            )
            .await
            {
                Ok(t) => Ok(t),
                Err(e) => {
                    let err_s = e.to_string().to_lowercase();
                    if err_s.contains("invalid_grant")
                        || err_s.contains("disabled")
                        || err_s.contains("deleted")
                        || err_s.contains("revoked")
                    {
                        Err(e)
                    } else {
                        oauth::refresh_access_token(
                            self.client.inner(),
                            &acc.refresh_token,
                            &self.client_id,
                            &self.client_secret,
                        )
                        .await
                    }
                }
            }
        };

        let (access_tok, _exp) = match token_res {
            Ok((t, exp)) => {
                let _ = self.db.update_tokens(acc.id, &t, exp).await;
                (t, exp)
            }
            Err(e) => {
                let err_str = e.to_string();
                let err_lower = err_str.to_lowercase();
                let is_ban = err_lower.contains("invalid_grant")
                    || err_lower.contains("disabled")
                    || err_lower.contains("deleted")
                    || err_lower.contains("revoked")
                    || err_lower.contains("access_denied")
                    || err_lower.contains("unauthorized_client")
                    || err_lower.contains("400")
                    || err_lower.contains("403");

                if is_ban {
                    let reason = format!("Google OAuth Ban: {}", err_str);
                    let _ = self.db.mark_account_banned(acc.id, &reason).await;
                    log::warn!("[ACCOUNT:PROBER] Acc #{} ({}) is BANNED: {}", acc.id, email, reason);
                    return ProbeResult {
                        id: acc.id,
                        email,
                        is_banned: true,
                        ban_reason: Some(reason.clone()),
                        status: "banned".to_string(),
                        message: reason,
                    };
                } else {
                    return ProbeResult {
                        id: acc.id,
                        email,
                        is_banned: acc.is_banned,
                        ban_reason: acc.ban_reason.clone(),
                        status: "error".to_string(),
                        message: format!("Сетевая ошибка обновления: {}", err_str),
                    };
                }
            }
        };

        // 2. Тестирование рабочего состояния через Quota API Cloud Code PA
        let quota_res = match quota::fetch_quota_summary(self.client.direct(), &access_tok).await {
            Ok(qs) => Ok(qs),
            Err(e) => {
                let err_s = e.to_string();
                if err_s.contains("403") || err_s.contains("PERMISSION_DENIED") || err_s.contains("suspended") {
                    Err(e)
                } else {
                    quota::fetch_quota_summary(self.client.inner(), &access_tok).await
                }
            }
        };

        let qs = match quota_res {
            Ok(qs) => {
                let _ = self
                    .db
                    .update_quota(acc.id, &qs.raw_json, qs.gemini_fraction, qs.claude_fraction, now)
                    .await;
                qs
            }
            Err(e) => {
                let err_str = e.to_string();
                let is_pa_ban = err_str.contains("403")
                    || err_str.contains("PERMISSION_DENIED")
                    || err_str.contains("suspended")
                    || err_str.contains("disabled");

                if is_pa_ban {
                    let reason = format!("Cloud Code PA Ban: {}", err_str);
                    let _ = self.db.mark_account_banned(acc.id, &reason).await;
                    log::warn!("[ACCOUNT:PROBER] Acc #{} ({}) BANNED on Cloud Code PA: {}", acc.id, email, reason);
                    return ProbeResult {
                        id: acc.id,
                        email,
                        is_banned: true,
                        ban_reason: Some(reason.clone()),
                        status: "banned".to_string(),
                        message: reason,
                    };
                } else {
                    return ProbeResult {
                        id: acc.id,
                        email,
                        is_banned: acc.is_banned,
                        ban_reason: acc.ban_reason.clone(),
                        status: "error".to_string(),
                        message: format!("Ошибка проверки квоты: {}", err_str),
                    };
                }
            }
        };

        // 3. Реальный тестовый инференс-зонд к upstream generateContent
        // (retrieveUserQuotaSummary отдает 200 даже на забаненных по TOS аккаунтах!)
        let probe_payload = serde_json::json!({
            "model": "gemini-2.5-flash",
            "project": "",
            "request": {
                "contents": [{
                    "role": "user",
                    "parts": [{ "text": "ping" }]
                }],
                "generationConfig": {
                    "maxOutputTokens": 1
                }
            }
        });

        let mut probe_resp = self
            .client
            .direct()
            .post("https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent")
            .header("Authorization", format!("Bearer {}", access_tok))
            .header("Content-Type", "application/json")
            .header("User-Agent", "antigravity/hub/2.23.1 darwin/arm64")
            .header("x-client-name", "antigravity")
            .json(&probe_payload)
            .send()
            .await;

        if probe_resp.is_err() {
            probe_resp = self
                .client
                .inner()
                .post("https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent")
                .header("Authorization", format!("Bearer {}", access_tok))
                .header("Content-Type", "application/json")
                .header("User-Agent", "antigravity/hub/2.23.1 darwin/arm64")
                .header("x-client-name", "antigravity")
                .json(&probe_payload)
                .send()
                .await;
        }

        match probe_resp {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    if acc.is_banned || acc.ban_reason.is_some() {
                        let _ = self.db.mark_account_unbanned(acc.id).await;
                        log::info!(
                            "[ACCOUNT:PROBER] Acc #{} ({}) UNBANNED! Appeal approved, reactivating.",
                            acc.id, email
                        );
                        ProbeResult {
                            id: acc.id,
                            email,
                            is_banned: false,
                            ban_reason: None,
                            status: "unbanned".to_string(),
                            message: format!("Разбанен! Инференс и квоты активны (Gemini: {:.0}%, Claude: {:.0}%)", qs.gemini_fraction * 100.0, qs.claude_fraction * 100.0),
                        }
                    } else {
                        ProbeResult {
                            id: acc.id,
                            email,
                            is_banned: false,
                            ban_reason: None,
                            status: "healthy".to_string(),
                            message: format!("Активен (Gemini: {:.0}%, Claude: {:.0}%)", qs.gemini_fraction * 100.0, qs.claude_fraction * 100.0),
                        }
                    }
                } else {
                    let body = resp.text().await.unwrap_or_default();
                    let body_lower = body.to_lowercase();
                    let is_tos_ban = status.as_u16() == 403
                        || body_lower.contains("terms of service")
                        || body_lower.contains("disabled in this account")
                        || body_lower.contains("permission_denied")
                        || body_lower.contains("suspended");

                    if is_tos_ban {
                        let reason = if body_lower.contains("terms of service") || body_lower.contains("disabled in this account") {
                            "Google TOS Ban: аккаунт заблокирован за нарушение условий использования".to_string()
                        } else {
                            format!("Cloud Code PA Ban (HTTP {}): {}", status.as_u16(), body)
                        };

                        let _ = self.db.mark_account_banned(acc.id, &reason).await;
                        log::warn!("[ACCOUNT:PROBER] Acc #{} ({}) TOS BANNED on generateContent: {}", acc.id, email, reason);
                        ProbeResult {
                            id: acc.id,
                            email,
                            is_banned: true,
                            ban_reason: Some(reason.clone()),
                            status: "banned".to_string(),
                            message: reason,
                        }
                    } else {
                        ProbeResult {
                            id: acc.id,
                            email,
                            is_banned: acc.is_banned,
                            ban_reason: acc.ban_reason.clone(),
                            status: "error".to_string(),
                            message: format!("Временная ошибка upstream (HTTP {}): {}", status.as_u16(), body),
                        }
                    }
                }
            }
            Err(e) => {
                let err_str = e.to_string();
                ProbeResult {
                    id: acc.id,
                    email,
                    is_banned: acc.is_banned,
                    ban_reason: acc.ban_reason.clone(),
                    status: "error".to_string(),
                    message: format!("Сетевой сбой зонда generateContent: {}", err_str),
                }
            }
        }
    }
}
