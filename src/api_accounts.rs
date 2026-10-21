use crate::AppState;
use crate::{oauth, quota};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
pub struct AccountResponse {
    pub id: i64,
    pub email: Option<String>,
    pub gemini_remaining: f64,
    pub claude_remaining: f64,
    pub enabled: bool,
    pub last_quota_check: f64,
    pub gemini_5h: f64,
    pub gemini_weekly: f64,
    pub claude_5h: f64,
    pub claude_weekly: f64,
    pub gemini_5h_reset: Option<String>,
    pub gemini_weekly_reset: Option<String>,
    pub claude_5h_reset: Option<String>,
    pub claude_weekly_reset: Option<String>,
}

pub(crate) fn parse_account_windows(
    raw_json: Option<&str>,
    gemini_rem: f64,
    claude_rem: f64,
) -> (
    f64,
    f64,
    f64,
    f64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let mut g_5h = gemini_rem;
    let mut g_wk = gemini_rem;
    let mut c_5h = claude_rem;
    let mut c_wk = claude_rem;
    let mut g_5h_reset = None;
    let mut g_wk_reset = None;
    let mut c_5h_reset = None;
    let mut c_wk_reset = None;

    if let Some(json_str) = raw_json {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
            if let Some(groups) = v.get("groups").and_then(|g| g.as_array()) {
                for grp in groups {
                    let desc = grp
                        .get("displayName")
                        .or_else(|| grp.get("description"))
                        .and_then(|d| d.as_str())
                        .unwrap_or("")
                        .to_lowercase();
                    let is_gemini = desc.contains("gemini");
                    let is_claude = desc.contains("claude") || desc.contains("3p");

                    if let Some(buckets) = grp.get("buckets").and_then(|b| b.as_array()) {
                        for b in buckets {
                            let window = b.get("window").and_then(|w| w.as_str()).unwrap_or("");
                            let b_id = b
                                .get("bucketId")
                                .and_then(|id| id.as_str())
                                .unwrap_or("")
                                .to_lowercase();
                            let rem = b
                                .get("remainingFraction")
                                .and_then(|r| r.as_f64())
                                .unwrap_or(1.0);
                            let reset = b
                                .get("resetTime")
                                .and_then(|rt| rt.as_str())
                                .map(|s| s.to_string());

                            if is_gemini || b_id.contains("gemini") {
                                if window == "5h" || b_id.contains("5h") {
                                    g_5h = rem;
                                    g_5h_reset = reset;
                                } else if window == "weekly" || b_id.contains("weekly") {
                                    g_wk = rem;
                                    g_wk_reset = reset;
                                }
                            } else if is_claude || b_id.contains("3p") || b_id.contains("claude") {
                                if window == "5h" || b_id.contains("5h") {
                                    c_5h = rem;
                                    c_5h_reset = reset;
                                } else if window == "weekly" || b_id.contains("weekly") {
                                    c_wk = rem;
                                    c_wk_reset = reset;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    (
        g_5h, g_wk, c_5h, c_wk, g_5h_reset, g_wk_reset, c_5h_reset, c_wk_reset,
    )
}

#[derive(Deserialize)]
pub struct AddAccountRequest {
    pub refresh_token: String,
    pub email: Option<String>,
}

pub(crate) async fn handle_get_accounts(State(state): State<AppState>) -> impl IntoResponse {
    match state.db.load_all_accounts().await {
        Ok(accounts) => {
            let res: Vec<AccountResponse> = accounts
                .into_iter()
                .map(|a| {
                    let (g_5h, g_wk, c_5h, c_wk, g_5h_rst, g_wk_rst, c_5h_rst, c_wk_rst) =
                        parse_account_windows(
                            a.quota_json.as_deref(),
                            a.gemini_remaining,
                            a.claude_remaining,
                        );
                    AccountResponse {
                        id: a.id,
                        email: a.email,
                        gemini_remaining: a.gemini_remaining,
                        claude_remaining: a.claude_remaining,
                        enabled: a.enabled,
                        last_quota_check: a.last_quota_check,
                        gemini_5h: g_5h,
                        gemini_weekly: g_wk,
                        claude_5h: c_5h,
                        claude_weekly: c_wk,
                        gemini_5h_reset: g_5h_rst,
                        gemini_weekly_reset: g_wk_rst,
                        claude_5h_reset: c_5h_rst,
                        claude_weekly_reset: c_wk_rst,
                    }
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({ "accounts": res })))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

pub(crate) async fn handle_add_account(
    State(state): State<AppState>,
    Json(payload): Json<AddAccountRequest>,
) -> impl IntoResponse {
    let tok = payload.refresh_token.trim();
    if tok.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "refresh_token is empty" })),
        );
    }

    let (client_id, client_secret) = {
        let cfg = state.config.read();
        (cfg.oauth.client_id.clone(), cfg.oauth.client_secret.clone())
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    // 1. Validate & Exchange token with Google (Direct first for instant ~150ms response, proxy fallback)
    let (access_tok, expires_at) = if tok.starts_with("ya29.") {
        (tok.to_string(), now + 3500.0)
    } else {
        match oauth::refresh_access_token(state.client.direct(), tok, &client_id, &client_secret)
            .await
        {
            Ok(t) => t,
            Err(_) => {
                // Повторная попытка через форвардер прокси при сбое прямого подключения
                match oauth::refresh_access_token(
                    state.client.inner(),
                    tok,
                    &client_id,
                    &client_secret,
                )
                .await
                {
                    Ok(t) => t,
                    Err(e_proxy) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({
                                "status": "error",
                                "valid": false,
                                "error": format!("Google OAuth validation failed: {}", e_proxy)
                            })),
                        );
                    }
                }
            }
        }
    };

    // Получение email учетной записи Google (сначала через прокси, затем напрямую)
    let user_email = match fetch_user_email(state.client.inner(), &access_tok).await {
        Some(em) => Some(em),
        None => match fetch_user_email(state.client.direct(), &access_tok).await {
            Some(em) => Some(em),
            None => payload.email.clone(),
        },
    };

    // Проверка онбординга аккаунта в Google Cloud Code (фоновый запуск для быстрого ответа)
    let client_bg = state.client.clone();
    let at_bg = access_tok.clone();
    tokio::spawn(async move {
        let _ = client_bg.ensure_code_assist_onboarding(&at_bg).await;
    });

    // 2. Получение актуальных квот
    let quota_summary = quota::fetch_quota_summary(state.client.inner(), &access_tok)
        .await
        .unwrap_or(quota::QuotaSummary {
            raw_json: "{}".to_string(),
            gemini_fraction: 1.0,
            claude_fraction: 1.0,
        });

    // 3. Сохранение в базу данных
    let id = match state.db.add_account(tok, user_email.as_deref()).await {
        Ok(id) => id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("DB error: {}", e), "valid": false })),
            );
        }
    };

    let _ = state.db.update_tokens(id, &access_tok, expires_at).await;
    let _ = state
        .db
        .update_quota(
            id,
            &quota_summary.raw_json,
            quota_summary.gemini_fraction,
            quota_summary.claude_fraction,
            now,
        )
        .await;

    let final_email = user_email.unwrap_or_else(|| format!("Account {}", id));

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "added",
            "account": {
                "id": id,
                "email": final_email,
                "gemini_remaining": quota_summary.gemini_fraction,
                "claude_remaining": quota_summary.claude_fraction,
                "enabled": true
            },
            "email": final_email,
            "id": id,
            "valid": true
        })),
    )
}

async fn fetch_user_email(client: &reqwest::Client, access_token: &str) -> Option<String> {
    let resp = client
        .get("https://www.googleapis.com/oauth2/v2/userinfo")
        .bearer_auth(access_token)
        .send()
        .await
        .ok()?;

    if resp.status().is_success() {
        let data: serde_json::Value = resp.json().await.ok()?;
        if let Some(em) = data.get("email").and_then(|v| v.as_str()) {
            return Some(em.to_string());
        }
    }
    None
}

pub(crate) async fn handle_toggle_account(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    match state.db.toggle_account(id).await {
        Ok(enabled) => (
            StatusCode::OK,
            Json(serde_json::json!({ "id": id, "enabled": enabled })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

pub(crate) async fn handle_delete_account(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    match state.db.delete_account(id).await {
        Ok(deleted) => {
            if deleted {
                (
                    StatusCode::OK,
                    Json(serde_json::json!({ "status": "deleted", "id": id })),
                )
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "Account not found" })),
                )
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}
