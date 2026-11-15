use crate::db::Database;
use crate::types::{ChatCompletionRequest, MultiProviderStats, ThirdPartyAccount, ThirdPartyAccountSummary, ThirdPartyStats};
use crate::AppState;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use log::{error, info, warn};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const THIRD_PARTY_MODELS: &[&str] = &[
    "deepseek-ai/DeepSeek-V4-Flash-0731",
    "deepseek-v4-flash",
    "MiniMaxAI/MiniMax-M2.7",
    "minimax-m2.7",
    "zai-org/GLM-5.3-Flash",
    "glm-5.3-flash",
];

pub struct ThirdPartyManager {
    db: Database,
    http_client: reqwest::Client,
    rotation_counter: Arc<AtomicUsize>,
}

impl ThirdPartyManager {
    pub fn new(db: Database) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(10))
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            .pool_max_idle_per_host(32)
            .build()
            .unwrap_or_default();

        Self {
            db,
            http_client,
            rotation_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn is_3pa_model(&self, model: &str) -> bool {
        let m = model.to_lowercase();
        m.starts_with("dahl-")
            || m.starts_with("dahl/")
            || m.contains("deepseek")
            || m.contains("minimax")
            || m.contains("glm")
    }

    pub fn resolve_model(&self, model: &str) -> &'static str {
        let m = model.to_lowercase();
        if m.contains("deepseek") {
            "deepseek-ai/DeepSeek-V4-Flash-0731"
        } else if m.contains("minimax") {
            "MiniMaxAI/MiniMax-M2.7"
        } else if m.contains("glm") {
            "zai-org/GLM-5.3-Flash"
        } else {
            "deepseek-ai/DeepSeek-V4-Flash-0731"
        }
    }

    pub(crate) async fn handle_chat_completions(
        &self,
        mut req: ChatCompletionRequest,
        auth_key_name: &str,
        _state: &AppState,
    ) -> Response {
        let target_model = self.resolve_model(&req.model);
        req.model = target_model.to_string();

        let accounts = match self.db.load_third_party_accounts("dahl").await {
            Ok(accs) => accs,
            Err(e) => {
                error!("[3PA:DB] Failed to load third-party accounts: {}", e);
                Vec::new()
            }
        };

        if accounts.is_empty() {
            warn!("[3PA] No active accounts in Dahl pool for request from '{}'", auth_key_name);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "message": "No active accounts in 3PA provider pool (Dahl Inference). Please add or re-enable accounts.",
                        "type": "provider_unavailable",
                        "code": "3pa_pool_empty"
                    }
                })),
            )
                .into_response();
        }

        let n = accounts.len();
        let upstream_url = "https://inference.dahl.global/v1/chat/completions";

        info!(
            "[3PA:REQ] Key: '{}' -> Model: '{}', Stream: {}, Pool size: {}",
            auth_key_name, req.model, req.stream, n
        );

        // Ротация бесконечная по кругу: при 429 или любой ошибке крутимся дальше.
        // За один запрос пробуем до N аккаунтов (полный круг).
        for attempt in 0..n {
            let idx = self.rotation_counter.fetch_add(1, Ordering::Relaxed) % n;
            let acc = &accounts[idx];

            let auth_header = format!("Bearer {}", acc.api_key);

            let send_payload = serde_json::json!({
                "model": req.model,
                "messages": req.messages,
                "stream": req.stream,
                "temperature": req.temperature,
                "max_tokens": req.max_tokens,
            });

            if req.stream {
                let send_res = self
                    .http_client
                    .post(upstream_url)
                    .header(header::AUTHORIZATION, &auth_header)
                    .header(header::CONTENT_TYPE, "application/json")
                    .json(&send_payload)
                    .send()
                    .await;

                match send_res {
                    Ok(resp) => {
                        let status = resp.status();
                        if status.is_success() {
                            info!(
                                "[3PA:STREAM:OK] Account #{} ({}) accepted stream for '{}' (Attempt {}/{})",
                                acc.id, acc.username, req.model, attempt + 1, n
                            );
                            let stream = resp.bytes_stream();
                            let body = axum::body::Body::from_stream(stream);
                            return (
                                StatusCode::OK,
                                [
                                    (header::CONTENT_TYPE, "text/event-stream"),
                                    (header::CACHE_CONTROL, "no-cache"),
                                    (header::CONNECTION, "keep-alive"),
                                ],
                                body,
                            )
                                .into_response();
                        } else {
                            let err_snippet = resp.text().await.unwrap_or_default();
                            warn!(
                                "[3PA:FAILOVER] Stream account #{} ({}) returned HTTP {}: {}. Advancing to next key (Attempt {}/{})",
                                acc.id, acc.username, status, err_snippet.chars().take(150).collect::<String>(), attempt + 1, n
                            );
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "[3PA:FAILOVER] Stream account #{} ({}) connection error: {}. Advancing to next key (Attempt {}/{})",
                            acc.id, acc.username, e, attempt + 1, n
                        );
                        continue;
                    }
                }
            } else {
                let send_res = self
                    .http_client
                    .post(upstream_url)
                    .header(header::AUTHORIZATION, &auth_header)
                    .header(header::CONTENT_TYPE, "application/json")
                    .json(&send_payload)
                    .send()
                    .await;

                match send_res {
                    Ok(resp) => {
                        let status = resp.status();
                        if status.is_success() {
                            info!(
                                "[3PA:JSON:OK] Account #{} ({}) responded for '{}' (Attempt {}/{})",
                                acc.id, acc.username, req.model, attempt + 1, n
                            );
                            let bytes = resp.bytes().await.unwrap_or_default();
                            return (
                                StatusCode::OK,
                                [(header::CONTENT_TYPE, "application/json")],
                                bytes,
                            )
                                .into_response();
                        } else {
                            let err_snippet = resp.text().await.unwrap_or_default();
                            warn!(
                                "[3PA:FAILOVER] Account #{} ({}) returned HTTP {}: {}. Advancing to next key (Attempt {}/{})",
                                acc.id, acc.username, status, err_snippet.chars().take(150).collect::<String>(), attempt + 1, n
                            );
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "[3PA:FAILOVER] Account #{} ({}) connection error: {}. Advancing to next key (Attempt {}/{})",
                            acc.id, acc.username, e, attempt + 1, n
                        );
                        continue;
                    }
                }
            }
        }

        // Если весь пул прошелся и ни один аккаунт не ответил успехом
        error!(
            "[3PA:EXHAUSTED] All {} accounts in Dahl pool failed or were rate-limited for request from '{}'",
            n, auth_key_name
        );
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": {
                    "message": format!("All {} accounts in 3PA pool (Dahl Inference) failed to respond or rate-limited (429)", n),
                    "type": "upstream_error",
                    "code": "3pa_pool_exhausted"
                }
            })),
        )
            .into_response()
    }
}

// ==========================================
// AXUM API HANDLERS ДЛЯ ДАШБОРДА
// ==========================================

pub(crate) async fn handle_get_pool(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let (auth_key_name, _auth_key_id, is_master) = match crate::authenticate_request(&headers, &state).await {
        crate::AuthResult::Allowed { key_name, key_id, is_master } => (key_name, key_id, is_master),
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "Unauthorized"})),
            )
                .into_response();
        }
    };

    if !is_master {
        warn!("[AUTH:3PA] Non-master key '{}' attempted to read 3PA pool", auth_key_name);
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Admin access required"})),
        )
            .into_response();
    }

    // 1. Dahl Inference
    let dahl_accounts = match state.db.load_all_third_party_accounts("dahl").await {
        Ok(accs) => accs,
        Err(e) => {
            error!("[3PA:DB] Error loading dahl accounts: {}", e);
            Vec::new()
        }
    };

    let build_stats = |provider_id: &str, provider_name: &str, models: Vec<String>, all_accounts: Vec<ThirdPartyAccount>| {
        let total_accounts = all_accounts.len();
        let active_accounts = all_accounts.iter().filter(|a| a.status == "active").count();
        let total_tokens: i64 = all_accounts.iter().map(|a| a.tokens).sum();

        let mut earliest_expiry: Option<String> = None;
        let mut expiring_soon_count = 0;
        let mut expiring_soon_tokens = 0;

        let mut dates: Vec<String> = all_accounts
            .iter()
            .filter_map(|a| a.trial_expires_at.clone())
            .collect();
        dates.sort();

        if let Some(first_date) = dates.first() {
            earliest_expiry = Some(first_date.clone());
            for a in &all_accounts {
                if a.trial_expires_at.as_deref() == Some(first_date.as_str()) {
                    expiring_soon_count += 1;
                    expiring_soon_tokens += a.tokens;
                }
            }
        }

        let summaries: Vec<ThirdPartyAccountSummary> = all_accounts
            .into_iter()
            .map(|a| {
                let masked = if a.api_key.len() > 14 {
                    format!("{}…{}", &a.api_key[..8], &a.api_key[a.api_key.len() - 6..])
                } else {
                    a.api_key.clone()
                };
                ThirdPartyAccountSummary {
                    id: a.id,
                    username: a.username,
                    key_masked: masked,
                    tokens: a.tokens,
                    trial_expires_at: a.trial_expires_at,
                    status: a.status,
                }
            })
            .collect();

        ThirdPartyStats {
            provider: provider_id.to_string(),
            provider_name: provider_name.to_string(),
            total_tokens,
            total_accounts,
            active_accounts,
            earliest_expiry,
            expiring_soon_count,
            expiring_soon_tokens,
            models,
            accounts: summaries,
        }
    };

    let dahl_stats = build_stats(
        "dahl",
        "Dahl Inference",
        vec![
            "deepseek-ai/DeepSeek-V4-Flash-0731".to_string(),
            "MiniMaxAI/MiniMax-M2.7".to_string(),
            "zai-org/GLM-5.3-Flash".to_string(),
        ],
        dahl_accounts,
    );

    let providers = vec![dahl_stats];
    let total_tokens: i64 = providers.iter().map(|p| p.total_tokens).sum();
    let total_accounts: usize = providers.iter().map(|p| p.total_accounts).sum();
    let active_accounts: usize = providers.iter().map(|p| p.active_accounts).sum();

    let multi_stats = MultiProviderStats {
        total_tokens,
        total_accounts,
        active_accounts,
        providers,
    };

    (StatusCode::OK, Json(multi_stats)).into_response()
}

pub(crate) async fn handle_sync_pool(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(accounts): Json<Vec<ThirdPartyAccount>>,
) -> impl IntoResponse {
    let (_auth_key_name, _auth_key_id, is_master) = match crate::authenticate_request(&headers, &state).await {
        crate::AuthResult::Allowed { key_name, key_id, is_master } => (key_name, key_id, is_master),
        _ => return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Unauthorized"}))).into_response(),
    };

    if !is_master {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "Forbidden"}))).into_response();
    }

    let mut imported = 0;
    for a in accounts {
        let res = state
            .db
            .upsert_third_party_account(
                &a.provider,
                &a.username,
                &a.fingerprint,
                &a.api_key,
                a.tokens,
                a.trial_expires_at.as_deref(),
            )
            .await;
        if res.is_ok() {
            imported += 1;
        }
    }

    info!("[3PA:SYNC] Synchronized {} accounts into third_party_accounts", imported);
    (StatusCode::OK, Json(json!({"status": "ok", "imported": imported}))).into_response()
}

pub(crate) async fn handle_toggle_account(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let (_auth_key_name, _auth_key_id, is_master) = match crate::authenticate_request(&headers, &state).await {
        crate::AuthResult::Allowed { key_name, key_id, is_master } => (key_name, key_id, is_master),
        _ => return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Unauthorized"}))).into_response(),
    };

    if !is_master {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "Forbidden"}))).into_response();
    }

    match state.db.toggle_third_party_account(id).await {
        Ok(new_status) => (StatusCode::OK, Json(json!({"status": "ok", "new_status": new_status}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

pub(crate) async fn handle_delete_account(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let (_auth_key_name, _auth_key_id, is_master) = match crate::authenticate_request(&headers, &state).await {
        crate::AuthResult::Allowed { key_name, key_id, is_master } => (key_name, key_id, is_master),
        _ => return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Unauthorized"}))).into_response(),
    };

    if !is_master {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "Forbidden"}))).into_response();
    }

    match state.db.delete_third_party_account(id).await {
        Ok(_) => (StatusCode::OK, Json(json!({"status": "ok"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}
