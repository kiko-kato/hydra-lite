pub mod anchor_stabilizer;
pub mod anthropic;
pub mod antidote;
pub mod api_accounts;
pub mod api_keys;
pub mod api_models;
pub mod circuit_breaker;
pub mod client;
pub mod config;
pub mod db;
pub mod logger;
pub mod minifier;
pub mod model_manager;
pub mod oauth;
pub mod oauth_worker;
pub mod quota;
pub mod signatures;
pub mod sse;
pub mod telemetry;
pub mod translator;
pub mod types;

pub use config::Config;
pub use logger::{AuditLogger, RequestLog};
pub use telemetry::TelemetryEngine;

use anyhow::Result;
use futures::StreamExt;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
    Json, Router,
};
use db::Database;
use log::info;
use sha2::{Digest, Sha256};
use signatures::SignatureStore;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use types::{ChatCompletionRequest, ModelItem, ModelListResponse};

#[derive(Clone)]
pub struct RpmTracker {
    timestamps: Arc<parking_lot::Mutex<std::collections::HashMap<i64, Vec<Instant>>>>,
}

impl RpmTracker {
    pub fn new() -> Self {
        Self {
            timestamps: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    pub fn check_and_record(&self, key_id: i64, rpm_limit: i64) -> bool {
        if rpm_limit <= 0 {
            return true;
        }
        let now = Instant::now();
        let one_minute_ago = now.checked_sub(Duration::from_secs(60)).unwrap_or(now);
        let mut map = self.timestamps.lock();
        let list = map.entry(key_id).or_insert_with(Vec::new);
        list.retain(|&t| t > one_minute_ago);
        if list.len() >= rpm_limit as usize {
            return false;
        }
        list.push(now);
        true
    }
}

#[derive(Clone)]
pub struct StreamLease {
    acc_id: i64,
    map: Arc<parking_lot::RwLock<std::collections::HashMap<i64, usize>>>,
}

impl StreamLease {
    pub fn new(acc_id: i64, map: Arc<parking_lot::RwLock<std::collections::HashMap<i64, usize>>>) -> Self {
        {
            let mut guard = map.write();
            *guard.entry(acc_id).or_insert(0) += 1;
        }
        Self { acc_id, map }
    }
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        let mut guard = self.map.write();
        if let Some(count) = guard.get_mut(&self.acc_id) {
            if *count > 0 {
                *count -= 1;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) config: Arc<parking_lot::RwLock<Config>>,
    pub(crate) db: Database,
    pub(crate) client: client::UpstreamClient,
    pub(crate) signatures: SignatureStore,
    #[allow(dead_code)]
    pub(crate) logger: AuditLogger,
    pub(crate) telemetry: TelemetryEngine,
    pub(crate) rpm_tracker: Arc<RpmTracker>,
    pub(crate) antidote: Arc<antidote::AntidoteEngine>,
    pub(crate) circuit_breaker: Arc<circuit_breaker::CircuitBreakerManager>,
    pub(crate) model_manager: Arc<model_manager::ModelManager>,
    pub(crate) active_streams: Arc<parking_lot::RwLock<std::collections::HashMap<i64, usize>>>,
}

pub async fn run_router(config: Arc<Config>) -> Result<()> {
    info!(
        "[ROUTER] Starting Hydra-Lite Router with DB: {:?}",
        config.database.path
    );
    info!(
        "[ROUTER] Audit Logs Database: {:?}",
        config.database.logs_db_path
    );

    let db = Database::open(&config.database.path)?;
    let logger = AuditLogger::init(&config.database.logs_db_path)?;
    logger.log_event("INFO", "router", "Hydra-Lite Router initialized");
    let telemetry = TelemetryEngine::init(&config.database.logs_db_path)?;

    let gen_ep = config
        .upstream
        .generate_endpoints
        .first()
        .cloned()
        .unwrap_or_default();
    let stream_ep = config
        .upstream
        .stream_endpoints
        .first()
        .cloned()
        .unwrap_or_default();

    let client = client::UpstreamClient::new(
        &config.upstream.proxy,
        config.upstream.connect_timeout_ms,
        config.upstream.total_timeout_ms,
        &gen_ep,
        &stream_ep,
    )?;
    let signatures = SignatureStore::new();

    let config_lock = Arc::new(parking_lot::RwLock::new((*config).clone()));

    // Запуск фонового перезагрузчика динамической конфигурации
    let cfg_lock_bg = config_lock.clone();
    tokio::spawn(async move {
        let mut last_mtime = None;
        loop {
            sleep(Duration::from_millis(1500)).await;
            if let Ok(metadata) = std::fs::metadata("config.toml") {
                if let Ok(mtime) = metadata.modified() {
                    if let Some(prev) = last_mtime {
                        if mtime > prev {
                            match Config::load("config.toml") {
                                Ok(new_cfg) => {
                                    info!(
                                        "[CONFIG:HOT_RELOAD] config.toml updated on the fly! caching.enabled={}, debug={}, hrw={}, exact={}",
                                        new_cfg.caching.enabled,
                                        new_cfg.caching.debug,
                                        new_cfg.caching.hrw_affinity,
                                        new_cfg.caching.exact_cache
                                    );
                                    *cfg_lock_bg.write() = new_cfg;
                                }
                                Err(e) => {
                                    log::warn!(
                                        "[CONFIG:HOT_RELOAD] Failed to parse config.toml: {}",
                                        e
                                    );
                                }
                            }
                        }
                    }
                    last_mtime = Some(mtime);
                }
            }
        }
    });

    let kb_path = std::path::Path::new("data/kb.sqlite");
    let antidote = Arc::new(
        antidote::AntidoteEngine::new(kb_path, None).unwrap_or_else(|e| {
            log::warn!(
                "[ANTIDOTE] Failed to init DB at {:?}, fallback in-memory/stub: {:?}",
                kb_path,
                e
            );
            antidote::AntidoteEngine::new(std::path::Path::new("kb.sqlite"), None)
                .unwrap_or_else(|_| panic!("Failed to init Antidote engine"))
        }),
    );
    let circuit_breaker = Arc::new(circuit_breaker::CircuitBreakerManager::new(3));

    let model_manager = Arc::new(model_manager::ModelManager::new(
        Arc::new(db.clone()),
        Arc::new(client.clone()),
        config.generation_defaults.default_model.clone(),
    ));
    model_manager.clone().start_worker(Duration::from_secs(300));

    let active_streams = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));

    let state = AppState {
        config: config_lock,
        db: db.clone(),
        client: client.clone(),
        signatures,
        logger,
        telemetry,
        rpm_tracker: Arc::new(RpmTracker::new()),
        antidote,
        circuit_breaker,
        model_manager,
        active_streams,
    };

    // Фоновый демон обновления токенов Google OAuth (за 5 мин до истечения)
    let oauth_worker = Arc::new(oauth_worker::OAuthWorker::new(
        db.clone(),
        client.direct().clone(),
        config.oauth.client_id.clone(),
        config.oauth.client_secret.clone(),
    ));
    oauth_worker.start();

    // Фоновый демон предпрогрева HTTP/2 сокетов (каждые 20 секунд)
    let client_warmup = client.clone();
    tokio::spawn(async move {
        // Мгновенный первый прогрев при старте
        client_warmup.warmup_socket().await;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(20));
        loop {
            interval.tick().await;
            client_warmup.warmup_socket().await;
        }
    });

    // Запуск фоновой задачи обновления квот
    let db_bg = db.clone();
    let client_bg = client.clone();
    let state_bg_cfg = state.config.clone();
    let poll_interval = config.quota.poll_interval_sec;
    tokio::spawn(async move {
        loop {
            if let Ok(accounts) = db_bg.load_accounts().await {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();
                for acc in accounts {
                    if !acc.enabled {
                        continue;
                    }

                    // Проверка необходимости обновления токена
                    let mut token = acc.access_token.clone().unwrap_or_default();
                    if token.is_empty() || acc.expires_at < now + 60.0 {
                        let (client_id, client_secret) = {
                            let cfg = state_bg_cfg.read();
                            (cfg.oauth.client_id.clone(), cfg.oauth.client_secret.clone())
                        };
                        if let Ok((new_token, exp)) = oauth::refresh_access_token(
                            client_bg.direct(),
                            &acc.refresh_token,
                            &client_id,
                            &client_secret,
                        )
                        .await
                        {
                            token = new_token.clone();
                            let _ = db_bg.update_tokens(acc.id, &new_token, exp).await;
                        }
                    }

                    if token.is_empty() {
                        continue;
                    }

                    // Опрос текущих квот
                    if let Ok(qs) = quota::fetch_quota_summary(client_bg.inner(), &token).await {
                        let _ = db_bg
                            .update_quota(
                                acc.id,
                                &qs.raw_json,
                                qs.gemini_fraction,
                                qs.claude_fraction,
                                now,
                            )
                            .await;
                        info!(
                            "[ROUTER:QUOTA] Acc #{} ({}): Gemini: {:.1}%, Claude: {:.1}%",
                            acc.id,
                            acc.email.as_deref().unwrap_or("anon"),
                            qs.gemini_fraction * 100.0,
                            qs.claude_fraction * 100.0
                        );
                    }
                }
            }
            sleep(Duration::from_secs(poll_interval)).await;
        }
    });

    let app = Router::new()
        .route("/", get(handle_admin_ui))
        .route("/admin", get(handle_admin_ui))
        .route("/health", get(handle_health))
        .route("/v1/models", get(handle_models))
        .route("/v1/chat/completions", post(handle_chat_completions))
        .route("/v1/messages", post(anthropic::handle_anthropic_messages))
        .route("/api/auth/login", post(handle_admin_login))
        .route(
            "/api/config",
            get(handle_get_config).post(handle_update_config),
        )
        .route("/api/telemetry/summary", get(handle_telemetry_summary))
        .route("/api/telemetry/requests", get(handle_telemetry_requests))
        .route("/api/telemetry/errors", get(handle_telemetry_errors))
        .route("/api/keys/analytics", get(handle_keys_analytics))
        .route("/api/user/me", get(handle_user_me))
        .route("/api/user/requests", get(handle_user_requests))
        .route("/api/antidote/problems", get(handle_antidote_problems).post(handle_antidote_add))
        .route("/api/antidote/stats", get(handle_antidote_stats))
        .route(
            "/api/accounts",
            get(api_accounts::handle_get_accounts).post(api_accounts::handle_add_account),
        )
        .route(
            "/api/accounts/:id",
            delete(api_accounts::handle_delete_account),
        )
        .route(
            "/api/accounts/:id/toggle",
            patch(api_accounts::handle_toggle_account),
        )
        .route(
            "/api/keys",
            get(api_keys::handle_get_keys).post(api_keys::handle_create_key),
        )
        .route(
            "/api/keys/:id",
            patch(api_keys::handle_update_key).delete(api_keys::handle_delete_key),
        )
        .route(
            "/api/models/config",
            get(api_models::handle_get_model_configs),
        )
        .route(
            "/api/models/config/:id",
            patch(api_models::handle_update_model_config),
        )
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(state);

    info!(
        "[ROUTER] AntiGravity Headless Router listening on http://{}",
        config.server.bind
    );
    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn handle_health(State(state): State<AppState>) -> impl IntoResponse {
    let accounts = state.db.load_all_accounts().await.unwrap_or_default();
    let total = accounts.len();
    let min_fraction = state.config.read().quota.min_fraction;
    let now_sec = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let active = accounts
        .iter()
        .filter(|a| a.enabled && a.gemini_remaining >= min_fraction && a.cooldown_until <= now_sec)
        .count();

    let account_items: Vec<serde_json::Value> = accounts
        .iter()
        .map(|a| {
            let (g_5h, g_wk, c_5h, c_wk, _, _, _, _) = api_accounts::parse_account_windows(
                a.quota_json.as_deref(),
                a.gemini_remaining,
                a.claude_remaining,
            );
            let status = if !a.enabled {
                "DISABLED"
            } else if a.cooldown_until > now_sec {
                "COOLDOWN"
            } else {
                "ACTIVE"
            };

            serde_json::json!({
                "id": a.id,
                "email": a.email.clone().unwrap_or_else(|| "anon".to_string()),
                "status": status,
                "gemini_remaining_pct": (a.gemini_remaining * 100.0).round(),
                "claude_remaining_pct": (a.claude_remaining * 100.0).round(),
                "quotas": {
                    "g5h_pct": (g_5h * 100.0).round(),
                    "g1w_pct": (g_wk * 100.0).round(),
                    "c5h_pct": (c_5h * 100.0).round(),
                    "c1w_pct": (c_wk * 100.0).round(),
                }
            })
        })
        .collect();

    let avg = state
        .telemetry
        .get_averages()
        .unwrap_or(telemetry::TelemetryAverages {
            total_requests: 0,
            avg_ttft_ms: 0.0,
            avg_thinking_time_ms: 0.0,
            avg_generation_time_ms: 0.0,
            avg_clean_tok_per_sec: 0.0,
            avg_max_chunk_gap_ms: 0.0,
        });

    let summary = state.telemetry.get_summary().ok();
    let cache_stats = summary.map(|s| {
        serde_json::json!({
            "cache_hitrate_percent": s.global_hitrate_percent,
            "tokens_saved_by_cache": s.total_tokens_saved_by_cache,
            "total_prompt_tokens": s.total_prompt_tokens,
            "total_completion_tokens": s.total_completion_tokens,
            "successful_requests": s.successful_requests,
            "failed_requests": s.failed_requests,
        })
    });

    Json(serde_json::json!({
        "status": "ok",
        "service": "antigravity_router",
        "caching": true,
        "cache_strategy": "hrw_affinity+exact_sqlite+prefix_norm",
        "total_accounts": total,
        "active_valid_accounts": active,
        "cache": cache_stats,
        "averages": {
            "total_requests": avg.total_requests,
            "avg_clean_tok_per_sec": avg.avg_clean_tok_per_sec,
            "avg_generation_time_ms": avg.avg_generation_time_ms,
            "avg_thinking_time_ms": avg.avg_thinking_time_ms,
            "avg_max_chunk_gap_ms": avg.avg_max_chunk_gap_ms,
            "avg_ttft_ms": avg.avg_ttft_ms,
        },
        "accounts": account_items,
    }))
}



async fn handle_get_config(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.config.read().clone();
    (
        StatusCode::OK,
        Json(serde_json::to_value(&cfg).unwrap_or_default()),
    )
}

async fn handle_update_config(
    State(state): State<AppState>,
    Json(update_cfg): Json<Config>,
) -> impl IntoResponse {
    if let Err(e) = update_cfg.save("config.toml") {
        log::error!("[CONFIG:UPDATE:ERR] Failed to save config.toml: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "status": "error", "error": e.to_string() })),
        );
    }
    *state.config.write() = update_cfg.clone();
    log::info!("[CONFIG:UPDATE:OK] Configuration updated and saved to config.toml successfully");
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "ok", "config": update_cfg })),
    )
}

async fn handle_telemetry_summary(State(state): State<AppState>) -> impl IntoResponse {
    match state.telemetry.get_summary() {
        Ok(summary) => (
            StatusCode::OK,
            Json(serde_json::to_value(summary).unwrap_or_default()),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

#[derive(serde::Deserialize)]
struct TelemetryQuery {
    limit: Option<usize>,
}

async fn handle_telemetry_requests(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<TelemetryQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(500);
    match state.telemetry.get_recent_requests(limit) {
        Ok(reqs) => (
            StatusCode::OK,
            Json(serde_json::to_value(reqs).unwrap_or_default()),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

async fn handle_telemetry_errors(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<TelemetryQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(500);
    match state.telemetry.get_recent_errors(limit) {
        Ok(errs) => (
            StatusCode::OK,
            Json(serde_json::to_value(errs).unwrap_or_default()),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

async fn handle_admin_ui(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut token = None;

    if let Some(auth) = headers.get("authorization").and_then(|h| h.to_str().ok()) {
        if let Some(bearer) = auth.strip_prefix("Bearer ") {
            token = Some(bearer.trim().to_string());
        }
    }

    if token.is_none() {
        if let Some(cookie_hdr) = headers.get("cookie").and_then(|h| h.to_str().ok()) {
            for part in cookie_hdr.split(';') {
                let trimmed = part.trim();
                if let Some(val) = trimmed.strip_prefix("hydra_admin_auth=") {
                    token = Some(val.trim().to_string());
                    break;
                }
            }
        }
    }

    let (is_auth, is_admin) = if let Some(ref t) = token {
        if t == &state.config.read().server.api_key {
            (true, true)
        } else if let Ok(Some(k)) = state.db.get_key_by_token(t).await {
            if k.enabled {
                (true, k.is_admin)
            } else {
                (false, false)
            }
        } else {
            (false, false)
        }
    } else {
        (false, false)
    };

    if !is_auth {
        axum::response::Html(include_str!("../static/login.html"))
    } else if is_admin {
        axum::response::Html(include_str!("../static/dashboard.html"))
    } else {
        axum::response::Html(include_str!("../static/user_cabinet.html"))
    }
}

#[derive(serde::Deserialize)]
struct AdminLoginRequest {
    key: String,
}

async fn handle_admin_login(
    State(state): State<AppState>,
    Json(payload): Json<AdminLoginRequest>,
) -> impl IntoResponse {
    let raw_key = payload.key.trim();
    if raw_key.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "status": "error", "message": "API key cannot be empty" })),
        );
    }

    if raw_key == state.config.read().server.api_key {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "ok",
                "role": "admin",
                "name": "master_admin"
            })),
        );
    }

    match state.db.get_key_by_token(raw_key).await {
        Ok(Some(k)) => {
            if !k.enabled {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "status": "error", "message": "API ключ отключен" })),
                );
            }
            if let Some(exp) = k.expires_at {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();
                if now > exp {
                    return (
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({ "status": "error", "message": "Срок действия API ключа истек" })),
                    );
                }
            }
            let role = if k.is_admin { "admin" } else { "user" };
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "ok",
                    "role": role,
                    "name": k.name
                })),
            )
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "status": "error", "message": "Неверный API ключ" })),
        ),
    }
}

async fn handle_keys_analytics(State(state): State<AppState>) -> impl IntoResponse {
    match state.db.get_keys_consumption_analytics().await {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

async fn handle_user_me(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut token = None;
    if let Some(auth) = headers.get("authorization").and_then(|h| h.to_str().ok()) {
        if let Some(bearer) = auth.strip_prefix("Bearer ") {
            token = Some(bearer.trim().to_string());
        }
    }
    if token.is_none() {
        if let Some(cookie_hdr) = headers.get("cookie").and_then(|h| h.to_str().ok()) {
            for part in cookie_hdr.split(';') {
                let trimmed = part.trim();
                if let Some(val) = trimmed.strip_prefix("hydra_admin_auth=") {
                    token = Some(val.trim().to_string());
                    break;
                }
            }
        }
    }

    let tok = match token {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Unauthorized" })),
            );
        }
    };

    match state.db.get_key_by_token(&tok).await {
        Ok(Some(k)) => (StatusCode::OK, Json(serde_json::to_value(&k).unwrap_or_default())),
        _ => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid key" })),
        ),
    }
}

async fn handle_user_requests(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let mut token = None;
    if let Some(auth) = headers.get("authorization").and_then(|h| h.to_str().ok()) {
        if let Some(bearer) = auth.strip_prefix("Bearer ") {
            token = Some(bearer.trim().to_string());
        }
    }
    if token.is_none() {
        if let Some(cookie_hdr) = headers.get("cookie").and_then(|h| h.to_str().ok()) {
            for part in cookie_hdr.split(';') {
                let trimmed = part.trim();
                if let Some(val) = trimmed.strip_prefix("hydra_admin_auth=") {
                    token = Some(val.trim().to_string());
                    break;
                }
            }
        }
    }

    let tok = match token {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Unauthorized" })),
            );
        }
    };

    if let Ok(Some(k)) = state.db.get_key_by_token(&tok).await {
        match state.telemetry.get_user_requests(&k.name, 50) {
            Ok(reqs) => (StatusCode::OK, Json(serde_json::to_value(reqs).unwrap_or_default())),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            ),
        }
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid key" })),
        )
    }
}

#[derive(serde::Deserialize)]
struct AntidoteQuery {
    q: Option<String>,
    limit: Option<usize>,
}

async fn handle_antidote_problems(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<AntidoteQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50).min(200);
    let res = if let Some(ref q) = query.q {
        let trimmed = q.trim();
        if !trimmed.is_empty() {
            state.antidote.search_problems(trimmed, limit)
        } else {
            state.antidote.get_recent_problems(limit)
        }
    } else {
        state.antidote.get_recent_problems(limit)
    };

    match res {
        Ok(items) => (StatusCode::OK, Json(serde_json::json!({ "problems": items }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

async fn handle_antidote_stats(State(state): State<AppState>) -> impl IntoResponse {
    match state.antidote.get_stats() {
        Ok(stats) => (StatusCode::OK, Json(stats)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

#[derive(serde::Deserialize)]
struct AddProblemRequest {
    title: String,
    error_signature: String,
    solution: String,
    tags: Option<String>,
}

async fn handle_antidote_add(
    State(state): State<AppState>,
    Json(payload): Json<AddProblemRequest>,
) -> impl IntoResponse {
    let tags = payload.tags.unwrap_or_default();
    match state.antidote.add_problem(&payload.title, &payload.error_signature, &payload.solution, &tags) {
        Ok(issue_id) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "status": "created", "issue_id": issue_id })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

fn compute_hrw_score(affinity_key: &str, acc_id: i64, quota_fraction: f64) -> f64 {
    let mut h = Sha256::new();
    h.update(affinity_key.as_bytes());
    h.update(b":");
    h.update(acc_id.to_string().as_bytes());
    let hash_bytes = h.finalize();
    let weight_u64 = u64::from_be_bytes(hash_bytes[0..8].try_into().unwrap_or([0u8; 8]));
    let hash_norm = (weight_u64 as f64) / (u64::MAX as f64);
    hash_norm * quota_fraction.sqrt()
}

async fn handle_models(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<ModelListResponse>, (StatusCode, Json<serde_json::Value>)> {
    let token_present = headers.contains_key("Authorization") || headers.contains_key("x-api-key");
    if token_present {
        match authenticate_request(&headers, &state).await {
            AuthResult::Allowed { .. } => {}
            AuthResult::Unauthorized(msg) => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(
                        serde_json::json!({ "error": { "message": msg, "type": "unauthorized" } }),
                    ),
                ));
            }
            AuthResult::Forbidden(msg) => {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": { "message": msg, "type": "forbidden" } })),
                ));
            }
            AuthResult::RateLimited(msg) => {
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(
                        serde_json::json!({ "error": { "message": msg, "type": "rate_limit_exceeded" } }),
                    ),
                ));
            }
        }
    }

    let items = state.model_manager.get_model_items().await;
    let final_models: Vec<ModelItem> = items
        .into_iter()
        .filter_map(|v| {
            let id = v.get("id")?.as_str()?.to_string();
            let created = v.get("created").and_then(|c| c.as_u64()).unwrap_or(0);
            Some(ModelItem {
                id,
                object: "model",
                created,
                owned_by: "google",
            })
        })
        .collect();

    Ok(Json(ModelListResponse {
        object: "list",
        data: final_models,
    }))
}

async fn handle_chat_completions(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(mut req): Json<ChatCompletionRequest>,
) -> Response {
    let (auth_key_name, auth_key_id) = match authenticate_request(&headers, &state).await {
        AuthResult::Allowed { key_name, key_id } => (key_name, key_id),
        AuthResult::Unauthorized(msg) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": { "message": msg, "type": "unauthorized" } })),
            )
                .into_response();
        }
        AuthResult::Forbidden(msg) => {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "error": { "message": msg, "type": "forbidden" } })),
            )
                .into_response();
        }
        AuthResult::RateLimited(msg) => {
            return (StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({ "error": { "message": msg, "type": "rate_limit_exceeded" } }))).into_response();
        }
    };

    // Адаптивное разрешение модели через ModelManager (алиасы -> target_id, а если нет - голое название напрямую!)
    let resolved_model = state.model_manager.resolve(&req.model).await;
    req.model = resolved_model;

    let cfg = state.config.read().clone();

    // 1. Anchor Cache Stabilizer (freeze system prompt, move dynamic date/time to messages.last())
    anchor_stabilizer::AnchorStabilizer::stabilize_openai(&mut req.messages);

    // 2. Anti-Loop Circuit Breaker
    state
        .circuit_breaker
        .inspect_and_trip_openai(&auth_key_name, &mut req.messages);

    // 3. Hydra Antidote Self-Healing Interceptor
    state
        .antidote
        .inspect_and_inject_openai(&mut req.messages)
        .await;

    let start_time = Instant::now();
    let debug = cfg.caching.debug;

    let _user_prompt_preview: Option<String> = req
        .messages
        .last()
        .and_then(|m| m.get("content"))
        .and_then(|v| v.as_str())
        .map(|s| s.chars().take(250).collect());

    if debug {
        log::info!(
            "[CACHE:DEBUG][REQ_START] Model: '{}', Stream: {}, Messages: {}, Temp: {:?}",
            req.model,
            req.stream,
            req.messages.len(),
            req.temperature
        );
    }

    // Exact-кэш намеренно ликвидирован: только честный префикс-кэшинг на GPU/TPU Google/Claude через HRW affinity!
    let exact_hash: Option<String> = None;

    // -------------------------------------------------------------------------
    // ПОДГОТОВКА И ПРОВЕРКА АККАУНТОВ
    // -------------------------------------------------------------------------
    let accounts = match state.db.load_accounts().await {
        Ok(accs) => accs,
        Err(e) => {
            log::error!("[CRITICAL] Database error loading accounts: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("DB error: {}", e),
            )
                .into_response();
        }
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let is_claude = req.model.contains("claude") || req.model.contains("gpt");

    let mut eligible: Vec<_> = accounts
        .into_iter()
        .filter(|a| {
            if !a.enabled {
                return false;
            }
            if a.cooldown_until > now {
                return false;
            }
            let fraction = if is_claude {
                a.claude_remaining
            } else {
                a.gemini_remaining
            };
            fraction >= cfg.quota.min_fraction
        })
        .collect();

    if eligible.is_empty() {
        log::error!(
            "[REPRO:DUMP] All accounts exhausted! Model: {}, Messages: {}",
            req.model,
            req.messages.len()
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": {
                    "message": "All AntiGravity accounts have < 2% quota remaining.",
                    "type": "insufficient_quota",
                }
            })),
        )
            .into_response();
    }

    // -------------------------------------------------------------------------
    // ЭТАП 1: HRW AFFINITY ROUTING (Привязка сессии к аккаунту)
    // -------------------------------------------------------------------------
    let affinity_key = translator::compute_session_affinity_key(&req);
    let active_streams_snapshot = state.active_streams.read().clone();
    if cfg.caching.enabled && cfg.caching.hrw_affinity {
        eligible.sort_by(|a, b| {
            let rem_a = if is_claude {
                a.claude_remaining
            } else {
                a.gemini_remaining
            };
            let rem_b = if is_claude {
                b.claude_remaining
            } else {
                b.gemini_remaining
            };

            let in_flight_a = active_streams_snapshot.get(&a.id).copied().unwrap_or(0);
            let in_flight_b = active_streams_snapshot.get(&b.id).copied().unwrap_or(0);

            let penalty_a = if in_flight_a >= 2 { 100.0 } else { 1.0 + (in_flight_a as f64) * 2.0 };
            let penalty_b = if in_flight_b >= 2 { 100.0 } else { 1.0 + (in_flight_b as f64) * 2.0 };

            let score_a = compute_hrw_score(&affinity_key, a.id, rem_a) / penalty_a;
            let score_b = compute_hrw_score(&affinity_key, b.id, rem_b) / penalty_b;
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if debug {
            log::info!(
                "[CACHE:DEBUG][HRW:ROUTING] Affinity Key: '{}'",
                &affinity_key[..12]
            );
            for (idx, acc) in eligible.iter().take(3).enumerate() {
                let rem = if is_claude {
                    acc.claude_remaining
                } else {
                    acc.gemini_remaining
                };
                let score = compute_hrw_score(&affinity_key, acc.id, rem);
                log::info!(
                    "   Rank #{}: Acc #{} ({}) -> HRW Score: {:.6}, Quota Rem: {:.1}%",
                    idx + 1,
                    acc.id,
                    acc.email.as_deref().unwrap_or("anon"),
                    score,
                    rem * 100.0
                );
            }
        }
    } else {
        eligible.sort_by(|a, b| {
            let f_a = if is_claude {
                a.claude_remaining
            } else {
                a.gemini_remaining
            };
            let f_b = if is_claude {
                b.claude_remaining
            } else {
                b.gemini_remaining
            };
            f_b.partial_cmp(&f_a).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    // -------------------------------------------------------------------------
    // ЭТАП 2 И 3: ТРАНСЛЯЦИЯ И ПРОВЕРКА ПОРОГА 1024 ТОКЕНА
    // -------------------------------------------------------------------------
    eprintln!("[TRACE 1] Entering translate");
    let (contents, sys_inst) = translator::convert_messages_to_gemini(&req, &state.signatures);
    eprintln!("[TRACE 2] Converted messages");
    let tools = translator::convert_tools(&req.tools);
    eprintln!("[TRACE 3] Converted tools");

    // Проверка границы префикса для активации кэша Google / Anthropic
    let mut total_chars = 0;
    if let Some(ref sys) = sys_inst {
        total_chars += serde_json::to_string(sys).unwrap_or_default().len();
    }
    if let Some(ref t) = tools {
        total_chars += serde_json::to_string(t).unwrap_or_default().len();
    }
    let est_prefix_tokens = total_chars / 3 + 1;
    if est_prefix_tokens >= 1024 {
        if debug {
            log::info!("[CACHE:DEBUG][BOUND:OK] Prefix ~{} tokens (>= 1024 threshold). Eligible for server-side cache boost!", est_prefix_tokens);
        }
    } else {
        log::warn!("[CACHE:WARN][BOUND:BELOW] Prefix ~{} tokens (< 1024). Google/Claude will NOT cache this prefix.", est_prefix_tokens);
    }

    // Рекурсивное вычищение всех вхождений cache_control из структуры
    fn strip_cache_control_recursive(val: &mut serde_json::Value) {
        match val {
            serde_json::Value::Object(map) => {
                map.remove("cache_control");
                for (_, v) in map.iter_mut() {
                    strip_cache_control_recursive(v);
                }
            }
            serde_json::Value::Array(arr) => {
                for v in arr.iter_mut() {
                    strip_cache_control_recursive(v);
                }
            }
            _ => {}
        }
    }

    let mut inner_req = serde_json::json!({
        "contents": contents,
    });
    if let Some(sys) = sys_inst {
        inner_req["systemInstruction"] = sys;
    }
    if let Some(t) = tools {
        inner_req["tools"] = t;
    }

    strip_cache_control_recursive(&mut inner_req);

    let (max_output_tokens, thinking_budget) = if is_claude {
        let max_t = req.max_tokens.unwrap_or(64000).min(64000);
        let budget = max_t.saturating_sub(1).min(63999);
        (max_t, budget)
    } else {
        (req.max_tokens.unwrap_or(65536).min(65536), 65535)
    };

    let mut gen_cfg = serde_json::json!({
        "maxOutputTokens": max_output_tokens,
        "thinkingConfig": {
            "thinkingBudget": thinking_budget,
            "includeThoughts": true
        }
    });
    if let Ok(body_str) = serde_json::to_string(&inner_req) {
        if body_str.contains("cache_control") {
            log::warn!("[LEAK_ALERT] inner_req still contains cache_control!");
            let _ = std::fs::write("C:/Hydra-Headless/leaked_req.json", &body_str);
        }
    }
    if let Some(temp) = req.temperature {
        gen_cfg["temperature"] = serde_json::json!(temp);
    }
    inner_req["generationConfig"] = gen_cfg;

    let safety_categories = [
        "HARM_CATEGORY_HARASSMENT",
        "HARM_CATEGORY_HATE_SPEECH",
        "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "HARM_CATEGORY_DANGEROUS_CONTENT",
        "HARM_CATEGORY_CIVIC_INTEGRITY",
    ];
    let safety_settings: Vec<serde_json::Value> = safety_categories
        .iter()
        .map(|c| {
            serde_json::json!({
                "category": c,
                "threshold": "BLOCK_NONE"
            })
        })
        .collect();
    inner_req["safetySettings"] = serde_json::json!(safety_settings);

    let upstream_model = match req.model.as_str() {
        "nano-banana" | "nano-banana-pro" | "nano-banana-2" => "gemini-3.1-flash-image",
        "gemini-3.8-flash"
        | "gemini-3.8-flash-high"
        | "gemini-3.8-flash-medium"
        | "gemini-3.8-flash-low" => "gemini-3.8-flash-tiered",
        "gemini-2.5-flash-high" => "gemini-2.5-flash",
        m if m.contains("sonnet-5-5") || m.contains("sonnet-5.5") => "claude-sonnet-5-5",
        m if m.contains("opus-5-5") || m.contains("opus-5.5") => "claude-opus-5-5",
        m if m.contains("sonnet") => "claude-sonnet-4-6",
        m if m.contains("opus") => "claude-opus-4-6-thinking",
        m if m.contains("argon") => "gemini-4-argon",
        other => other,
    };

    let mut gemini_payload = serde_json::json!({
        "project": "",
        "model": upstream_model,
        "request": inner_req,
        "userAgent": "antigravity/hub/2.23.1 darwin/arm64",
        "requestType": "agent",
    });

    // Zero-loss нормализация и минификация контекста (экономия 25-35% токенов)
    minifier::minify_gemini_payload(&mut gemini_payload);

    let last_error_response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::json!({
            "error": {
                "message": "All eligible accounts failed or exhausted",
                "type": "upstream_error",
            }
        })),
    )
        .into_response();

    let mut proxy_transport_failed = false;
    let mut last_transport_err = String::new();

    let mut last_account_err = String::new();

    // -------------------------------------------------------------------------
    // ЦИКЛ ОТПРАВКИ НА АПСТРИМ С ОТСЛЕЖИВАНИЕМ КЭША
    // -------------------------------------------------------------------------
    let max_candidate_attempts = 2;
    let mut candidate_attempts = 0;
    for chosen_acc in &eligible {
        candidate_attempts += 1;
        if candidate_attempts > max_candidate_attempts {
            log::warn!("[FAILOVER:LIMIT] Reached max candidate attempts ({}), aborting failover loop early", max_candidate_attempts);
            break;
        }
        let mut token = chosen_acc.access_token.clone().unwrap_or_default();
        if token.is_empty() || chosen_acc.expires_at < now + 60.0 {
            match oauth::refresh_access_token(
                state.client.direct(),
                &chosen_acc.refresh_token,
                &cfg.oauth.client_id,
                &cfg.oauth.client_secret,
            )
            .await
            {
                Ok((new_token, exp)) => {
                    token = new_token.clone();
                    let _ = state.db.update_tokens(chosen_acc.id, &new_token, exp).await;
                }
                Err(e) => {
                    let _ = state.db.set_cooldown(chosen_acc.id, now + 300.0).await;
                    log::error!(
                        "[OAUTH:ERROR] Refresh failed on Acc #{}: {}",
                        chosen_acc.id,
                        e
                    );
                    continue;
                }
            }
        }

        if req.stream {
            log::info!(
                "[ROUTER:STREAM:PAYLOAD] {}",
                serde_json::to_string(&gemini_payload).unwrap_or_default()
            );
            match state
                .client
                .stream_generate_content(&token, &gemini_payload)
                .await
            {
                Ok(stream) => {
                    log::info!(
                        "[ROUTER:STREAM:OK] Stream started via Acc #{} ({})",
                        chosen_acc.id,
                        chosen_acc.email.as_deref().unwrap_or("anon")
                    );
                    let transformed_stream = sse::transform_gemini_sse_stream(
                        Box::pin(stream),
                        state.client.clone(),
                        token.clone(),
                        gemini_payload.clone(),
                        req.model.clone(),
                        state.signatures.clone(),
                        Some(state.telemetry.clone()),
                        start_time,
                        Some(chosen_acc.id),
                        chosen_acc.email.clone(),
                        Some(auth_key_name.clone()),
                        auth_key_id,
                        Some(state.db.clone()),
                    );

                    let lease = StreamLease::new(chosen_acc.id, state.active_streams.clone());
                    let guarded_stream = async_stream::stream! {
                        let _holder = lease;
                        tokio::pin!(transformed_stream);
                        while let Some(chunk) = transformed_stream.next().await {
                            yield chunk;
                        }
                    };

                    return Response::builder()
                        .header("Content-Type", "text/event-stream")
                        .header("Cache-Control", "no-cache")
                        .header("Connection", "keep-alive")
                        .body(Body::from_stream(guarded_stream))
                        .unwrap_or_else(|_| {
                            (StatusCode::INTERNAL_SERVER_ERROR, "Stream build error")
                                .into_response()
                        });
                }
                Err(e) => {
                    let err_str = e.to_string();
                    log::warn!("[STREAM:FAIL] Acc #{} failed stream: {}. Rotating to next HRW candidate...", chosen_acc.id, err_str);
                    last_account_err = format!("Acc #{} stream error: {}", chosen_acc.id, err_str);
                    if err_str.contains("No Active Proxies")
                        || err_str.contains("Bad Gateway")
                        || err_str.contains("Connection refused")
                        || err_str.contains("error sending request for url")
                        || err_str.contains("timed out")
                        || err_str.contains("timeout")
                        || (err_str.contains("503")
                            && (err_str.contains("Proxy") || err_str.contains("forwarder")))
                    {
                        proxy_transport_failed = true;
                        last_transport_err = err_str;
                        break;
                    }
                    if err_str.contains("429") || err_str.contains("RESOURCE_EXHAUSTED") {
                        if !err_str.contains("Proxy")
                            && !err_str.contains("forwarder")
                            && !err_str.contains("Connect")
                        {
                            let cooldown_sec = if err_str.contains("RESOURCE_EXHAUSTED") {
                                60.0
                            } else {
                                cfg.quota.rate_limit_cooldown_sec.min(30.0)
                            };
                            let _ = state
                                .db
                                .set_cooldown(chosen_acc.id, now + cooldown_sec)
                                .await;
                            log::warn!("[ROUTER:EXHAUSTION:FAILOVER] Stream Acc #{} exhausted ({:.0}s cooldown). Instant 0ms failover.", chosen_acc.id, cooldown_sec);
                        }
                    } else if err_str.contains("401")
                        || err_str.contains("403")
                        || err_str.contains("UNAUTHENTICATED")
                        || err_str.contains("PERMISSION_DENIED")
                    {
                        let _ = state.db.set_account_enabled(chosen_acc.id, false).await;
                        log::error!("[ACCOUNT:DISABLED] Acc #{} auto-disabled due to permanent auth error: {}", chosen_acc.id, err_str);
                    }
                    continue;
                }
            }
        } else {
            let _lease = StreamLease::new(chosen_acc.id, state.active_streams.clone());
            match state.client.generate_content(&token, &gemini_payload).await {
                Ok(gemini_resp) => {
                    match translator::parse_gemini_response(
                        &gemini_resp,
                        &req.model,
                        &state.signatures,
                    ) {
                        Ok(oai_resp) => {
                            let cached_tok = oai_resp
                                .usage
                                .prompt_tokens_details
                                .as_ref()
                                .map(|d| d.cached_tokens)
                                .unwrap_or(0);
                            let total_elapsed = start_time.elapsed().as_millis() as u64;
                            let gen_time_sec = (total_elapsed as f64) / 1000.0;
                            let is_image_model =
                                req.model.contains("banana") || req.model.contains("image");
                            let tok_speed = if is_image_model {
                                0.0
                            } else if gen_time_sec > 0.05 && oai_resp.usage.completion_tokens > 0 {
                                let speed =
                                    (oai_resp.usage.completion_tokens as f64 / gen_time_sec * 10.0)
                                        .round()
                                        / 10.0;
                                speed.min(500.0)
                            } else {
                                0.0
                            };

                            let (epoch, time_str) = telemetry::now_epoch_and_string();
                            let hit_type = if exact_hash.is_some() && cached_tok == 0 {
                                "miss".to_string()
                            } else if cached_tok > 0 {
                                "prefix".to_string()
                            } else {
                                "miss".to_string()
                            };

                            state.telemetry.record(telemetry::RequestMetric {
                                timestamp_epoch: epoch,
                                created_at: time_str,
                                model: req.model.clone(),
                                account_id: Some(chosen_acc.id),
                                account_email: chosen_acc.email.clone(),
                                proxy_used: None,
                                stream: false,
                                status_code: 200,
                                connect_latency_ms: total_elapsed.min(1500),
                                ttft_ms: total_elapsed,
                                thinking_time_ms: 0,
                                generation_time_ms: total_elapsed,
                                total_latency_ms: total_elapsed,
                                prompt_tokens: oai_resp.usage.prompt_tokens,
                                completion_tokens: oai_resp.usage.completion_tokens,
                                cached_tokens: cached_tok,
                                reasoning_tokens: 0,
                                total_tokens: oai_resp.usage.total_tokens,
                                tok_per_sec: tok_speed,
                                clean_tok_per_sec: tok_speed,
                                chunks_count: 1,
                                max_chunk_gap_ms: 0,
                                fast_gate_us: 0,
                                cache_hit_type: hit_type,
                                error: None,
                                key_name: Some(auth_key_name.clone()),
                            });

                            if let Some(kid) = auth_key_id {
                                let _ = state
                                    .db
                                    .record_key_usage(kid, oai_resp.usage.total_tokens as i64)
                                    .await;
                            }

                            if cached_tok > 0 {
                                log::info!("[CACHE:HIT:UPSTREAM] Google reused {} cached tokens! Latency: {}ms", cached_tok, start_time.elapsed().as_millis());
                            } else if debug {
                                log::info!("[CACHE:MISS:UPSTREAM] Full prefill. Prompt: {} tok, Compl: {} tok", oai_resp.usage.prompt_tokens, oai_resp.usage.completion_tokens);
                            }

                            return Json(oai_resp).into_response();
                        }
                        Err(e) => {
                            log::error!("[TRANSLATE:ERROR] Parse failed: {}", e);
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("Parse error: {}", e),
                            )
                                .into_response();
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    log::warn!(
                        "[UPSTREAM:FAIL] Acc #{} error: {}. Rolling over...",
                        chosen_acc.id,
                        err_str
                    );
                    last_account_err = format!("Acc #{} error: {}", chosen_acc.id, err_str);
                    if err_str.contains("No Active Proxies")
                        || err_str.contains("Bad Gateway")
                        || err_str.contains("Connection refused")
                        || err_str.contains("error sending request for url")
                        || err_str.contains("timed out")
                        || err_str.contains("timeout")
                        || (err_str.contains("503")
                            && (err_str.contains("Proxy") || err_str.contains("forwarder")))
                    {
                        proxy_transport_failed = true;
                        last_transport_err = err_str;
                        break;
                    }
                    if err_str.contains("429") || err_str.contains("RESOURCE_EXHAUSTED") {
                        if !err_str.contains("Proxy")
                            && !err_str.contains("forwarder")
                            && !err_str.contains("Connect")
                        {
                            let cooldown_sec = if err_str.contains("RESOURCE_EXHAUSTED") {
                                60.0
                            } else {
                                cfg.quota.rate_limit_cooldown_sec.min(30.0)
                            };
                            let _ = state
                                .db
                                .set_cooldown(chosen_acc.id, now + cooldown_sec)
                                .await;
                            log::warn!("[ROUTER:EXHAUSTION:FAILOVER] Sync Acc #{} exhausted ({:.0}s cooldown). Instant 0ms failover.", chosen_acc.id, cooldown_sec);
                        }
                    } else if err_str.contains("401")
                        || err_str.contains("403")
                        || err_str.contains("UNAUTHENTICATED")
                        || err_str.contains("PERMISSION_DENIED")
                    {
                        // Сбрасываем access_token и ставим временный кулдаун 60с для обновления токена вместо перманентного бана
                        let _ = state.db.update_tokens(chosen_acc.id, "", 0.0).await;
                        let _ = state.db.set_cooldown(chosen_acc.id, now + 60.0).await;
                        log::warn!("[ACCOUNT:AUTH_REFRESH] Acc #{} auth token reset + 60s cooldown due to auth error: {}", chosen_acc.id, err_str);
                    }
                }
            }
        }
    }

    let total_elapsed = start_time.elapsed().as_millis() as u64;
    let (epoch, time_str) = telemetry::now_epoch_and_string();

    if proxy_transport_failed {
        log::error!(
            "[ROUTER:PROXY_FAIL] Upstream proxy forwarder failed: {}. Accounts are NOT exhausted!",
            last_transport_err
        );
        state.telemetry.record(telemetry::RequestMetric {
            timestamp_epoch: epoch,
            created_at: time_str,
            model: req.model.clone(),
            account_id: None,
            account_email: None,
            proxy_used: None,
            stream: req.stream,
            status_code: 503,
            connect_latency_ms: total_elapsed,
            ttft_ms: 0,
            thinking_time_ms: 0,
            generation_time_ms: 0,
            total_latency_ms: total_elapsed,
            prompt_tokens: 0,
            completion_tokens: 0,
            cached_tokens: 0,
            reasoning_tokens: 0,
            total_tokens: 0,
            tok_per_sec: 0.0,
            clean_tok_per_sec: 0.0,
            chunks_count: 0,
            max_chunk_gap_ms: 0,
            fast_gate_us: 0,
            cache_hit_type: "miss".to_string(),
            error: Some(format!("Proxy forwarder failure: {}", last_transport_err)),
            key_name: Some(auth_key_name.clone()),
        });

        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": {
                    "message": format!("Upstream proxy forwarder failed or no active proxies in pool: {}", last_transport_err),
                    "type": "proxy_unavailable",
                }
            })),
        ).into_response();
    }

    log::error!(
        "[REPRO:DUMP] All eligible accounts exhausted! Model: {}, AffinityKey: '{}', LastErr: {}",
        req.model,
        &affinity_key[..12],
        last_account_err
    );
    state.telemetry.record(telemetry::RequestMetric {
        timestamp_epoch: epoch,
        created_at: time_str,
        model: req.model.clone(),
        account_id: None,
        account_email: None,
        proxy_used: None,
        stream: req.stream,
        status_code: 429,
        connect_latency_ms: total_elapsed,
        ttft_ms: 0,
        thinking_time_ms: 0,
        generation_time_ms: 0,
        total_latency_ms: total_elapsed,
        prompt_tokens: 0,
        completion_tokens: 0,
        cached_tokens: 0,
        reasoning_tokens: 0,
        total_tokens: 0,
        tok_per_sec: 0.0,
        clean_tok_per_sec: 0.0,
        chunks_count: 0,
        max_chunk_gap_ms: 0,
        fast_gate_us: 0,
        cache_hit_type: "exhausted".to_string(),
        error: Some(format!("All accounts failed: {}", last_account_err)),
        key_name: Some(auth_key_name.clone()),
    });

    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::json!({
            "error": {
                "message": format!("All eligible accounts failed or exhausted. Last error: {}", last_account_err),
                "type": "upstream_error",
            }
        })),
    ).into_response()
}

#[derive(Debug)]
pub(crate) enum AuthResult {
    Allowed {
        key_name: String,
        key_id: Option<i64>,
    },
    Unauthorized(String),
    Forbidden(String),
    RateLimited(String),
}

pub(crate) async fn authenticate_request(headers: &HeaderMap, state: &AppState) -> AuthResult {
    let total_keys = state.db.count_keys().await.unwrap_or(0);
    if total_keys == 0 {
        // Ключи в базе не настроены: разрешаем запросы по умолчанию
        return AuthResult::Allowed {
            key_name: "dummy".to_string(),
            key_id: None,
        };
    }

    let token_opt = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|auth| auth.strip_prefix("Bearer "))
        .map(|s| s.trim())
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim())
        });

    let token = match token_opt {
        Some(t) if !t.is_empty() => t,
        _ => {
            return AuthResult::Unauthorized(
                "Missing API key in Authorization header. Keys are configured in Hydra."
                    .to_string(),
            )
        }
    };

    // Если токен совпадает с мастер-ключом из config.toml (например, "kiko")
    if token == state.config.read().server.api_key {
        return AuthResult::Allowed {
            key_name: "master_key".to_string(),
            key_id: None,
        };
    }

    match state.db.get_key_by_token(token).await {
        Ok(Some(key)) => {
            if !key.enabled {
                return AuthResult::Forbidden(format!("API key '{}' is disabled", key.name));
            }
            let now_f64 = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();
            if let Some(exp) = key.expires_at {
                if now_f64 > exp {
                    return AuthResult::Forbidden(format!("Срок действия API ключа '{}' истек", key.name));
                }
            }
            if key.limit_type == "requests" {
                if key.request_limit > 0 && key.requests_count >= key.request_limit {
                    return AuthResult::RateLimited(format!(
                        "Лимит запросов исчерпан для ключа '{}' ({} / {} запросов)",
                        key.name, key.requests_count, key.request_limit
                    ));
                }
            } else {
                if key.token_limit > 0 && key.tokens_used >= key.token_limit {
                    return AuthResult::RateLimited(format!(
                        "Лимит токенов исчерпан для ключа '{}' ({} / {} токенов)",
                        key.name, key.tokens_used, key.token_limit
                    ));
                }
            }
            if key.rpm_limit > 0 && !state.rpm_tracker.check_and_record(key.id, key.rpm_limit) {
                return AuthResult::RateLimited(format!(
                    "RPM лимит превышен для ключа '{}' (limit: {} req/min)",
                    key.name, key.rpm_limit
                ));
            }
            AuthResult::Allowed {
                key_name: key.name,
                key_id: Some(key.id),
            }
        }
        Ok(None) => AuthResult::Unauthorized("Invalid API key".to_string()),
        Err(e) => AuthResult::Unauthorized(format!("Database auth error: {}", e)),
    }
}
