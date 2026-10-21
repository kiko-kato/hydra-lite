use crate::AppState;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
    pub key: Option<String>,
    pub rpm_limit: Option<i64>,
    pub token_limit: Option<i64>,
    pub request_limit: Option<i64>,
    pub limit_type: Option<String>,
    pub is_admin: Option<bool>,
    pub expires_at: Option<f64>,
}

#[derive(Deserialize)]
pub struct UpdateKeyRequest {
    pub name: Option<String>,
    pub key: Option<String>,
    pub rpm_limit: Option<i64>,
    pub token_limit: Option<i64>,
    pub request_limit: Option<i64>,
    pub limit_type: Option<String>,
    pub enabled: Option<bool>,
    pub is_admin: Option<bool>,
    pub expires_at: Option<Option<f64>>,
}

#[derive(Serialize)]
pub struct KeyDto {
    pub id: i64,
    pub name: String,
    pub key: String,
    pub rpm_limit: i64,
    pub token_limit: i64,
    pub request_limit: i64,
    pub limit_type: String,
    pub tokens_used: i64,
    pub requests_count: i64,
    pub enabled: bool,
    pub created_at: f64,
    pub is_admin: bool,
    pub expires_at: Option<f64>,
    pub created_by: Option<i64>,
    pub can_edit: bool,
    pub is_self: bool,
}

enum Caller {
    Master,
    Key(crate::db::ApiKey),
}

async fn extract_caller(headers: &HeaderMap, state: &AppState) -> Option<Caller> {
    let mut tok_str = None;
    if let Some(auth) = headers.get("authorization").and_then(|h| h.to_str().ok()) {
        if let Some(bearer) = auth.strip_prefix("Bearer ") {
            tok_str = Some(bearer.trim().to_string());
        }
    }
    if tok_str.is_none() {
        if let Some(cookie_hdr) = headers.get("cookie").and_then(|h| h.to_str().ok()) {
            for part in cookie_hdr.split(';') {
                let trimmed = part.trim();
                if let Some(val) = trimmed.strip_prefix("hydra_admin_auth=") {
                    tok_str = Some(val.trim().to_string());
                    break;
                }
            }
        }
    }

    let tok = tok_str?;
    if tok == state.config.read().server.api_key {
        return Some(Caller::Master);
    }

    if let Ok(Some(k)) = state.db.get_key_by_token(&tok).await {
        if k.enabled && k.is_admin {
            return Some(Caller::Key(k));
        }
    }
    None
}

pub(crate) async fn handle_get_keys(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let caller = match extract_caller(&headers, &state).await {
        Some(c) => c,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Unauthorized" })),
            );
        }
    };

    match state.db.list_keys().await {
        Ok(keys) => {
            let mut dtos = Vec::with_capacity(keys.len());
            for k in keys {
                let (can_edit, is_self) = match &caller {
                    Caller::Master => (true, false),
                    Caller::Key(my_key) => {
                        if k.id == my_key.id {
                            // Сам себе — запрет редактирования!
                            (false, true)
                        } else if my_key.id == 1 {
                            // Root админ (Admin (Owner) ID=1) управляет всеми ниже него
                            (true, false)
                        } else {
                            // Обычный админ управляет только своим поддеревом
                            let in_tree = state
                                .db
                                .is_in_creator_tree(k.id, my_key.id)
                                .await
                                .unwrap_or(false);
                            (in_tree, false)
                        }
                    }
                };

                dtos.push(KeyDto {
                    id: k.id,
                    name: k.name,
                    key: k.key,
                    rpm_limit: k.rpm_limit,
                    token_limit: k.token_limit,
                    request_limit: k.request_limit,
                    limit_type: k.limit_type,
                    tokens_used: k.tokens_used,
                    requests_count: k.requests_count,
                    enabled: k.enabled,
                    created_at: k.created_at,
                    is_admin: k.is_admin,
                    expires_at: k.expires_at,
                    created_by: k.created_by,
                    can_edit,
                    is_self,
                });
            }
            (StatusCode::OK, Json(serde_json::json!({ "keys": dtos })))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

pub(crate) async fn handle_create_key(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(payload): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    let caller = match extract_caller(&headers, &state).await {
        Some(c) => c,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Unauthorized" })),
            );
        }
    };

    let name = payload.name.trim();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Имя ключа не может быть пустым" })),
        );
    }

    let is_admin = payload.is_admin.unwrap_or(false);
    let token = payload.key.unwrap_or_default().trim().to_string();
    let final_key = if token.is_empty() {
        let prefix = if is_admin { "hydra-adm" } else { "hydra-usr" };
        format!("{}-{}", prefix, uuid::Uuid::new_v4().simple())
    } else {
        token
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    let created_by = match caller {
        Caller::Master => Some(1),
        Caller::Key(ref k) => Some(k.id),
    };

    let limit_type = payload.limit_type.unwrap_or_else(|| "tokens".to_string());

    match state
        .db
        .create_key(
            name,
            &final_key,
            payload.rpm_limit.unwrap_or(0),
            payload.token_limit.unwrap_or(0),
            payload.request_limit.unwrap_or(0),
            &limit_type,
            is_admin,
            payload.expires_at,
            created_by,
            now,
        )
        .await
    {
        Ok(k) => (StatusCode::CREATED, Json(serde_json::json!({ "key": k }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

pub(crate) async fn handle_update_key(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<UpdateKeyRequest>,
) -> impl IntoResponse {
    let caller = match extract_caller(&headers, &state).await {
        Some(c) => c,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Unauthorized" })),
            );
        }
    };

    // Защита 1: Админ не может редактировать свой собственный ключ!
    if let Caller::Key(ref my_key) = caller {
        if my_key.id == id {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Запрещено редактировать собственный ключ во избежание случайной самоблокировки"
                })),
            );
        }

        // Защита 2: Дерево создателей — можно редактировать только свое поддерево!
        if my_key.id != 1 {
            let in_tree = state
                .db
                .is_in_creator_tree(id, my_key.id)
                .await
                .unwrap_or(false);
            if !in_tree {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({
                        "error": "Доступ запрещен: вы можете редактировать только ключи из своего дерева создателей"
                    })),
                );
            }
        }
    }

    match state
        .db
        .update_key(
            id,
            payload.name.as_deref(),
            payload.key.as_deref(),
            payload.rpm_limit,
            payload.token_limit,
            payload.request_limit,
            payload.limit_type.as_deref(),
            payload.enabled,
            payload.is_admin,
            payload.expires_at,
        )
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "updated" })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

pub(crate) async fn handle_delete_key(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let caller = match extract_caller(&headers, &state).await {
        Some(c) => c,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Unauthorized" })),
            );
        }
    };

    // Защита 1: Нельзя удалить себя
    if let Caller::Key(ref my_key) = caller {
        if my_key.id == id {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "Запрещено удалять собственный ключ" })),
            );
        }

        // Защита 2: Дерево создателей
        if my_key.id != 1 {
            let in_tree = state
                .db
                .is_in_creator_tree(id, my_key.id)
                .await
                .unwrap_or(false);
            if !in_tree {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({
                        "error": "Доступ запрещен: вы можете удалять только ключи из своего дерева создателей"
                    })),
                );
            }
        }
    }

    match state.db.delete_key(id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "deleted" })),
        ),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Key not found" })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}
