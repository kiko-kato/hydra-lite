use crate::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
    pub key: Option<String>,
    pub rpm_limit: Option<i64>,
    pub token_limit: Option<i64>,
    pub is_admin: Option<bool>,
}

#[derive(Deserialize)]
pub struct UpdateKeyRequest {
    pub name: Option<String>,
    pub key: Option<String>,
    pub rpm_limit: Option<i64>,
    pub token_limit: Option<i64>,
    pub enabled: Option<bool>,
    pub is_admin: Option<bool>,
}

pub(crate) async fn handle_get_keys(State(state): State<AppState>) -> impl IntoResponse {
    match state.db.list_keys().await {
        Ok(keys) => (StatusCode::OK, Json(serde_json::json!({ "keys": keys }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

pub(crate) async fn handle_create_key(
    State(state): State<AppState>,
    Json(payload): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    let name = payload.name.trim();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Name cannot be empty" })),
        );
    }

    let token = payload.key.unwrap_or_default().trim().to_string();
    let final_key = if token.is_empty() {
        format!("hydra-{}", uuid::Uuid::new_v4().simple())
    } else {
        token
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    let rpm = payload.rpm_limit.unwrap_or(0).max(0);
    let tok_lim = payload.token_limit.unwrap_or(0).max(0);
    let is_admin = payload.is_admin.unwrap_or(false);

    match state
        .db
        .create_key(name, &final_key, rpm, tok_lim, is_admin, now)
        .await
    {
        Ok(created) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "status": "created", "key": created })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("Failed to create key: {}", e) })),
        ),
    }
}

pub(crate) async fn handle_update_key(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<UpdateKeyRequest>,
) -> impl IntoResponse {
    let name_opt = payload.name.as_deref().map(|s| s.trim());
    let key_opt = payload.key.as_deref().map(|s| s.trim());
    match state
        .db
        .update_key(
            id,
            name_opt,
            key_opt,
            payload.rpm_limit,
            payload.token_limit,
            payload.enabled,
            payload.is_admin,
        )
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "updated", "id": id })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

pub(crate) async fn handle_delete_key(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    match state.db.delete_key(id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "deleted", "id": id })),
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
