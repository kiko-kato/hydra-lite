use crate::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize)]
pub struct UpdateModelConfigRequest {
    pub alias: Option<String>,
    pub enabled: Option<bool>,
}

pub(crate) async fn handle_get_model_configs(State(state): State<AppState>) -> impl IntoResponse {
    let saved_configs = state.db.get_all_model_configs().await.unwrap_or_default();
    let discovered = state.model_manager.get_model_items().await;

    let mut list = Vec::new();
    let mut seen = std::collections::HashSet::new();

    // 1. Модели из активного каталога Upstream
    for item in discovered {
        if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
            if seen.insert(id.to_string()) {
                let (custom_alias, enabled) = saved_configs.get(id).cloned().unwrap_or((None, true));
                list.push(serde_json::json!({
                    "original_id": id,
                    "alias": custom_alias,
                    "enabled": enabled,
                }));
            }
        }
    }

    // 2. Дополнительные сохраненные в базе модели
    for (orig, (alias, enabled)) in saved_configs {
        if seen.insert(orig.clone()) {
            list.push(serde_json::json!({
                "original_id": orig,
                "alias": alias,
                "enabled": enabled,
            }));
        }
    }

    (StatusCode::OK, Json(serde_json::json!({ "models": list })))
}

pub(crate) async fn handle_update_model_config(
    Path(original_id): Path<String>,
    State(state): State<AppState>,
    Json(req): Json<UpdateModelConfigRequest>,
) -> impl IntoResponse {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let current_configs = state.db.get_all_model_configs().await.unwrap_or_default();
    let (cur_alias, cur_enabled) = current_configs
        .get(&original_id)
        .cloned()
        .unwrap_or((None, true));

    let final_alias = match req.alias {
        Some(a) => {
            let trimmed = a.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        None => cur_alias,
    };
    let final_enabled = req.enabled.unwrap_or(cur_enabled);

    match state
        .db
        .set_model_config(&original_id, final_alias.as_deref(), final_enabled, now)
        .await
    {
        Ok(_) => {
            // Мгновенная синхронизация кэша в ModelManager
            let _ = state.model_manager.sync_models().await;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "original_id": original_id,
                    "alias": final_alias,
                    "enabled": final_enabled,
                    "status": "ok"
                })),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}
