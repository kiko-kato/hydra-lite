use crate::client::UpstreamClient;
use crate::db::Database;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub const STANDARD_MODELS: &[&str] = &[
    "gemini-3.8",
    "gemini-3.8-flash",
    "gemini-3.8-flash-high",
    "gemini-3.8-flash-medium",
    "gemini-3.8-flash-low",
    "gemini-3.8-flash-tiered",
    "nano-banana",
    "nano-banana-pro",
    "nano-banana-2",
    "gemini-3.1-flash-image",
    "claude-opus-5-5",
    "claude-opus-5-5-high",
    "claude-opus-5-5-medium",
    "claude-opus-5-5-low",
    "claude-sonnet-5-5",
    "claude-sonnet-5-5-high",
    "claude-sonnet-5-5-medium",
    "claude-sonnet-5-5-low",
    "gemini-4-argon",
    "gemini-4-argon-high",
    "gemini-4-argon-medium",
    "gemini-4-argon-low",
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-2.5-flash-thinking",
    "gemini-2.5-flash-lite",
    "gemini-3.5-flash-lite",
    "gemini-3-flash",
    "gemini-3.1-flash-lite",
    "gpt-oss-120b-medium",
    "tab_flash_lite_preview",
    "tab_jump_flash_lite_preview",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredModel {
    pub original_id: String,
    pub display_name: Option<String>,
    pub primary_alias: Option<String>,
    pub short_aliases: Vec<String>,
    pub enabled: bool,
}

pub struct ModelManager {
    db: Arc<Database>,
    client: Arc<UpstreamClient>,
    catalog: RwLock<HashMap<String, DiscoveredModel>>,
    alias_map: RwLock<HashMap<String, String>>,
    default_model: String,
}

impl ModelManager {
    pub fn new(db: Arc<Database>, client: Arc<UpstreamClient>, default_model: String) -> Self {
        Self {
            db,
            client,
            catalog: RwLock::new(HashMap::new()),
            alias_map: RwLock::new(HashMap::new()),
            default_model,
        }
    }

    /// Правила генерации алиасов:
    /// 1. Нормализация DisplayName в slug (e.g. "Gemini 3.8 Flash (High)" -> "gemini-3.8-flash-high")
    /// 2. Генерация сокращенных и удобных псевдонимов
    /// 3. Главное правило: если генерация не удалась — возвращаем голое название
    pub fn generate_aliases(key: &str, display_name: Option<&str>) -> (Option<String>, Vec<String>) {
        let mut short_aliases = Vec::new();

        let generated = if let Some(dn) = display_name {
            let trimmed = dn.trim();
            if !trimmed.is_empty() {
                let mut slug = trimmed
                    .to_lowercase()
                    .replace(['(', ')', '[', ']', '"', '\'', ':'], "")
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join("-");

                while slug.contains("--") {
                    slug = slug.replace("--", "-");
                }

                if !slug.is_empty() && slug != key {
                    Some(slug)
                } else {
                    Some(key.to_string())
                }
            } else {
                None
            }
        } else {
            None
        };

        // Если генерация не удалась по правилам — выводим голое название
        let mut primary_alias = generated.or_else(|| Some(key.to_string()));
        let base = primary_alias.as_deref().unwrap_or(key);

        if base.contains("3.8-flash-high") || key.contains("3.8-flash-high") {
            primary_alias = Some("gemini-3.8".to_string());
            short_aliases.push("gemini-3.8-flash-high".to_string());
            short_aliases.push("gemini-3.8-flash".to_string());
            short_aliases.push("flash".to_string());
            short_aliases.push("flash-high".to_string());
        } else if base.contains("3.1-flash-image") || key.contains("3.1-flash-image") || base.contains("nano-banana") {
            primary_alias = Some("nano-banana".to_string());
            short_aliases.push("gemini-3.1-flash-image".to_string());
            short_aliases.push("nano-banana-2".to_string());
            short_aliases.push("nano-banana-pro".to_string());
        } else if base.contains("3.8-flash-medium") || key.contains("3.8-flash-medium") {
            short_aliases.push("flash-medium".to_string());
        } else if base.contains("3.8-flash-low") || key.contains("3.8-flash-low") {
            short_aliases.push("flash-low".to_string());
        } else if base.contains("2.5-pro") || key.contains("2.5-pro") {
            short_aliases.push("gemini-pro".to_string());
            short_aliases.push("pro".to_string());
        } else if base.contains("2.5-flash") || key.contains("2.5-flash") {
            short_aliases.push("flash".to_string());
        } else if base.contains("gpt-oss-120b") || key.contains("gpt-oss-120b") {
            short_aliases.push("gpt-oss".to_string());
            short_aliases.push("gpt-oss-120b".to_string());
        } else if base.contains("opus") {
            short_aliases.push("claude-opus".to_string());
            short_aliases.push("opus".to_string());
        } else if base.contains("sonnet") {
            short_aliases.push("claude-sonnet".to_string());
            short_aliases.push("sonnet".to_string());
        }

        (primary_alias, short_aliases)
    }

    /// Адаптивная синхронизация моделей с Google Upstream
    pub async fn sync_models(&self) -> anyhow::Result<usize> {
        let accounts = self.db.load_accounts().await?;
        let active_acc = accounts
            .into_iter()
            .find(|a| a.enabled && a.access_token.is_some())
            .ok_or_else(|| anyhow::anyhow!("No active accounts with access_token available"))?;

        let token = active_acc.access_token.unwrap();
        let raw_models = self.client.fetch_raw_models(&token).await?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let saved_configs = self.db.get_all_model_configs().await.unwrap_or_default();

        let mut new_catalog = HashMap::new();
        let mut new_alias_map = HashMap::new();

        for m in &raw_models {
            let (prim_alias, shorts) = Self::generate_aliases(&m.key, m.display_name.as_deref());

            let (custom_alias, is_enabled) = if let Some((db_alias, db_enabled)) = saved_configs.get(&m.key) {
                // Если в Google модель явно disabled, форсим отключение
                let final_enabled = if m.disabled { false } else { *db_enabled };
                (db_alias.clone(), final_enabled)
            } else {
                let init_enabled = !m.disabled;
                let alias_to_save = prim_alias.clone();
                let _ = self
                    .db
                    .set_model_config(&m.key, alias_to_save.as_deref(), init_enabled, now)
                    .await;
                (alias_to_save, init_enabled)
            };

            // Маппинг для резолвера
            new_alias_map.insert(m.key.clone(), m.key.clone());
            if let Some(ref pa) = prim_alias {
                new_alias_map.insert(pa.clone(), m.key.clone());
            }
            if let Some(ref ca) = custom_alias {
                new_alias_map.insert(ca.clone(), m.key.clone());
            }
            for s in &shorts {
                new_alias_map.insert(s.clone(), m.key.clone());
            }

            new_catalog.insert(
                m.key.clone(),
                DiscoveredModel {
                    original_id: m.key.clone(),
                    display_name: m.display_name.clone(),
                    primary_alias: custom_alias.or(prim_alias),
                    short_aliases: shorts,
                    enabled: is_enabled,
                },
            );
        }

        let count = new_catalog.len();
        *self.catalog.write().await = new_catalog;
        *self.alias_map.write().await = new_alias_map;

        info!("[MODELS:SYNC] Synchronized {} upstream models with auto-generated aliases", count);
        Ok(count)
    }

    /// Разрешение модели: псевдоним -> оригинальный ID upstream
    pub async fn resolve(&self, requested: &str) -> String {
        let req_trim = requested.trim();
        if req_trim.is_empty() {
            return self.default_model.clone();
        }

        // 1. Быстрый каноничный маппинг разновидностей
        match req_trim {
            // Nano Banana
            "nano-banana" | "nano-banana-pro" | "nano-banana-2" | "gemini-3.1-flash-image" => {
                return "gemini-3.1-flash-image".to_string();
            }
            // Gemini 3.8 Flash
            "gemini-3.8" | "gemini-3.8-flash" | "gemini-3.8-flash-tiered" => {
                return "gemini-3.8-flash-tiered".to_string();
            }
            "gemini-3.8-flash-high" => return "gemini-3.8-flash-high".to_string(),
            "gemini-3.8-flash-medium" => return "gemini-3.8-flash-medium".to_string(),
            "gemini-3.8-flash-low" => return "gemini-3.8-flash-low".to_string(),

            // Claude 5.5
            "claude-opus-5-5-high" => return "claude-opus-5-5-high".to_string(),
            "claude-opus-5-5-medium" => return "claude-opus-5-5-medium".to_string(),
            "claude-opus-5-5-low" => return "claude-opus-5-5-low".to_string(),
            "claude-opus-5-5" | "claude-opus" | "opus" => return "claude-opus-5-5-high".to_string(),

            "claude-sonnet-5-5-high" => return "claude-sonnet-5-5-high".to_string(),
            "claude-sonnet-5-5-medium" => return "claude-sonnet-5-5-medium".to_string(),
            "claude-sonnet-5-5-low" => return "claude-sonnet-5-5-low".to_string(),
            "claude-sonnet-5-5" | "claude-sonnet" | "sonnet" => return "claude-sonnet-5-5-high".to_string(),

            // Gemini 4 Argon
            "gemini-4-argon-high" => return "gemini-4-argon-high".to_string(),
            "gemini-4-argon-medium" => return "gemini-4-argon-medium".to_string(),
            "gemini-4-argon-low" => return "gemini-4-argon-low".to_string(),
            "gemini-4-argon" | "argon" => return "gemini-4-argon-high".to_string(),

            _ => {}
        }

        // 2. Поиск по in-memory алиасам
        {
            let map = self.alias_map.read().await;
            if let Some(target) = map.get(req_trim) {
                return target.clone();
            }
        }

        // 3. Поиск в SQLite (на случай ручного апдейта через API)
        if let Ok(Some(db_target)) = self.db.resolve_model_alias(req_trim).await {
            return db_target;
        }

        // 4. Главное правило: если не найдено — отдаем голое название
        req_trim.to_string()
    }

    /// Список всех доступных моделей для /v1/models
    pub async fn get_model_items(&self) -> Vec<serde_json::Value> {
        let saved_configs = self.db.get_all_model_configs().await.unwrap_or_default();
        let catalog = self.catalog.read().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut list = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // 1. Стандартный каталог моделей (как в гидре на ноуте)
        for orig in STANDARD_MODELS {
            let (custom_alias, enabled) = saved_configs.get(*orig).cloned().unwrap_or((None, true));
            if !enabled {
                continue;
            }
            let public_id = custom_alias.unwrap_or_else(|| orig.to_string());
            if seen.insert(public_id.clone()) {
                list.push(serde_json::json!({
                    "id": public_id,
                    "object": "model",
                    "created": now,
                    "owned_by": "google",
                }));
            }
        }

        // 2. Дополнительные динамически обнаруженные модели Upstream
        for m in catalog.values() {
            if !m.enabled {
                continue;
            }
            let public_id = m.primary_alias.as_deref().unwrap_or(&m.original_id);
            if seen.insert(public_id.to_string()) {
                list.push(serde_json::json!({
                    "id": public_id,
                    "object": "model",
                    "created": now,
                    "owned_by": "google",
                }));
            }
        }

        list
    }

    /// Фоновый воркер синхронизации
    pub fn start_worker(self: Arc<Self>, interval: Duration) {
        tokio::spawn(async move {
            info!("[MODELS:WORKER] Background adaptive model updater started");
            // Первичный прогрев с небольшой задержкой для завершения инициализации OAuth
            tokio::time::sleep(Duration::from_secs(3)).await;
            if let Err(e) = self.sync_models().await {
                warn!("[MODELS:WORKER] Initial sync failed: {}", e);
            }

            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = self.sync_models().await {
                    warn!("[MODELS:WORKER] Periodic model sync failed: {}", e);
                }
            }
        });
    }
}
