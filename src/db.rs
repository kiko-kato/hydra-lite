// Пул базы данных SQLite с разделением на чтение и запись (WAL-режим)
// Без exact-кэширования ответов — только честный префикс-кэшинг провайдера

use crate::types::Account;
use anyhow::Result;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Database {
    writer: Arc<Mutex<Connection>>,
    readers: Vec<Arc<parking_lot::Mutex<Connection>>>,
    reader_idx: Arc<AtomicUsize>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        // 1. Соединение для записи и миграция схемы
        let writer = Connection::open(path_ref)?;
        writer.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 30000;

            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT,
                refresh_token TEXT NOT NULL UNIQUE,
                access_token TEXT,
                expires_at REAL DEFAULT 0,
                quota_json TEXT,
                gemini_remaining REAL DEFAULT 1.0,
                claude_remaining REAL DEFAULT 1.0,
                last_quota_check REAL DEFAULT 0,
                cooldown_until REAL DEFAULT 0,
                enabled INTEGER DEFAULT 1
            );

            CREATE INDEX IF NOT EXISTS idx_accounts_active 
            ON accounts(enabled, cooldown_until, gemini_remaining DESC);

            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                key TEXT NOT NULL UNIQUE,
                rpm_limit INTEGER DEFAULT 0,
                token_limit INTEGER DEFAULT 0,
                tokens_used INTEGER DEFAULT 0,
                requests_count INTEGER DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                created_at REAL NOT NULL,
                is_admin INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_api_keys_lookup ON api_keys(key, enabled);

            CREATE TABLE IF NOT EXISTS model_configs (
                original_id TEXT PRIMARY KEY,
                alias TEXT,
                enabled INTEGER DEFAULT 1,
                updated_at REAL NOT NULL
            );
            "#,
        )?;

        // Автомиграция для старых БД (добавление колонки is_admin)
        let _ = writer.execute("ALTER TABLE api_keys ADD COLUMN is_admin INTEGER DEFAULT 0;", []);

        // 2. Пул параллельных читателей (4 подключения)
        let num_readers = 4;
        let mut readers = Vec::with_capacity(num_readers);
        for _ in 0..num_readers {
            let r_conn = Connection::open(path_ref)?;
            r_conn.execute_batch(
                r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA busy_timeout = 30000;
                PRAGMA query_only = ON;
                "#,
            )?;
            readers.push(Arc::new(parking_lot::Mutex::new(r_conn)));
        }

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers,
            reader_idx: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn reader(&self) -> Arc<parking_lot::Mutex<Connection>> {
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        self.readers[idx].clone()
    }

    // ==========================================
    // ЧТЕНИЕ (ПАРАЛЛЕЛЬНО ЧЕРЕЗ READ-ПУЛ БЕЗ БЛОКИРОВКИ TOKIO)
    // ==========================================

    pub async fn load_accounts(&self) -> Result<Vec<Account>> {
        let r = self.reader();
        tokio::task::spawn_blocking(move || {
            let conn = r.lock();
            let mut stmt = conn.prepare(
                r#"
                SELECT id, email, refresh_token, access_token, expires_at, quota_json,
                       gemini_remaining, claude_remaining, last_quota_check, cooldown_until, enabled
                FROM accounts
                WHERE enabled = 1
                ORDER BY id ASC
                "#,
            )?;

            let rows = stmt.query_map([], |row| {
                let enabled_int: i64 = row.get(10)?;
                Ok(Account {
                    id: row.get(0)?,
                    email: row.get(1)?,
                    refresh_token: row.get(2)?,
                    access_token: row.get(3)?,
                    expires_at: row.get(4)?,
                    quota_json: row.get(5)?,
                    gemini_remaining: row.get(6)?,
                    claude_remaining: row.get(7)?,
                    last_quota_check: row.get(8)?,
                    cooldown_until: row.get(9)?,
                    enabled: enabled_int == 1,
                })
            })?;

            let mut list = Vec::new();
            for res in rows {
                list.push(res?);
            }
            Ok(list)
        })
        .await?
    }

    pub async fn load_all_accounts(&self) -> Result<Vec<Account>> {
        let r = self.reader();
        tokio::task::spawn_blocking(move || {
            let conn = r.lock();
            let mut stmt = conn.prepare(
                r#"
                SELECT id, email, refresh_token, access_token, expires_at, quota_json,
                       gemini_remaining, claude_remaining, last_quota_check, cooldown_until, enabled
                FROM accounts
                ORDER BY id ASC
                "#,
            )?;

            let rows = stmt.query_map([], |row| {
                let enabled_int: i64 = row.get(10)?;
                Ok(Account {
                    id: row.get(0)?,
                    email: row.get(1)?,
                    refresh_token: row.get(2)?,
                    access_token: row.get(3)?,
                    expires_at: row.get(4)?,
                    quota_json: row.get(5)?,
                    gemini_remaining: row.get(6)?,
                    claude_remaining: row.get(7)?,
                    last_quota_check: row.get(8)?,
                    cooldown_until: row.get(9)?,
                    enabled: enabled_int == 1,
                })
            })?;

            let mut list = Vec::new();
            for res in rows {
                list.push(res?);
            }
            Ok(list)
        })
        .await?
    }

    pub async fn list_keys(&self) -> Result<Vec<ApiKey>> {
        let r = self.reader();
        tokio::task::spawn_blocking(move || {
            let conn = r.lock();
            let mut stmt = conn.prepare(
                r#"
                SELECT id, name, key, rpm_limit, token_limit, tokens_used, requests_count, enabled, created_at, COALESCE(is_admin, 0)
                FROM api_keys
                ORDER BY id ASC
                "#,
            )?;
            let rows = stmt.query_map([], |row| {
                let enabled_int: i64 = row.get(7)?;
                let is_adm_int: i64 = row.get(9)?;
                Ok(ApiKey {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    key: row.get(2)?,
                    rpm_limit: row.get(3)?,
                    token_limit: row.get(4)?,
                    tokens_used: row.get(5)?,
                    requests_count: row.get(6)?,
                    enabled: enabled_int == 1,
                    created_at: row.get(8)?,
                    is_admin: is_adm_int == 1,
                })
            })?;
            let mut list = Vec::new();
            for res in rows {
                list.push(res?);
            }
            Ok(list)
        })
        .await?
    }

    pub async fn get_key_by_token(&self, token: &str) -> Result<Option<ApiKey>> {
        let r = self.reader();
        let token_owned = token.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = r.lock();
            let mut stmt = conn.prepare(
                r#"
                SELECT id, name, key, rpm_limit, token_limit, tokens_used, requests_count, enabled, created_at, COALESCE(is_admin, 0)
                FROM api_keys
                WHERE key = ?1
                LIMIT 1
                "#,
            )?;
            let mut rows = stmt.query_map(params![token_owned], |row| {
                let enabled_int: i64 = row.get(7)?;
                let is_adm_int: i64 = row.get(9)?;
                Ok(ApiKey {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    key: row.get(2)?,
                    rpm_limit: row.get(3)?,
                    token_limit: row.get(4)?,
                    tokens_used: row.get(5)?,
                    requests_count: row.get(6)?,
                    enabled: enabled_int == 1,
                    created_at: row.get(8)?,
                    is_admin: is_adm_int == 1,
                })
            })?;
            if let Some(res) = rows.next() {
                Ok(Some(res?))
            } else {
                Ok(None)
            }
        })
        .await?
    }

    pub async fn count_keys(&self) -> Result<i64> {
        let r = self.reader();
        tokio::task::spawn_blocking(move || {
            let conn = r.lock();
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM api_keys", [], |r| r.get(0))?;
            Ok(count)
        })
        .await?
    }

    pub async fn get_all_model_configs(
        &self,
    ) -> Result<std::collections::HashMap<String, (Option<String>, bool)>> {
        let r = self.reader();
        tokio::task::spawn_blocking(move || {
            let conn = r.lock();
            let mut stmt = conn.prepare("SELECT original_id, alias, enabled FROM model_configs")?;
            let rows = stmt.query_map([], |row| {
                let orig: String = row.get(0)?;
                let alias: Option<String> = row.get(1)?;
                let enabled: i64 = row.get(2)?;
                Ok((orig, (alias, enabled == 1)))
            })?;

            let mut map = std::collections::HashMap::new();
            for res in rows {
                if let Ok((orig, cfg)) = res {
                    map.insert(orig, cfg);
                }
            }
            Ok(map)
        })
        .await?
    }

    pub async fn resolve_model_alias(&self, requested: &str) -> Result<Option<String>> {
        let r = self.reader();
        let req_owned = requested.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = r.lock();
            let mut stmt = conn.prepare(
                "SELECT original_id FROM model_configs WHERE alias = ?1 AND enabled = 1",
            )?;
            let mut rows = stmt.query(params![req_owned])?;
            if let Some(row) = rows.next()? {
                let orig: String = row.get(0)?;
                return Ok(Some(orig));
            }

            let mut stmt2 =
                conn.prepare("SELECT enabled FROM model_configs WHERE original_id = ?1")?;
            let mut rows2 = stmt2.query(params![req_owned])?;
            if let Some(row2) = rows2.next()? {
                let enabled: i64 = row2.get(0)?;
                if enabled == 0 {
                    return Ok(None);
                }
            }

            Ok(Some(req_owned))
        })
        .await?
    }

    // ==========================================
    // ЗАПИСЬ (ЧЕРЕЗ WRITER-МЬЮТЕКС)
    // ==========================================

    pub async fn toggle_account(&self, id: i64) -> Result<bool> {
        let conn = self.writer.lock().await;
        conn.execute(
            "UPDATE accounts SET enabled = 1 - enabled WHERE id = ?1",
            params![id],
        )?;
        let mut stmt = conn.prepare("SELECT enabled FROM accounts WHERE id = ?1")?;
        let enabled_int: i64 = stmt.query_row(params![id], |r| r.get(0))?;
        Ok(enabled_int == 1)
    }

    pub async fn set_account_enabled(&self, id: i64, enabled: bool) -> Result<()> {
        let conn = self.writer.lock().await;
        conn.execute(
            "UPDATE accounts SET enabled = ?1 WHERE id = ?2",
            params![if enabled { 1 } else { 0 }, id],
        )?;
        Ok(())
    }

    pub async fn update_tokens(&self, id: i64, access_token: &str, expires_at: f64) -> Result<()> {
        let conn = self.writer.lock().await;
        conn.execute(
            "UPDATE accounts SET access_token = ?1, expires_at = ?2 WHERE id = ?3",
            params![access_token, expires_at, id],
        )?;
        Ok(())
    }

    pub async fn update_quota(
        &self,
        id: i64,
        quota_json: &str,
        gemini_rem: f64,
        claude_rem: f64,
        now: f64,
    ) -> Result<()> {
        let conn = self.writer.lock().await;
        conn.execute(
            r#"
            UPDATE accounts 
            SET quota_json = ?1, gemini_remaining = ?2, claude_remaining = ?3, last_quota_check = ?4 
            WHERE id = ?5
            "#,
            params![quota_json, gemini_rem, claude_rem, now, id],
        )?;
        Ok(())
    }

    pub async fn set_cooldown(&self, id: i64, cooldown_until: f64) -> Result<()> {
        let conn = self.writer.lock().await;
        conn.execute(
            "UPDATE accounts SET cooldown_until = ?1 WHERE id = ?2",
            params![cooldown_until, id],
        )?;
        Ok(())
    }

    pub async fn reset_all_cooldowns(&self) -> Result<()> {
        let conn = self.writer.lock().await;
        conn.execute("UPDATE accounts SET cooldown_until = 0", [])?;
        Ok(())
    }

    pub async fn add_account(&self, refresh_token: &str, email: Option<&str>) -> Result<i64> {
        let conn = self.writer.lock().await;
        conn.execute(
            "INSERT OR IGNORE INTO accounts (refresh_token, email) VALUES (?1, ?2)",
            params![refresh_token, email],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub async fn delete_account(&self, id: i64) -> Result<bool> {
        let conn = self.writer.lock().await;
        let rows = conn.execute("DELETE FROM accounts WHERE id = ?1", params![id])?;
        Ok(rows > 0)
    }

    pub async fn create_key(
        &self,
        name: &str,
        key: &str,
        rpm_limit: i64,
        token_limit: i64,
        is_admin: bool,
        now: f64,
    ) -> Result<ApiKey> {
        let conn = self.writer.lock().await;
        conn.execute(
            r#"
            INSERT INTO api_keys (name, key, rpm_limit, token_limit, tokens_used, requests_count, enabled, created_at, is_admin)
            VALUES (?1, ?2, ?3, ?4, 0, 0, 1, ?5, ?6)
            "#,
            params![name, key, rpm_limit, token_limit, now, if is_admin { 1 } else { 0 }],
        )?;
        let id = conn.last_insert_rowid();
        Ok(ApiKey {
            id,
            name: name.to_string(),
            key: key.to_string(),
            rpm_limit,
            token_limit,
            tokens_used: 0,
            requests_count: 0,
            enabled: true,
            created_at: now,
            is_admin,
        })
    }

    pub async fn update_key(
        &self,
        id: i64,
        name: Option<&str>,
        key: Option<&str>,
        rpm_limit: Option<i64>,
        token_limit: Option<i64>,
        enabled: Option<bool>,
        is_admin: Option<bool>,
    ) -> Result<()> {
        let conn = self.writer.lock().await;
        if let Some(n) = name {
            conn.execute(
                "UPDATE api_keys SET name = ?1 WHERE id = ?2",
                params![n, id],
            )?;
        }
        if let Some(k) = key {
            conn.execute("UPDATE api_keys SET key = ?1 WHERE id = ?2", params![k, id])?;
        }
        if let Some(r) = rpm_limit {
            conn.execute(
                "UPDATE api_keys SET rpm_limit = ?1 WHERE id = ?2",
                params![r, id],
            )?;
        }
        if let Some(t) = token_limit {
            conn.execute(
                "UPDATE api_keys SET token_limit = ?1 WHERE id = ?2",
                params![t, id],
            )?;
        }
        if let Some(en) = enabled {
            conn.execute(
                "UPDATE api_keys SET enabled = ?1 WHERE id = ?2",
                params![if en { 1 } else { 0 }, id],
            )?;
        }
        if let Some(adm) = is_admin {
            conn.execute(
                "UPDATE api_keys SET is_admin = ?1 WHERE id = ?2",
                params![if adm { 1 } else { 0 }, id],
            )?;
        }
        Ok(())
    }

    pub async fn delete_key(&self, id: i64) -> Result<bool> {
        let conn = self.writer.lock().await;
        let rows = conn.execute("DELETE FROM api_keys WHERE id = ?1", params![id])?;
        Ok(rows > 0)
    }

    pub async fn record_key_usage(&self, id: i64, tokens: i64) -> Result<()> {
        let conn = self.writer.lock().await;
        conn.execute(
            "UPDATE api_keys SET tokens_used = tokens_used + ?1, requests_count = requests_count + 1 WHERE id = ?2",
            params![tokens, id],
        )?;
        Ok(())
    }

    pub async fn set_model_config(
        &self,
        original_id: &str,
        alias: Option<&str>,
        enabled: bool,
        now: f64,
    ) -> Result<()> {
        let conn = self.writer.lock().await;
        conn.execute(
            r#"
            INSERT INTO model_configs (original_id, alias, enabled, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(original_id) DO UPDATE SET
                alias = excluded.alias,
                enabled = excluded.enabled,
                updated_at = excluded.updated_at
            "#,
            params![original_id, alias, if enabled { 1 } else { 0 }, now],
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: i64,
    pub name: String,
    pub key: String,
    pub rpm_limit: i64,
    pub token_limit: i64,
    pub tokens_used: i64,
    pub requests_count: i64,
    pub enabled: bool,
    pub created_at: f64,
    #[serde(default)]
    pub is_admin: bool,
}
