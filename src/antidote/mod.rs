pub mod cargo_helper;
pub mod learner;
pub mod lock_resolver;
pub mod rustc_gate;
pub mod win_decoder;

use log::info;
use regex::Regex;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cargo_helper::diagnose_cargo_missing;
use learner::AutoLearner;
use lock_resolver::{resolve_file_lock, resolve_port_lock};
use rustc_gate::diagnose_rustc_error;
use win_decoder::decode_windows_error;

const DEFAULT_SO_KEY: &str = "rl_6c8ymMCr3MxhmEswF9vbpUHfj";

#[derive(Debug, Clone)]
pub struct SolutionHit {
    pub issue_id: String,
    pub title: String,
    pub solution: String,
    pub source: String,
    pub score: f64,
}

#[derive(Clone)]
pub struct AntidoteEngine {
    db: Arc<Mutex<Connection>>,
    http: reqwest::Client,
    fast_gate: Regex,
    ignore_tools: Regex,
    so_key: String,
    learner: Arc<AutoLearner>,
}

pub fn load_key_from_vault(service: &str) -> Option<String> {
    let conn = rusqlite::Connection::open_with_flags(
        "C:/Hermes/db/keys.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    let mut stmt = conn
        .prepare("SELECT value FROM keys WHERE service = ?1")
        .ok()?;
    stmt.query_row(rusqlite::params![service], |r| r.get(0))
        .ok()
}

impl AntidoteEngine {
    pub fn new(db_path: &Path, so_key: Option<String>) -> anyhow::Result<Self> {
        if let Some(parent) = db_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS problems (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                error_signature TEXT NOT NULL,
                problem_text TEXT NOT NULL,
                solution_text TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'local',
                external_url TEXT,
                hit_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_problems_issue_id ON problems(issue_id);

            CREATE VIRTUAL TABLE IF NOT EXISTS problems_fts USING fts5(
                title,
                error_signature,
                problem_text,
                content='problems',
                content_rowid='id',
                tokenize='unicode61 remove_diacritics 2'
            );

            CREATE TRIGGER IF NOT EXISTS problems_ai AFTER INSERT ON problems BEGIN
                INSERT INTO problems_fts(rowid, title, error_signature, problem_text)
                VALUES (new.id, new.title, new.error_signature, new.problem_text);
            END;

            CREATE TRIGGER IF NOT EXISTS problems_ad AFTER DELETE ON problems BEGIN
                INSERT INTO problems_fts(problems_fts, rowid, title, error_signature, problem_text)
                VALUES ('delete', old.id, old.title, old.error_signature, old.problem_text);
            END;

            CREATE TRIGGER IF NOT EXISTS problems_au AFTER UPDATE ON problems BEGIN
                INSERT INTO problems_fts(problems_fts, rowid, title, error_signature, problem_text)
                VALUES ('delete', old.id, old.title, old.error_signature, old.problem_text);
                INSERT INTO problems_fts(rowid, title, error_signature, problem_text)
                VALUES (new.id, new.title, new.error_signature, new.problem_text);
            END;
        ",
        )?;

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(800))
            .build()?;

        let fast_gate = Regex::new(
            r#"(?i)(?:"?exit[ _]code"?:?\s*[1-9]|traceback \(most recent call last\)|panic! at|0x[0-9a-f]{8}|STATUS_[A-Z_]+|fatal error|segmentation fault|overflowed its stack|error\[E\d{4}\]|cannot find derive macro|no method named|os error \d+|WSAE[A-Z]+)"#,
        )?;

        let ignore_tools =
            Regex::new(r#"(?i)"name"\s*:\s*"(?:read_file|search_files|session_search)""#)?;

        let key = so_key
            .or_else(|| load_key_from_vault("stackexchange_api_key"))
            .unwrap_or_else(|| DEFAULT_SO_KEY.to_string());

        info!(
            "[ANTIDOTE] Engine v2.0 initialized with DB at {:?}",
            db_path
        );

        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            http,
            fast_gate,
            ignore_tools,
            so_key: key,
            learner: Arc::new(AutoLearner::new()),
        })
    }

    pub fn check_fast_gate(&self, msg_str: &str, is_anthropic_error: bool) -> bool {
        if is_anthropic_error {
            return true;
        }
        if self.ignore_tools.is_match(msg_str) {
            return false;
        }
        self.fast_gate.is_match(msg_str)
    }

    pub fn query_l1(&self, query: &str) -> Option<SolutionHit> {
        let conn = match self.db.lock() {
            Ok(c) => c,
            Err(_) => return None,
        };

        let text_to_search = if let Ok(val) = serde_json::from_str::<Value>(query) {
            val.get("output")
                .or_else(|| val.get("content"))
                .or_else(|| val.get("error"))
                .and_then(|v| v.as_str())
                .unwrap_or(query)
                .to_string()
        } else {
            query.to_string()
        };

        let stopwords = [
            "output",
            "exit_code",
            "error",
            "failed",
            "to",
            "run",
            "for",
            "the",
            "a",
            "an",
            "in",
            "of",
            "and",
            "is",
        ];
        let cleaned_text = text_to_search.replace(
            [
                '-', '"', '\'', '(', ')', '{', '}', ':', '*', '^', '\\', '\n', '\r', '/', '.', '_',
                '`',
            ],
            " ",
        );
        let keywords: Vec<&str> = cleaned_text
            .split_whitespace()
            .filter(|w| w.len() > 2 && !stopwords.contains(&w.to_lowercase().as_str()))
            .take(6)
            .collect();

        if keywords.is_empty() {
            return None;
        }

        let clean_q = keywords.join(" OR ");

        let mut stmt = conn
            .prepare(
                "SELECT p.issue_id, p.title, p.solution_text, p.source, bm25(problems_fts) as score
             FROM problems_fts f
             JOIN problems p ON p.id = f.rowid
             WHERE problems_fts MATCH ?1
             ORDER BY score ASC
             LIMIT 1",
            )
            .ok()?;

        let mut rows = stmt.query(params![clean_q]).ok()?;
        if let Some(row) = rows.next().ok()? {
            Some(SolutionHit {
                issue_id: row.get(0).unwrap_or_default(),
                title: row.get(1).unwrap_or_default(),
                solution: row.get(2).unwrap_or_default(),
                source: row.get(3).unwrap_or_default(),
                score: row.get(4).unwrap_or(0.0),
            })
        } else {
            None
        }
    }

    pub async fn query_l2_so(&self, error_text: &str) -> Option<SolutionHit> {
        let search_terms =
            if error_text.contains("overflowed its stack") || error_text.contains("0xc00000fd") {
                "tokio stack overflow"
            } else if error_text.contains("10054") {
                "os error 10054 connection reset"
            } else if error_text.contains("openssl") {
                "openssl Could not find directory of OpenSSL"
            } else {
                "custom build command"
            };

        let search_url = "https://api.stackexchange.com/2.3/search/advanced";

        let resp = self
            .http
            .get(search_url)
            .query(&[
                ("site", "stackoverflow"),
                ("q", search_terms),
                ("accepted", "True"),
                ("sort", "votes"),
                ("pagesize", "1"),
                ("key", &self.so_key),
            ])
            .header("Accept-Encoding", "gzip")
            .header("User-Agent", "Hydra-Antidote/2.0")
            .send()
            .await
            .ok()?;

        let val: Value = resp.json().await.ok()?;
        let items = val.get("items")?.as_array()?;
        if items.is_empty() {
            return None;
        }

        let q = &items[0];
        let q_id = q.get("question_id")?.as_i64()?;
        let ans_id = q.get("accepted_answer_id")?.as_i64()?;
        let title = q.get("title")?.as_str()?.to_string();

        let ans_url = format!(
            "https://api.stackexchange.com/2.3/answers/{}?site=stackoverflow&filter=withbody&key={}",
            ans_id, self.so_key
        );

        let ans_resp = self
            .http
            .get(&ans_url)
            .header("Accept-Encoding", "gzip")
            .header("User-Agent", "Hydra-Antidote/2.0")
            .send()
            .await
            .ok()?;

        let ans_val: Value = ans_resp.json().await.ok()?;
        let ans_items = ans_val.get("items")?.as_array()?;
        if ans_items.is_empty() {
            return None;
        }

        let body_raw = ans_items[0].get("body")?.as_str()?.to_string();
        let clean_body = body_raw
            .replace("<p>", "")
            .replace("</p>", "\n")
            .replace("<code>", "`")
            .replace("</code>", "`")
            .replace("<pre>", "\n```\n")
            .replace("</pre>", "```\n")
            .replace("&#39;", "'")
            .replace("&quot;", "\"")
            .trim()
            .to_string();

        let issue_id = format!("so-{}", q_id);

        if let Ok(conn) = self.db.lock() {
            let _ = conn.execute(
                "INSERT INTO problems (issue_id, title, error_signature, problem_text, solution_text, source, external_url, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'stackoverflow', ?6, datetime('now'), datetime('now'))
                 ON CONFLICT(issue_id) DO NOTHING",
                params![
                    issue_id,
                    title,
                    search_terms,
                    error_text,
                    clean_body,
                    format!("https://stackoverflow.com/q/{}", q_id)
                ],
            );
        }

        Some(SolutionHit {
            issue_id,
            title,
            solution: clean_body,
            source: "stackoverflow".to_string(),
            score: 0.0,
        })
    }

    pub fn generate_fast_hint(&self, content_str: &str) -> Option<String> {
        let t0 = Instant::now();

        // 1. Port Lock Resolver
        if let Some(lock) = resolve_port_lock(content_str) {
            let dur = t0.elapsed();
            return Some(format!(
                "\n\n[HYDRA ANTIDOTE HINT: Port Lock Resolver ({:?})]\n[LOCK DETECTED: Порт {} занят -> PID: {}, Process: {}]\nРекомендация: Освободите сокет или укажите альтернативный порт перед повторным запуском.",
                dur, lock.port, lock.pid, lock.process_name
            ));
        }

        // 2. File Lock Resolver
        if let Some(file_lock) = resolve_file_lock(content_str) {
            let dur = t0.elapsed();
            return Some(format!(
                "\n\n[HYDRA ANTIDOTE HINT: File Lock Resolver ({:?})]\n[FILE LOCK DETECTED: Файл {} заблокирован -> PID: {}, Process: {}]\nРекомендация: Процесс удерживает файловый дескриптор. Завершите процесс для обновления бинарника.",
                dur, file_lock.filename, file_lock.pid, file_lock.process_name
            ));
        }

        // 3. WinAPI / NTSTATUS / Winsock Fast-Decoder
        if let Some(win_diag) = decode_windows_error(content_str) {
            let dur = t0.elapsed();
            return Some(format!(
                "\n\n[HYDRA ANTIDOTE HINT: WinAPI Fast-Decoder ({:?})]\n[{}: {} | {}]\nСуть: {}\nРешение: {}",
                dur, win_diag.category, win_diag.code, win_diag.name, win_diag.description, win_diag.recommended_action
            ));
        }

        // 4. Rustc Compiler Fast-Gate
        if let Some(rustc) = diagnose_rustc_error(content_str) {
            let dur = t0.elapsed();
            return Some(format!(
                "\n\n[HYDRA ANTIDOTE HINT: Rustc Fast-Gate ({:?})]\n[RUSTC: {} \"{}\"]\nСуть: {}\nРешение: {}",
                dur, rustc.code, rustc.title, rustc.explanation, rustc.common_fix
            ));
        }

        // 5. Cargo Trait & Feature Helper
        if let Some(cargo) = diagnose_cargo_missing(content_str) {
            let dur = t0.elapsed();
            let mut sol = format!("Суть: {}\n", cargo.explanation);
            if let Some(imp) = cargo.required_import {
                sol.push_str(&format!("Требуемый импорт: {}\n", imp));
            }
            if let Some(feat) = cargo.required_feature {
                sol.push_str(&format!("Требуемая фича в Cargo.toml: {}\n", feat));
            }
            return Some(format!(
                "\n\n[HYDRA ANTIDOTE HINT: Cargo Trait/Feature Helper ({:?})]\n[CARGO: {}]\n{}",
                dur,
                cargo.target,
                sol.trim()
            ));
        }

        None
    }

    pub async fn inspect_and_inject_openai(&self, messages: &mut [Value]) -> Option<String> {
        let last_msg = messages.last_mut()?;
        let obj = last_msg.as_object_mut()?;

        let role = obj.get("role").and_then(|v| v.as_str()).unwrap_or_default();
        if role != "tool" {
            return None;
        }

        let content_str = match obj.get("content") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => return None,
        };

        // Auto-Learner hook: track success or failure
        if content_str.contains("\"exit_code\": 0") || content_str.contains("\"exit_code\":0") {
            self.learner.record_success(&self.db);
        } else if content_str.contains("exit_code") && !content_str.contains("\"exit_code\": 0") {
            self.learner
                .record_failure("terminal_exit_error", &content_str);
        }

        if !self.check_fast_gate(&content_str, false) {
            return None;
        }

        // Fast-Gates (0.01 - 0.5 ms)
        if let Some(hint) = self.generate_fast_hint(&content_str) {
            if let Some(Value::String(s)) = obj.get_mut("content") {
                s.push_str(&hint);
            }
            info!("[ANTIDOTE] Local Fast-Gate triggered for OpenAI tool result");
            return Some("FAST_GATE_HIT".to_string());
        }

        // L1 query (SQLite FTS5)
        let t1 = Instant::now();
        if let Some(hit) = self.query_l1(&content_str) {
            let dur = t1.elapsed();
            let hint = format!(
                "\n\n[HYDRA ANTIDOTE HINT: #{} \"{}\"]\nРешение (L1 Fast-Cache, {:?}):\n{}",
                hit.issue_id, hit.title, dur, hit.solution
            );
            if let Some(Value::String(s)) = obj.get_mut("content") {
                s.push_str(&hint);
            }
            info!(
                "[ANTIDOTE] L1 Cache Hit in {:?} for OpenAI tool result",
                dur
            );
            return Some(format!("L1_HIT in {:?}", dur));
        }

        // L2 query (Stack Overflow API)
        let t2 = Instant::now();
        if let Some(hit) = self.query_l2_so(&content_str).await {
            let dur = t2.elapsed();
            let hint = format!(
                "\n\n[HYDRA ANTIDOTE HINT: #{} \"{}\"]\nРешение (L2 Stack Overflow, {:?}):\n{}",
                hit.issue_id, hit.title, dur, hit.solution
            );
            if let Some(Value::String(s)) = obj.get_mut("content") {
                s.push_str(&hint);
            }
            info!(
                "[ANTIDOTE] L2 Stack Overflow Hit in {:?} for OpenAI tool result",
                dur
            );
            return Some(format!("L2_HIT in {:?}", dur));
        }

        None
    }

    pub async fn inspect_and_inject_anthropic(&self, messages: &mut [Value]) -> Option<String> {
        let last_msg = messages.last_mut()?;
        let obj = last_msg.as_object_mut()?;
        let content_arr = obj.get_mut("content")?.as_array_mut()?;

        for block in content_arr.iter_mut() {
            let b_obj = block.as_object_mut()?;
            if b_obj.get("type").and_then(|v| v.as_str()) != Some("tool_result") {
                continue;
            }

            let is_error = b_obj
                .get("is_error")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let content_str = match b_obj.get("content") {
                Some(Value::String(s)) => s.clone(),
                Some(other) => other.to_string(),
                None => continue,
            };

            // Auto-Learner hook: track success or failure
            if content_str.contains("\"exit_code\": 0") || content_str.contains("\"exit_code\":0") {
                self.learner.record_success(&self.db);
            } else if content_str.contains("exit_code") && !content_str.contains("\"exit_code\": 0")
            {
                self.learner
                    .record_failure("terminal_exit_error", &content_str);
            }

            if !self.check_fast_gate(&content_str, is_error) {
                continue;
            }

            // Fast-Gates (0.01 - 0.5 ms)
            if let Some(hint) = self.generate_fast_hint(&content_str) {
                if let Some(Value::String(s)) = b_obj.get_mut("content") {
                    s.push_str(&hint);
                }
                info!("[ANTIDOTE] Local Fast-Gate triggered for Anthropic tool result");
                return Some("FAST_GATE_HIT".to_string());
            }

            // L1 query (SQLite FTS5)
            let t1 = Instant::now();
            if let Some(hit) = self.query_l1(&content_str) {
                let dur = t1.elapsed();
                let hint = format!(
                    "\n\n[HYDRA ANTIDOTE HINT: #{} \"{}\"]\nРешение (L1 Fast-Cache, {:?}):\n{}",
                    hit.issue_id, hit.title, dur, hit.solution
                );
                if let Some(Value::String(s)) = b_obj.get_mut("content") {
                    s.push_str(&hint);
                }
                info!(
                    "[ANTIDOTE] L1 Cache Hit in {:?} for Anthropic tool result",
                    dur
                );
                return Some(format!("L1_HIT in {:?}", dur));
            }

            // L2 query (Stack Overflow API)
            let t2 = Instant::now();
            if let Some(hit) = self.query_l2_so(&content_str).await {
                let dur = t2.elapsed();
                let hint = format!(
                    "\n\n[HYDRA ANTIDOTE HINT: #{} \"{}\"]\nРешение (L2 Stack Overflow, {:?}):\n{}",
                    hit.issue_id, hit.title, dur, hit.solution
                );
                if let Some(Value::String(s)) = b_obj.get_mut("content") {
                    s.push_str(&hint);
                }
                info!(
                    "[ANTIDOTE] L2 Stack Overflow Hit in {:?} for Anthropic tool result",
                    dur
                );
                return Some(format!("L2_HIT in {:?}", dur));
            }
        }

        None
    }

    pub fn get_recent_problems(&self, limit: usize, offset: usize) -> anyhow::Result<Vec<serde_json::Value>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT issue_id, title, error_signature, problem_text, solution_text, source, hit_count, created_at, COALESCE(tags, '')
            FROM problems
            ORDER BY id DESC
            LIMIT ?1 OFFSET ?2
            "#,
        )?;
        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(serde_json::json!({
                "issue_id": row.get::<_, String>(0)?,
                "title": row.get::<_, String>(1)?,
                "error_signature": row.get::<_, String>(2)?,
                "problem_text": row.get::<_, String>(3)?,
                "solution_text": row.get::<_, String>(4)?,
                "source": row.get::<_, String>(5)?,
                "hit_count": row.get::<_, i64>(6)?,
                "created_at": row.get::<_, String>(7)?,
                "tags": row.get::<_, String>(8)?,
            }))
        })?;
        let mut list = Vec::new();
        for r in rows {
            if let Ok(v) = r {
                list.push(v);
            }
        }
        Ok(list)
    }

    pub fn search_problems(&self, query: &str, limit: usize) -> anyhow::Result<Vec<serde_json::Value>> {
        let conn = self.db.lock().unwrap();
        let q_pattern = format!("%{}%", query.trim());
        let mut stmt = conn.prepare(
            r#"
            SELECT issue_id, title, error_signature, problem_text, solution_text, source, hit_count, created_at, COALESCE(tags, '')
            FROM problems
            WHERE title LIKE ?1 OR error_signature LIKE ?1 OR problem_text LIKE ?1 OR tags LIKE ?1
            ORDER BY hit_count DESC, id DESC
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![q_pattern, limit as i64], |row| {
            Ok(serde_json::json!({
                "issue_id": row.get::<_, String>(0)?,
                "title": row.get::<_, String>(1)?,
                "error_signature": row.get::<_, String>(2)?,
                "problem_text": row.get::<_, String>(3)?,
                "solution_text": row.get::<_, String>(4)?,
                "source": row.get::<_, String>(5)?,
                "hit_count": row.get::<_, i64>(6)?,
                "created_at": row.get::<_, String>(7)?,
                "tags": row.get::<_, String>(8)?,
            }))
        })?;
        let mut list = Vec::new();
        for r in rows {
            if let Ok(v) = r {
                list.push(v);
            }
        }
        Ok(list)
    }

    pub fn get_stats(&self) -> anyhow::Result<serde_json::Value> {
        let conn = self.db.lock().unwrap();
        let total_problems: i64 = conn.query_row("SELECT count(*) FROM problems", [], |r| r.get(0)).unwrap_or(0);
        let total_hits: i64 = conn.query_row("SELECT COALESCE(sum(hit_count), 0) FROM problems", [], |r| r.get(0)).unwrap_or(0);
        Ok(serde_json::json!({
            "total_solutions": total_problems,
            "total_injections": total_hits,
            "engine_version": "2.0.0",
        }))
    }

    pub fn add_problem(&self, title: &str, error_sig: &str, solution: &str, tags: &str) -> anyhow::Result<String> {
        let conn = self.db.lock().unwrap();
        let issue_id = format!("ISSUE-{}", uuid::Uuid::new_v4().simple());
        let now = chrono_like_now_antidote();
        conn.execute(
            r#"
            INSERT INTO problems (issue_id, title, error_signature, problem_text, solution_text, source, hit_count, created_at, updated_at, tags)
            VALUES (?1, ?2, ?3, ?2, ?4, 'admin', 0, ?5, ?5, ?6)
            "#,
            params![issue_id, title, error_sig, solution, now, tags],
        )?;
        Ok(issue_id)
    }
}

fn chrono_like_now_antidote() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let sec = now % 60;
    let min = (now / 60) % 60;
    let hour = (now / 3600) % 24;
    format!("{:02}:{:02}:{:02} UTC", hour, min, sec)
}
