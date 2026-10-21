use anyhow::Result;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

#[derive(Debug, Clone)]
pub struct RequestLog {
    pub account_id: Option<i64>,
    pub account_email: Option<String>,
    pub model: String,
    pub stream: bool,
    pub status_code: u16,
    pub latency_ms: u64,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    pub user_prompt_preview: Option<String>,
    pub response_preview: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EventLog {
    pub level: String,
    pub component: String,
    pub message: String,
}

pub enum LogEntry {
    Request(RequestLog),
    Event(EventLog),
}

#[derive(Clone)]
pub struct AuditLogger {
    tx: UnboundedSender<LogEntry>,
}

impl AuditLogger {
    pub fn init<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let path_buf = PathBuf::from(db_path.as_ref());
        if let Some(parent) = path_buf.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }

        let (tx, mut rx) = unbounded_channel::<LogEntry>();

        // Запуск выделенного потока фоновой записи логов
        std::thread::Builder::new()
            .name("audit_db_writer".to_string())
            .spawn(move || {
                let conn = match Connection::open(&path_buf) {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("[AUDIT_LOG] Failed to open database {:?}: {}", path_buf, e);
                        return;
                    }
                };

                let _ = conn.execute_batch(
                    r#"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA busy_timeout = 10000;

                    CREATE TABLE IF NOT EXISTS request_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        timestamp_epoch REAL NOT NULL,
                        account_id INTEGER,
                        account_email TEXT,
                        model TEXT NOT NULL,
                        stream INTEGER NOT NULL,
                        status_code INTEGER NOT NULL,
                        latency_ms INTEGER NOT NULL,
                        prompt_tokens INTEGER DEFAULT 0,
                        completion_tokens INTEGER DEFAULT 0,
                        total_tokens INTEGER DEFAULT 0,
                        user_prompt_preview TEXT,
                        response_preview TEXT,
                        error TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_requests_created 
                    ON request_logs(timestamp_epoch DESC);

                    CREATE INDEX IF NOT EXISTS idx_requests_model 
                    ON request_logs(model, status_code);

                    CREATE TABLE IF NOT EXISTS system_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        timestamp_epoch REAL NOT NULL,
                        level TEXT NOT NULL,
                        component TEXT NOT NULL,
                        message TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_events_created 
                    ON system_events(timestamp_epoch DESC);
                    "#,
                );

                while let Some(entry) = rx.blocking_recv() {
                    let now_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
                    let created_at = chrono_like_now();

                    match entry {
                        LogEntry::Request(req) => {
                            let _ = conn.execute(
                                r#"
                                INSERT INTO request_logs (
                                    created_at, timestamp_epoch, account_id, account_email,
                                    model, stream, status_code, latency_ms,
                                    prompt_tokens, completion_tokens, total_tokens,
                                    user_prompt_preview, response_preview, error
                                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                                "#,
                                params![
                                    created_at,
                                    now_epoch,
                                    req.account_id,
                                    req.account_email,
                                    req.model,
                                    if req.stream { 1 } else { 0 },
                                    req.status_code as i64,
                                    req.latency_ms as i64,
                                    req.prompt_tokens as i64,
                                    req.completion_tokens as i64,
                                    req.total_tokens as i64,
                                    req.user_prompt_preview,
                                    req.response_preview,
                                    req.error,
                                ],
                            );
                        }
                        LogEntry::Event(ev) => {
                            let _ = conn.execute(
                                r#"
                                INSERT INTO system_events (
                                    created_at, timestamp_epoch, level, component, message
                                ) VALUES (?1, ?2, ?3, ?4, ?5)
                                "#,
                                params![
                                    created_at,
                                    now_epoch,
                                    ev.level,
                                    ev.component,
                                    ev.message,
                                ],
                            );
                        }
                    }
                }
            })?;

        Ok(Self { tx })
    }

    pub fn log_request(&self, req: RequestLog) {
        let _ = self.tx.send(LogEntry::Request(req));
    }

    pub fn log_event(&self, level: &str, component: &str, message: &str) {
        let _ = self.tx.send(LogEntry::Event(EventLog {
            level: level.to_string(),
            component: component.to_string(),
            message: message.to_string(),
        }));
    }
}

fn chrono_like_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let sec = now % 60;
    let min = (now / 60) % 60;
    let hour = (now / 3600) % 24;
    format!("{:02}:{:02}:{:02} UTC", hour, min, sec)
}
