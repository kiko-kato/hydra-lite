use anyhow::Result;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMetric {
    pub timestamp_epoch: f64,
    pub created_at: String,
    pub model: String,
    pub account_id: Option<i64>,
    pub account_email: Option<String>,
    pub proxy_used: Option<String>,
    pub stream: bool,
    pub status_code: u16,
    pub connect_latency_ms: u64,
    pub ttft_ms: u64,
    pub thinking_time_ms: u64,
    pub generation_time_ms: u64,
    pub total_latency_ms: u64,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub cached_tokens: u32,
    pub reasoning_tokens: u32,
    pub total_tokens: u32,
    pub tok_per_sec: f64,
    pub clean_tok_per_sec: f64,
    pub chunks_count: u32,
    pub max_chunk_gap_ms: u64,
    pub fast_gate_us: u64,
    pub cache_hit_type: String, // "miss" | "prefix" | "exact"
    pub error: Option<String>,
    pub key_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryAverages {
    pub total_requests: u64,
    pub avg_ttft_ms: f64,
    pub avg_thinking_time_ms: f64,
    pub avg_generation_time_ms: f64,
    pub avg_clean_tok_per_sec: f64,
    pub avg_max_chunk_gap_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySummary {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub global_hitrate_percent: f64,
    pub total_tokens_saved_by_cache: u64,
    pub total_prompt_tokens: u64,
    pub total_completion_tokens: u64,
    pub avg_ttft_ms: f64,
    pub avg_tok_per_sec: f64,
    pub model_breakdown: Vec<ModelMetricSummary>,
    pub account_breakdown: Vec<AccountMetricSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetricSummary {
    pub model: String,
    pub request_count: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub avg_tok_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMetricSummary {
    pub account_id: i64,
    pub email: String,
    pub request_count: u64,
    pub total_tokens: u64,
}

#[derive(Clone)]
pub struct TelemetryEngine {
    tx: UnboundedSender<RequestMetric>,
    db_path: PathBuf,
}

impl TelemetryEngine {
    pub fn init<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let path_buf = PathBuf::from(db_path.as_ref());
        if let Some(parent) = path_buf.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }

        let (tx, mut rx) = unbounded_channel::<RequestMetric>();
        let worker_path = path_buf.clone();

        std::thread::Builder::new()
            .name("telemetry_writer".to_string())
            .spawn(move || {
                let conn = match Connection::open(&worker_path) {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("[TELEMETRY] Failed to open database {:?}: {}", worker_path, e);
                        return;
                    }
                };

                let _ = conn.execute_batch(
                    r#"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA busy_timeout = 10000;

                    CREATE TABLE IF NOT EXISTS request_analytics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp_epoch REAL NOT NULL,
                        created_at TEXT NOT NULL,
                        model TEXT NOT NULL,
                        account_id INTEGER,
                        account_email TEXT,
                        proxy_used TEXT,
                        stream INTEGER NOT NULL,
                        status_code INTEGER NOT NULL,
                        connect_latency_ms INTEGER NOT NULL,
                        ttft_ms INTEGER NOT NULL,
                        thinking_time_ms INTEGER NOT NULL DEFAULT 0,
                        generation_time_ms INTEGER NOT NULL DEFAULT 0,
                        total_latency_ms INTEGER NOT NULL,
                        prompt_tokens INTEGER NOT NULL,
                        completion_tokens INTEGER NOT NULL,
                        cached_tokens INTEGER NOT NULL,
                        reasoning_tokens INTEGER NOT NULL,
                        total_tokens INTEGER NOT NULL,
                        tok_per_sec REAL NOT NULL,
                        clean_tok_per_sec REAL NOT NULL DEFAULT 0.0,
                        chunks_count INTEGER NOT NULL DEFAULT 0,
                        max_chunk_gap_ms INTEGER NOT NULL DEFAULT 0,
                        fast_gate_us INTEGER NOT NULL DEFAULT 0,
                        cache_hit_type TEXT NOT NULL,
                        error TEXT,
                        key_name TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_analytics_epoch ON request_analytics(timestamp_epoch DESC);
                    CREATE INDEX IF NOT EXISTS idx_analytics_model ON request_analytics(model, status_code);
                    "#,
                );

                let _ = conn.execute("ALTER TABLE request_analytics ADD COLUMN key_name TEXT;", []);
                let _ = conn.execute("ALTER TABLE request_analytics ADD COLUMN thinking_time_ms INTEGER NOT NULL DEFAULT 0;", []);
                let _ = conn.execute("ALTER TABLE request_analytics ADD COLUMN generation_time_ms INTEGER NOT NULL DEFAULT 0;", []);
                let _ = conn.execute("ALTER TABLE request_analytics ADD COLUMN clean_tok_per_sec REAL NOT NULL DEFAULT 0.0;", []);
                let _ = conn.execute("ALTER TABLE request_analytics ADD COLUMN chunks_count INTEGER NOT NULL DEFAULT 0;", []);
                let _ = conn.execute("ALTER TABLE request_analytics ADD COLUMN max_chunk_gap_ms INTEGER NOT NULL DEFAULT 0;", []);
                let _ = conn.execute("ALTER TABLE request_analytics ADD COLUMN fast_gate_us INTEGER NOT NULL DEFAULT 0;", []);

                while let Some(m) = rx.blocking_recv() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO request_analytics (
                            timestamp_epoch, created_at, model, account_id, account_email,
                            proxy_used, stream, status_code, connect_latency_ms, ttft_ms,
                            thinking_time_ms, generation_time_ms, total_latency_ms,
                            prompt_tokens, completion_tokens, cached_tokens, reasoning_tokens,
                            total_tokens, tok_per_sec, clean_tok_per_sec, chunks_count,
                            max_chunk_gap_ms, fast_gate_us, cache_hit_type, error, key_name
                        ) VALUES (
                            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
                            ?21, ?22, ?23, ?24, ?25, ?26
                        )
                        "#,
                        params![
                            m.timestamp_epoch,
                            m.created_at,
                            m.model,
                            m.account_id,
                            m.account_email,
                            m.proxy_used,
                            if m.stream { 1 } else { 0 },
                            m.status_code as i64,
                            m.connect_latency_ms as i64,
                            m.ttft_ms as i64,
                            m.thinking_time_ms as i64,
                            m.generation_time_ms as i64,
                            m.total_latency_ms as i64,
                            m.prompt_tokens as i64,
                            m.completion_tokens as i64,
                            m.cached_tokens as i64,
                            m.reasoning_tokens as i64,
                            m.total_tokens as i64,
                            m.tok_per_sec,
                            m.clean_tok_per_sec,
                            m.chunks_count as i64,
                            m.max_chunk_gap_ms as i64,
                            m.fast_gate_us as i64,
                            m.cache_hit_type,
                            m.error,
                            m.key_name,
                        ],
                    );
                }
            })?;

        Ok(Self {
            tx,
            db_path: path_buf,
        })
    }

    pub fn record(&self, metric: RequestMetric) {
        let _ = self.tx.send(metric);
    }

    pub fn get_recent_requests(&self, limit: usize) -> Result<Vec<RequestMetric>> {
        let conn = Connection::open(&self.db_path)?;
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_epoch, created_at, model, account_id, account_email,
                   proxy_used, stream, status_code, connect_latency_ms, ttft_ms,
                   thinking_time_ms, generation_time_ms, total_latency_ms,
                   prompt_tokens, completion_tokens, cached_tokens, reasoning_tokens,
                   total_tokens, tok_per_sec, clean_tok_per_sec, chunks_count,
                   max_chunk_gap_ms, fast_gate_us, cache_hit_type, error, key_name
            FROM request_analytics
            ORDER BY id DESC
            LIMIT ?1
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(RequestMetric {
                timestamp_epoch: row.get(0)?,
                created_at: row.get(1)?,
                model: row.get(2)?,
                account_id: row.get(3)?,
                account_email: row.get(4)?,
                proxy_used: row.get(5)?,
                stream: row.get::<_, i64>(6)? == 1,
                status_code: row.get::<_, i64>(7)? as u16,
                connect_latency_ms: row.get::<_, i64>(8)? as u64,
                ttft_ms: row.get::<_, i64>(9)? as u64,
                thinking_time_ms: row.get::<_, i64>(10)? as u64,
                generation_time_ms: row.get::<_, i64>(11)? as u64,
                total_latency_ms: row.get::<_, i64>(12)? as u64,
                prompt_tokens: row.get::<_, i64>(13)? as u32,
                completion_tokens: row.get::<_, i64>(14)? as u32,
                cached_tokens: row.get::<_, i64>(15)? as u32,
                reasoning_tokens: row.get::<_, i64>(16)? as u32,
                total_tokens: row.get::<_, i64>(17)? as u32,
                tok_per_sec: row.get(18)?,
                clean_tok_per_sec: row.get(19)?,
                chunks_count: row.get::<_, i64>(20)? as u32,
                max_chunk_gap_ms: row.get::<_, i64>(21)? as u64,
                fast_gate_us: row.get::<_, i64>(22)? as u64,
                cache_hit_type: row.get(23)?,
                error: row.get(24)?,
                key_name: row.get(25)?,
            })
        })?;

        let mut res = Vec::new();
        for r in rows {
            if let Ok(m) = r {
                res.push(m);
            }
        }
        Ok(res)
    }

    pub fn get_recent_errors(&self, limit: usize) -> Result<Vec<RequestMetric>> {
        let conn = Connection::open(&self.db_path)?;
        let mut stmt = conn.prepare(
            r#"
            SELECT timestamp_epoch, created_at, model, account_id, account_email,
                   proxy_used, stream, status_code, connect_latency_ms, ttft_ms,
                   thinking_time_ms, generation_time_ms, total_latency_ms,
                   prompt_tokens, completion_tokens, cached_tokens, reasoning_tokens,
                   total_tokens, tok_per_sec, clean_tok_per_sec, chunks_count,
                   max_chunk_gap_ms, fast_gate_us, cache_hit_type, error, key_name
            FROM request_analytics
            WHERE error IS NOT NULL OR status_code >= 400
            ORDER BY id DESC
            LIMIT ?1
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(RequestMetric {
                timestamp_epoch: row.get(0)?,
                created_at: row.get(1)?,
                model: row.get(2)?,
                account_id: row.get(3)?,
                account_email: row.get(4)?,
                proxy_used: row.get(5)?,
                stream: row.get::<_, i64>(6)? == 1,
                status_code: row.get::<_, i64>(7)? as u16,
                connect_latency_ms: row.get::<_, i64>(8)? as u64,
                ttft_ms: row.get::<_, i64>(9)? as u64,
                thinking_time_ms: row.get::<_, i64>(10)? as u64,
                generation_time_ms: row.get::<_, i64>(11)? as u64,
                total_latency_ms: row.get::<_, i64>(12)? as u64,
                prompt_tokens: row.get::<_, i64>(13)? as u32,
                completion_tokens: row.get::<_, i64>(14)? as u32,
                cached_tokens: row.get::<_, i64>(15)? as u32,
                reasoning_tokens: row.get::<_, i64>(16)? as u32,
                total_tokens: row.get::<_, i64>(17)? as u32,
                tok_per_sec: row.get(18)?,
                clean_tok_per_sec: row.get(19)?,
                chunks_count: row.get::<_, i64>(20)? as u32,
                max_chunk_gap_ms: row.get::<_, i64>(21)? as u64,
                fast_gate_us: row.get::<_, i64>(22)? as u64,
                cache_hit_type: row.get(23)?,
                error: row.get(24)?,
                key_name: row.get(25)?,
            })
        })?;

        let mut res = Vec::new();
        for r in rows {
            if let Ok(m) = r {
                res.push(m);
            }
        }
        Ok(res)
    }

    pub fn get_averages(&self) -> Result<TelemetryAverages> {
        let conn = Connection::open(&self.db_path)?;
        let mut stmt = conn.prepare(
            r#"
            SELECT 
                count(*),
                avg(CASE WHEN status_code = 200 AND ttft_ms > 0 THEN ttft_ms ELSE NULL END),
                avg(CASE WHEN status_code = 200 AND thinking_time_ms > 0 THEN thinking_time_ms ELSE NULL END),
                avg(CASE WHEN status_code = 200 AND generation_time_ms > 0 THEN generation_time_ms ELSE NULL END),
                avg(CASE WHEN status_code = 200 AND clean_tok_per_sec > 0 THEN clean_tok_per_sec ELSE NULL END),
                avg(CASE WHEN status_code = 200 AND max_chunk_gap_ms > 0 THEN max_chunk_gap_ms ELSE NULL END)
            FROM request_analytics
            "#,
        )?;

        let row = stmt.query_row([], |r| {
            let total_req: i64 = r.get(0).unwrap_or(0);
            let avg_ttft: f64 = r.get(1).unwrap_or(0.0);
            let avg_thinking: f64 = r.get(2).unwrap_or(0.0);
            let avg_gen: f64 = r.get(3).unwrap_or(0.0);
            let avg_clean_tps: f64 = r.get(4).unwrap_or(0.0);
            let avg_gap: f64 = r.get(5).unwrap_or(0.0);

            Ok(TelemetryAverages {
                total_requests: total_req as u64,
                avg_ttft_ms: (avg_ttft * 10.0).round() / 10.0,
                avg_thinking_time_ms: (avg_thinking * 10.0).round() / 10.0,
                avg_generation_time_ms: (avg_gen * 10.0).round() / 10.0,
                avg_clean_tok_per_sec: (avg_clean_tps * 10.0).round() / 10.0,
                avg_max_chunk_gap_ms: (avg_gap * 10.0).round() / 10.0,
            })
        })?;

        Ok(row)
    }

    pub fn get_summary(&self) -> Result<TelemetrySummary> {
        let conn = Connection::open(&self.db_path)?;
        let mut stmt = conn.prepare(
            r#"
            SELECT 
                count(*),
                sum(CASE WHEN status_code = 200 THEN 1 ELSE 0 END),
                sum(CASE WHEN status_code != 200 THEN 1 ELSE 0 END),
                sum(prompt_tokens),
                sum(completion_tokens),
                sum(cached_tokens),
                avg(CASE WHEN status_code = 200 THEN ttft_ms ELSE NULL END),
                avg(CASE WHEN status_code = 200 AND tok_per_sec > 0 AND tok_per_sec <= 500.0 THEN tok_per_sec ELSE NULL END)
            FROM request_analytics
            "#,
        )?;

        let summary_row = stmt.query_row([], |row| {
            let total_req: i64 = row.get(0).unwrap_or(0);
            let succ_req: i64 = row.get(1).unwrap_or(0);
            let fail_req: i64 = row.get(2).unwrap_or(0);
            let p_tokens: i64 = row.get(3).unwrap_or(0);
            let c_tokens: i64 = row.get(4).unwrap_or(0);
            let cached_tok: i64 = row.get(5).unwrap_or(0);
            let avg_ttft: f64 = row.get(6).unwrap_or(0.0);
            let avg_speed: f64 = row.get(7).unwrap_or(0.0);

            let hitrate = if (p_tokens + cached_tok) > 0 {
                (cached_tok as f64 / (p_tokens + cached_tok) as f64) * 100.0
            } else {
                0.0
            };

            Ok(TelemetrySummary {
                total_requests: total_req as u64,
                successful_requests: succ_req as u64,
                failed_requests: fail_req as u64,
                global_hitrate_percent: (hitrate * 10.0).round() / 10.0,
                total_tokens_saved_by_cache: cached_tok as u64,
                total_prompt_tokens: p_tokens as u64,
                total_completion_tokens: c_tokens as u64,
                avg_ttft_ms: (avg_ttft * 10.0).round() / 10.0,
                avg_tok_per_sec: ((avg_speed.min(500.0)) * 10.0).round() / 10.0,
                model_breakdown: Vec::new(),
                account_breakdown: Vec::new(),
            })
        })?;

        let mut model_stmt = conn.prepare(
            r#"
            SELECT model, count(*), sum(prompt_tokens), sum(completion_tokens), sum(cached_tokens), avg(CASE WHEN tok_per_sec > 0 AND tok_per_sec <= 500.0 THEN tok_per_sec ELSE NULL END)
            FROM request_analytics
            GROUP BY model
            ORDER BY count(*) DESC
            "#,
        )?;
        let model_rows = model_stmt.query_map([], |row| {
            Ok(ModelMetricSummary {
                model: row.get(0)?,
                request_count: row.get::<_, i64>(1)? as u64,
                prompt_tokens: row.get::<_, i64>(2)? as u64,
                completion_tokens: row.get::<_, i64>(3)? as u64,
                cached_tokens: row.get::<_, i64>(4)? as u64,
                avg_tok_per_sec: (row.get::<_, f64>(5).unwrap_or(0.0) * 10.0).round() / 10.0,
            })
        })?;

        let mut models = Vec::new();
        for m in model_rows {
            if let Ok(v) = m {
                models.push(v);
            }
        }

        let mut acc_stmt = conn.prepare(
            r#"
            SELECT account_id, account_email, count(*), sum(total_tokens)
            FROM request_analytics
            WHERE account_id IS NOT NULL
            GROUP BY account_id
            ORDER BY count(*) DESC
            "#,
        )?;
        let acc_rows = acc_stmt.query_map([], |row| {
            Ok(AccountMetricSummary {
                account_id: row.get(0)?,
                email: row.get(1)?,
                request_count: row.get::<_, i64>(2)? as u64,
                total_tokens: row.get::<_, i64>(3)? as u64,
            })
        })?;

        let mut accounts = Vec::new();
        for a in acc_rows {
            if let Ok(v) = a {
                accounts.push(v);
            }
        }

        let mut final_summary = summary_row;
        final_summary.model_breakdown = models;
        final_summary.account_breakdown = accounts;

        Ok(final_summary)
    }
}

pub fn now_epoch_and_string() -> (f64, String) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let secs = now as u64;
    let tm_hour = (secs / 3600) % 24;
    let tm_min = (secs / 60) % 60;
    let tm_sec = secs % 60;
    let str_val = format!("{:02}:{:02}:{:02} UTC", tm_hour, tm_min, tm_sec);
    (now, str_val)
}
