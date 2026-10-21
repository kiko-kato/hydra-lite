use log::info;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone)]
struct FailedEvent {
    signature: String,
    raw_error: String,
    timestamp: Instant,
}

#[derive(Debug, Clone)]
struct PatchEvent {
    path: String,
    patch_text: String,
    timestamp: Instant,
}

pub struct AutoLearner {
    last_failure: Mutex<Option<FailedEvent>>,
    last_patch: Mutex<Option<PatchEvent>>,
}

impl AutoLearner {
    pub fn new() -> Self {
        Self {
            last_failure: Mutex::new(None),
            last_patch: Mutex::new(None),
        }
    }

    pub fn record_failure(&self, signature: &str, raw_error: &str) {
        let mut fail_lock = self.last_failure.lock().unwrap();
        *fail_lock = Some(FailedEvent {
            signature: signature.to_string(),
            raw_error: raw_error.to_string(),
            timestamp: Instant::now(),
        });
    }

    pub fn record_patch(&self, path: &str, patch_text: &str) {
        let mut patch_lock = self.last_patch.lock().unwrap();
        *patch_lock = Some(PatchEvent {
            path: path.to_string(),
            patch_text: patch_text.to_string(),
            timestamp: Instant::now(),
        });
    }

    pub fn record_success(&self, db: &Mutex<Connection>) -> Option<String> {
        let mut fail_lock = self.last_failure.lock().unwrap();
        let mut patch_lock = self.last_patch.lock().unwrap();

        let fail = fail_lock.take()?;
        let patch = patch_lock.take()?;

        // Only pair if patch was applied after failure and within 5 minutes
        if patch.timestamp < fail.timestamp || fail.timestamp.elapsed().as_secs() > 300 {
            return None;
        }

        let hash_input = format!("{}:{}", fail.signature, patch.path);
        let mut hasher = Sha256::new();
        hasher.update(hash_input.as_bytes());
        let res = hasher.finalize();
        let hash: String = res.iter().map(|b| format!("{:02x}", b)).collect();
        let issue_id = format!("local-learn-{}", &hash[..8]);

        let title = format!("Auto-Learned Fix: {}", fail.signature);

        let solution = format!(
            "Target: {}\nApplied Patch:\n```\n{}\n```",
            patch.path, patch.patch_text
        );

        let conn = db.lock().ok()?;
        let res = conn.execute(
            "INSERT INTO problems (issue_id, title, error_signature, problem_text, solution_text, source, external_url, hit_count, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'local_verified', 'auto_learned', 1, datetime('now'), datetime('now'))
             ON CONFLICT(issue_id) DO UPDATE SET hit_count = hit_count + 1, updated_at = datetime('now')",
            params![issue_id, title, fail.signature, fail.raw_error, solution],
        );

        if res.is_ok() {
            info!(
                "[ANTIDOTE:LEARN] Successfully auto-learned and persisted fix for '{}' -> {}",
                fail.signature, issue_id
            );
            Some(issue_id)
        } else {
            None
        }
    }
}
