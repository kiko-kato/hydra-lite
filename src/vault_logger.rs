use crate::config::VaultConfig;
use age::x25519::Recipient;
use anyhow::{Context, Result};
use chrono::Utc;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};

pub const VAULT_MAGIC: &[u8; 4] = b"HVL1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub cached_tokens: u32,
    pub total_tokens: u32,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultEvent {
    pub timestamp: String,
    pub endpoint: String,
    pub api_key_name: String,
    pub model: String,
    pub stream: bool,
    pub status_code: u16,
    pub request_payload: Value,
    pub response_payload: Value,
    pub usage: Option<VaultUsage>,
}

#[derive(Clone)]
pub struct VaultLogger {
    tx: Option<Sender<VaultEvent>>,
}

impl VaultLogger {
    pub fn disabled() -> Self {
        Self { tx: None }
    }

    pub fn init(cfg: &VaultConfig) -> Result<Self> {
        if !cfg.enabled || cfg.public_key.trim().is_empty() {
            info!("[VAULT] Zero-Knowledge Vault Logger is disabled");
            return Ok(Self::disabled());
        }

        let pubkey_str = cfg.public_key.trim().to_string();
        let recipient = Recipient::from_str(&pubkey_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse age recipient public key ({}): {}", pubkey_str, e))?;

        let vault_dir = PathBuf::from(&cfg.vault_dir);
        std::fs::create_dir_all(&vault_dir)
            .with_context(|| format!("Failed to create vault directory at {:?}", vault_dir))?;

        info!(
            "[VAULT] Initialized Zero-Knowledge Vault Logger (dir: {:?}, pubkey: {}...{})",
            vault_dir,
            &pubkey_str.chars().take(12).collect::<String>(),
            &pubkey_str.chars().rev().take(6).collect::<String>().chars().rev().collect::<String>()
        );

        let (tx, mut rx) = channel::<VaultEvent>(64);
        let dir_clone = vault_dir.clone();
        let recipient_box = Arc::new(recipient);

        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let rec_arc = Arc::clone(&recipient_box);
                let dir = dir_clone.clone();

                let write_res = tokio::task::spawn_blocking(move || {
                    write_vault_event(&dir, &rec_arc, &event)
                })
                .await;

                match write_res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        error!("[VAULT:ERROR] Failed to write vault record: {}", e);
                    }
                    Err(join_err) => {
                        error!("[VAULT:ERROR] Worker panic: {}", join_err);
                    }
                }
            }
            info!("[VAULT] Worker receiver closed.");
        });

        Ok(Self { tx: Some(tx) })
    }

    pub fn log(&self, event: VaultEvent) {
        if let Some(ref tx) = self.tx {
            if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(event) {
                warn!("[VAULT:DROP] Vault channel buffer full (64). Event dropped to avoid latency.");
            }
        }
    }
}

fn write_vault_event(
    vault_dir: &Path,
    recipient: &Recipient,
    event: &VaultEvent,
) -> Result<()> {
    let raw_json = serde_json::to_vec(event)?;
    let compressed = zstd::encode_all(&raw_json[..], 3)?;

    let encryptor = age::Encryptor::with_recipients(vec![Box::new(recipient.clone())])
        .ok_or_else(|| anyhow::anyhow!("Failed to create age Encryptor"))?;

    let mut ciphertext = Vec::new();
    let mut writer = encryptor.wrap_output(&mut ciphertext)?;
    writer.write_all(&compressed)?;
    writer.finish()?;

    let today = Utc::now().format("%Y-%m-%d").to_string();
    let file_path = vault_dir.join(format!("{}.vault.age", today));

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .with_context(|| format!("Failed to open {:?}", file_path))?;

    let len = ciphertext.len() as u32;
    file.write_all(VAULT_MAGIC)?;
    file.write_all(&len.to_be_bytes())?;
    file.write_all(&ciphertext)?;
    file.flush()?;

    Ok(())
}

pub fn decrypt_vault_records(
    data: &[u8],
    identity_str: &str,
) -> Result<Vec<VaultEvent>> {
    let identity = age::x25519::Identity::from_str(identity_str.trim())
        .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;

    let mut cursor = 0;
    let mut events = Vec::new();

    while cursor + 8 <= data.len() {
        if &data[cursor..cursor + 4] != VAULT_MAGIC {
            anyhow::bail!("Invalid magic header at offset {}", cursor);
        }
        cursor += 4;

        let len = u32::from_be_bytes(data[cursor..cursor + 4].try_into()?) as usize;
        cursor += 4;

        if cursor + len > data.len() {
            anyhow::bail!("Unexpected EOF in vault record at offset {}", cursor);
        }

        let chunk = &data[cursor..cursor + len];
        cursor += len;

        let decryptor = match age::Decryptor::new(chunk)? {
            age::Decryptor::Recipients(d) => d,
            _ => anyhow::bail!("Unexpected decryptor type"),
        };

        let mut decrypted_zstd = Vec::new();
        let mut reader = decryptor.decrypt(std::iter::once(&identity as &dyn age::Identity))?;
        reader.read_to_end(&mut decrypted_zstd)?;

        let decompressed = zstd::decode_all(&decrypted_zstd[..])?;
        let event: VaultEvent = serde_json::from_slice(&decompressed)?;
        events.push(event);
    }

    Ok(events)
}
