use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub oauth: OAuthConfig,
    pub upstream: UpstreamConfig,
    pub quota: QuotaConfig,
    #[serde(default)]
    pub signatures: SignaturesConfig,
    #[serde(default)]
    pub generation_defaults: GenerationDefaultsConfig,
    #[serde(default)]
    pub caching: CachingConfig,
    pub database: DatabaseConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    pub api_key: String,
    #[serde(default = "default_body_limit")]
    pub request_body_limit_mb: usize,
    #[serde(default = "default_true")]
    pub cors_allow_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    #[serde(default = "default_token_ep")]
    pub token_endpoint: String,
    #[serde(default = "default_refresh_skew")]
    pub token_refresh_skew_sec: f64,
    #[serde(default = "default_error_cooldown")]
    pub token_error_cooldown_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamConfig {
    #[serde(default)]
    pub proxy: String,
    pub connect_timeout_ms: u64,
    pub total_timeout_ms: u64,
    pub stream_idle_timeout_ms: u64,
    #[serde(default = "default_retries")]
    pub max_retries: usize,
    #[serde(default = "default_backoff_initial")]
    pub backoff_initial_ms: u64,
    #[serde(default = "default_backoff_mult")]
    pub backoff_multiplier: f64,
    #[serde(default)]
    pub generate_endpoints: Vec<String>,
    #[serde(default)]
    pub stream_endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaConfig {
    pub poll_interval_sec: u64,
    pub min_fraction: f64,
    pub rate_limit_cooldown_sec: f64,
    #[serde(default = "default_lb_strategy")]
    pub load_balancing_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SignaturesConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationDefaultsConfig {
    #[serde(default = "default_model")]
    pub default_model: String,
    #[serde(default = "default_max_tokens")]
    pub max_output_tokens: u32,
    #[serde(default = "default_temp")]
    pub default_temperature: f32,
    #[serde(default = "default_top_p")]
    pub top_p: f32,
    #[serde(default = "default_top_k")]
    pub top_k: u32,
}

impl Default for GenerationDefaultsConfig {
    fn default() -> Self {
        Self {
            default_model: default_model(),
            max_output_tokens: default_max_tokens(),
            default_temperature: default_temp(),
            top_p: default_top_p(),
            top_k: default_top_k(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub debug: bool,
    #[serde(default = "default_true")]
    pub hrw_affinity: bool,
    #[serde(default)]
    pub exact_cache: bool,
    #[serde(default = "default_true")]
    pub prefix_normalization: bool,
    #[serde(default = "default_true")]
    pub strip_ansi: bool,
    #[serde(default = "default_true")]
    pub cache_control_headers: bool,
}

impl Default for CachingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            debug: false,
            hrw_affinity: true,
            exact_cache: false,
            prefix_normalization: true,
            strip_ansi: true,
            cache_control_headers: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub path: String,
    #[serde(default = "default_logs_db_path")]
    pub logs_db_path: String,
    #[serde(default = "default_wal_checkpoint")]
    pub wal_autocheckpoint_pages: u32,
    #[serde(default = "default_busy_timeout")]
    pub busy_timeout_ms: u64,
}

// Defaults
fn default_true() -> bool {
    true
}
fn default_body_limit() -> usize {
    64
}
fn default_token_ep() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}
fn default_refresh_skew() -> f64 {
    300.0
}
fn default_error_cooldown() -> f64 {
    60.0
}
fn default_retries() -> usize {
    3
}
fn default_backoff_initial() -> u64 {
    500
}
fn default_backoff_mult() -> f64 {
    2.0
}
fn default_lb_strategy() -> String {
    "fraction_desc".to_string()
}
fn default_model() -> String {
    "gemini-3.8-flash-high".to_string()
}
fn default_max_tokens() -> u32 {
    65536
}
fn default_temp() -> f32 {
    0.7
}
fn default_top_p() -> f32 {
    0.95
}
fn default_top_k() -> u32 {
    40
}
fn default_logs_db_path() -> String {
    "data/logs.db".to_string()
}
fn default_wal_checkpoint() -> u32 {
    1000
}
fn default_busy_timeout() -> u64 {
    5000
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}
