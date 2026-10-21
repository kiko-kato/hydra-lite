use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: i64,
    pub email: Option<String>,
    pub refresh_token: String,
    pub access_token: Option<String>,
    pub expires_at: f64,
    pub quota_json: Option<String>,
    pub gemini_remaining: f64,
    pub claude_remaining: f64,
    pub last_quota_check: f64,
    pub cooldown_until: f64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<serde_json::Value>,
    #[serde(default)]
    pub stream: bool,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub tools: Option<Vec<serde_json::Value>>,
    pub tool_choice: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelItem {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub owned_by: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelListResponse {
    pub object: &'static str,
    pub data: Vec<ModelItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: serde_json::Value,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens_details: Option<PromptTokensDetails>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PromptTokensDetails {
    pub cached_tokens: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: serde_json::Value,
    pub finish_reason: Option<String>,
}
