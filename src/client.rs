use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use reqwest::{Client, Proxy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamModelRaw {
    pub key: String,
    pub internal_id: Option<String>,
    pub display_name: Option<String>,
    pub disabled: bool,
}

const UPSTREAM_GENERATE_ENDPOINTS: &[&str] =
    &["https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent"];

const UPSTREAM_STREAM_ENDPOINTS: &[&str] =
    &["https://daily-cloudcode-pa.googleapis.com/v1internal:streamGenerateContent?alt=sse"];

#[derive(Clone)]
pub struct UpstreamClient {
    client: Client,
    direct_client: Client,
    generate_ep: String,
    stream_ep: String,
}

impl UpstreamClient {
    pub fn new(
        proxy_url: &str,
        connect_timeout_ms: u64,
        total_timeout_ms: u64,
        generate_ep_opt: &str,
        stream_ep_opt: &str,
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_millis(connect_timeout_ms))
            .timeout(Duration::from_millis(total_timeout_ms))
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(10)
            .user_agent("antigravity/hub/2.23.1 darwin/arm64");

        if !proxy_url.trim().is_empty() {
            let proxy = Proxy::all(proxy_url)?;
            builder = builder.proxy(proxy);
        }

        let direct_client = Client::builder()
            .connect_timeout(Duration::from_millis(2000))
            .timeout(Duration::from_millis(3500))
            .user_agent("antigravity/hub/2.23.1 darwin/arm64")
            .build()?;

        let gen_ep = if generate_ep_opt.trim().is_empty() {
            "https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent".to_string()
        } else {
            generate_ep_opt.to_string()
        };

        let str_ep = if stream_ep_opt.trim().is_empty() {
            "https://daily-cloudcode-pa.googleapis.com/v1internal:streamGenerateContent?alt=sse"
                .to_string()
        } else {
            stream_ep_opt.to_string()
        };

        Ok(Self {
            client: builder.build()?,
            direct_client,
            generate_ep: gen_ep,
            stream_ep: str_ep,
        })
    }

    /// Прогрев TCP/TLS HTTP/2 соединения к Cloudflare Worker
    pub async fn warmup_socket(&self) {
        let _ = self
            .client
            .request(reqwest::Method::OPTIONS, &self.stream_ep)
            .timeout(Duration::from_millis(2500))
            .send()
            .await;
    }

    pub async fn generate_content(&self, access_token: &str, payload: &Value) -> Result<Value> {
        let resp = match self
            .client
            .post(&self.generate_ep)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .header("User-Agent", "antigravity/hub/2.23.1 darwin/arm64")
            .header("x-client-name", "antigravity")
            .json(payload)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Network error on {}: {}",
                    self.generate_ep,
                    e
                ));
            }
        };

        if resp.status().is_success() {
            let json_val = resp.json::<Value>().await?;
            Ok(json_val)
        } else {
            let status = resp.status();
            let err_text = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "Endpoint {} returned HTTP {}: {}",
                self.generate_ep,
                status,
                err_text
            ))
        }
    }

    pub async fn stream_generate_content(
        &self,
        access_token: &str,
        payload: &Value,
    ) -> Result<impl Stream<Item = reqwest::Result<Bytes>>> {
        let resp = match self
            .client
            .post(&self.stream_ep)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .header("User-Agent", "antigravity/hub/2.23.1 darwin/arm64")
            .header("x-client-name", "antigravity")
            .json(payload)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Stream network error on {}: {}",
                    self.stream_ep,
                    e
                ));
            }
        };

        if resp.status().is_success() {
            Ok(resp.bytes_stream())
        } else {
            let status = resp.status();
            let err_text = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "Stream {} returned HTTP {}: {}",
                self.stream_ep,
                status,
                err_text
            ))
        }
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    pub fn direct(&self) -> &Client {
        &self.client
    }

    pub async fn fetch_models(&self, access_token: &str) -> Result<Vec<String>> {
        let endpoints = &[
            "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
            "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels",
            "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary",
            "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:retrieveUserQuotaSummary",
        ];

        let mut models = Vec::new();

        for ep in endpoints {
            let resp = match self
                .client
                .post(*ep)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .header("User-Agent", "antigravity/hub/2.23.1 darwin/arm64")
                .header("x-client-name", "antigravity")
                .json(&serde_json::json!({}))
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };

            if resp.status().is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    // 1. Прямой объект или массив моделей
                    if let Some(map) = json.get("models").and_then(|m| m.as_object()) {
                        for (key, val) in map {
                            let name = val
                                .get("name")
                                .or_else(|| val.get("id"))
                                .or_else(|| val.get("modelId"))
                                .and_then(|v| v.as_str())
                                .unwrap_or(key.as_str());
                            let clean = name.strip_prefix("models/").unwrap_or(name).to_string();
                            if !models.contains(&clean) {
                                models.push(clean);
                            }
                        }
                    } else if let Some(list) = json.get("models").and_then(|m| m.as_array()) {
                        for item in list {
                            if let Some(name) = item
                                .get("name")
                                .or_else(|| item.get("id"))
                                .or_else(|| item.get("modelId"))
                                .and_then(|v| v.as_str())
                            {
                                let clean =
                                    name.strip_prefix("models/").unwrap_or(name).to_string();
                                if !models.contains(&clean) {
                                    models.push(clean);
                                }
                            }
                        }
                    }
                    // 2. Модели из групп и сводки квот
                    if let Some(groups) = json.get("groups").and_then(|g| g.as_array()) {
                        for g in groups {
                            if let Some(buckets) = g.get("buckets").and_then(|b| b.as_array()) {
                                for b in buckets {
                                    if let Some(m_id) = b
                                        .get("modelId")
                                        .or_else(|| b.get("model"))
                                        .or_else(|| b.get("name"))
                                        .and_then(|v| v.as_str())
                                    {
                                        let clean = m_id
                                            .strip_prefix("models/")
                                            .unwrap_or(m_id)
                                            .to_string();
                                        if !models.contains(&clean) {
                                            models.push(clean);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // 3. Специальные массивы моделей из метаданных Antigravity
                    for field in &["imageGenerationModelIds", "commandModelIds", "tabModelIds"] {
                        if let Some(arr) = json.get(*field).and_then(|a| a.as_array()) {
                            for item in arr {
                                if let Some(m_id) = item.as_str() {
                                    let clean =
                                        m_id.strip_prefix("models/").unwrap_or(m_id).to_string();
                                    if !models.contains(&clean) {
                                        models.push(clean);
                                    }
                                }
                            }
                        }
                    }

                    // 4. Автогенерация псевдонимов для визуальных моделей (Nano Banana)
                    let current = models.clone();
                    for m in &current {
                        if m.contains("image") || m.contains("banana") {
                            for nb_alias in &["nano-banana", "nano-banana-pro", "nano-banana-2"] {
                                let s = nb_alias.to_string();
                                if !models.contains(&s) {
                                    models.push(s);
                                }
                            }
                        }
                        if m.ends_with("-tiered") {
                            let base = m.trim_end_matches("-tiered");
                            for suffix in &["", "-high", "-medium", "-low"] {
                                let alias = format!("{}{}", base, suffix);
                                if !models.contains(&alias) {
                                    models.push(alias);
                                }
                            }
                        }
                    }

                    if !models.is_empty() {
                        return Ok(models);
                    }
                }
            }
        }

        // Дополнительная проверка моделей через generativelanguage v1beta
        if let Ok(resp) = self
            .client
            .get("https://generativelanguage.googleapis.com/v1beta/models")
            .header("Authorization", format!("Bearer {}", access_token))
            .send()
            .await
        {
            if resp.status().is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    if let Some(list) = json.get("models").and_then(|m| m.as_array()) {
                        for item in list {
                            if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                                let clean =
                                    name.strip_prefix("models/").unwrap_or(name).to_string();
                                if !models.contains(&clean) {
                                    models.push(clean);
                                }
                            }
                        }
                    }
                }
            }
        }

        if !models.is_empty() {
            Ok(models)
        } else {
            Err(anyhow::anyhow!("No models returned from upstream"))
        }
    }

    pub async fn fetch_raw_models(&self, access_token: &str) -> Result<Vec<UpstreamModelRaw>> {
        let endpoints = &[
            "https://cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
            "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
            "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels",
        ];

        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for ep in endpoints {
            let resp = match self
                .client
                .post(*ep)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .header("User-Agent", "antigravity/hub/2.23.1 darwin/arm64")
                .header("x-client-name", "antigravity")
                .json(&serde_json::json!({}))
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };

            if resp.status().is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    if let Some(map) = json.get("models").and_then(|m| m.as_object()) {
                        for (key, val) in map {
                            let key_clean = key.strip_prefix("models/").unwrap_or(key).to_string();
                            if !seen.insert(key_clean.clone()) {
                                continue;
                            }
                            let internal_id = val.get("model").and_then(|v| v.as_str()).map(|s| s.to_string());
                            let display_name = val.get("displayName").and_then(|v| v.as_str()).map(|s| s.to_string());
                            let disabled = val.get("disabled").and_then(|v| v.as_bool()).unwrap_or(false);

                            results.push(UpstreamModelRaw {
                                key: key_clean,
                                internal_id,
                                display_name,
                                disabled,
                            });
                        }
                    }
                }
            }
            if !results.is_empty() {
                break;
            }
        }

        if !results.is_empty() {
            Ok(results)
        } else {
            Err(anyhow::anyhow!("Failed to fetch raw models from upstream"))
        }
    }

    pub async fn ensure_code_assist_onboarding(&self, access_token: &str) -> Result<String> {
        let cc_ua = "antigravity/hub/2.9.1 darwin/arm64 google-api-nodejs-client/10.3.0";
        let cc_api_client = "gl-node/22.21.1";

        // 1. Первоначальный вызов loadCodeAssist
        let load_ep = "https://daily-cloudcode-pa.googleapis.com/v1internal:loadCodeAssist";
        let load_payload = serde_json::json!({
            "metadata": { "ideType": "ANTIGRAVITY" }
        });

        for c in &[&self.client, &self.direct_client] {
            if let Ok(r) = c
                .post(load_ep)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .header("Accept", "*/*")
                .header("User-Agent", cc_ua)
                .header("X-Goog-Api-Client", cc_api_client)
                .json(&load_payload)
                .send()
                .await
            {
                if r.status().is_success() {
                    if let Ok(data) = r.json::<Value>().await {
                        if let Some(pid) = extract_project_id(&data) {
                            return Ok(pid);
                        }
                    }
                }
            }
        }

        // 2. При отсутствии проекта — онбординг пользователя (создание проекта standard-tier)
        let onboard_ep = "https://daily-cloudcode-pa.googleapis.com/v1internal:onboardUser";
        for tier in &["standard-tier", "free-tier"] {
            let onboard_payload = serde_json::json!({
                "tier_id": tier,
                "metadata": {
                    "ide_type": "ANTIGRAVITY",
                    "ide_version": "2.9.1",
                    "ide_name": "antigravity"
                }
            });

            for c in &[&self.client, &self.direct_client] {
                if let Ok(r) = c
                    .post(onboard_ep)
                    .header("Authorization", format!("Bearer {}", access_token))
                    .header("Content-Type", "application/json")
                    .header("Accept", "*/*")
                    .header("User-Agent", cc_ua)
                    .header("X-Goog-Api-Client", cc_api_client)
                    .json(&onboard_payload)
                    .send()
                    .await
                {
                    if r.status().is_success() {
                        if let Ok(data) = r.json::<Value>().await {
                            if let Some(pid) = extract_project_id(&data) {
                                return Ok(pid);
                            }
                        }
                    }
                }
            }
        }

        Ok("antigravity-default".to_string())
    }
}

fn extract_project_id(v: &Value) -> Option<String> {
    if let Some(p) = v
        .pointer("/cloudaicompanionProject/projectId")
        .and_then(|s| s.as_str())
    {
        return Some(p.to_string());
    }
    if let Some(p) = v.get("projectId").and_then(|s| s.as_str()) {
        return Some(p.to_string());
    }
    if let Some(p) = v.pointer("/project/id").and_then(|s| s.as_str()) {
        return Some(p.to_string());
    }
    if let Some(p) = v
        .pointer("/response/cloudaicompanionProject/projectId")
        .and_then(|s| s.as_str())
    {
        return Some(p.to_string());
    }
    None
}
