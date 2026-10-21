use anyhow::Result;
use reqwest::Client;
use serde_json::Value;

const UPSTREAM_QUOTA_ENDPOINTS: &[&str] = &[
    "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary",
    "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:retrieveUserQuotaSummary",
];

pub struct QuotaSummary {
    pub raw_json: String,
    pub gemini_fraction: f64,
    pub claude_fraction: f64,
}

pub async fn fetch_quota_summary(client: &Client, access_token: &str) -> Result<QuotaSummary> {
    let mut last_err = anyhow::anyhow!("All quota endpoints failed");

    for ep in UPSTREAM_QUOTA_ENDPOINTS {
        let resp = match client
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
            Err(e) => {
                last_err = anyhow::anyhow!("Endpoint {} network error: {}", ep, e);
                continue;
            }
        };

        if resp.status().is_success() {
            let json: Value = resp.json().await?;
            let mut gemini_frac: f64 = 1.0;
            let mut claude_frac: f64 = 1.0;

            if let Some(groups) = json.get("groups").and_then(|g| g.as_array()) {
                for group in groups {
                    let name = group
                        .get("displayName")
                        .and_then(|n| n.as_str())
                        .unwrap_or("");
                    if let Some(buckets) = group.get("buckets").and_then(|b| b.as_array()) {
                        for bucket in buckets {
                            let disabled = bucket
                                .get("disabled")
                                .and_then(|d| d.as_bool())
                                .unwrap_or(false);
                            let rem = bucket
                                .get("remainingFraction")
                                .and_then(|r| r.as_f64())
                                .unwrap_or(1.0);
                            let frac = if disabled { 0.0 } else { rem };

                            if name == "Gemini Models" {
                                gemini_frac = gemini_frac.min(frac);
                            } else if name == "Claude and GPT models" {
                                claude_frac = claude_frac.min(frac);
                            }
                        }
                    }
                }
            }

            return Ok(QuotaSummary {
                raw_json: serde_json::to_string(&json)?,
                gemini_fraction: gemini_frac,
                claude_fraction: claude_frac,
            });
        } else {
            last_err = anyhow::anyhow!("Endpoint {} returned HTTP {}", ep, resp.status());
        }
    }

    Err(last_err)
}
