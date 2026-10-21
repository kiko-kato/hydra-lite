use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

const TOKEN_EP: &str = "https://oauth2.googleapis.com/token";

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

pub async fn refresh_access_token(
    client: &Client,
    refresh_token: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<(String, f64)> {
    let params = [
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
    ];

    let mut last_err = anyhow::anyhow!("Token refresh failed");

    for attempt in 0..3 {
        let resp_res = client
            .post(TOKEN_EP)
            .form(&params)
            .header("User-Agent", "antigravity/hub/2.23.1 darwin/arm64")
            .send()
            .await;

        match resp_res {
            Ok(resp) => {
                if resp.status().is_success() {
                    let token_data = resp.json::<TokenResponse>().await?;
                    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
                    let expires_at = now + token_data.expires_in.unwrap_or(3599) as f64;
                    return Ok((token_data.access_token, expires_at));
                } else {
                    let status = resp.status();
                    let err_text = resp.text().await.unwrap_or_default();
                    last_err = anyhow::anyhow!("OAuth HTTP {}: {}", status, err_text);
                }
            }
            Err(e) => {
                last_err = anyhow::anyhow!("OAuth network error (attempt {}): {}", attempt, e);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }

    Err(last_err)
}
