use crate::signatures::SignatureStore;
use crate::AppState;
use async_stream::stream;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicRequest {
    pub model: String,
    pub messages: Vec<Value>,
    #[serde(default)]
    pub system: Option<Value>,
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stream: bool,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub top_k: Option<u32>,
    pub tools: Option<Vec<Value>>,
    pub tool_choice: Option<Value>,
    pub thinking: Option<Value>,
}

pub fn convert_anthropic_to_gemini(
    req: &AnthropicRequest,
    _signatures: &SignatureStore,
) -> (Value, Option<Value>, Option<Value>) {
    // 1. System instruction
    let mut sys_parts = Vec::new();
    if let Some(ref sys) = req.system {
        if let Some(s) = sys.as_str() {
            if !s.trim().is_empty() {
                sys_parts.push(json!({ "text": s.trim() }));
            }
        } else if let Some(arr) = sys.as_array() {
            for item in arr {
                if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                    if !text.trim().is_empty() {
                        sys_parts.push(json!({ "text": text.trim() }));
                    }
                }
            }
        }
    }

    let sys_inst = if sys_parts.is_empty() {
        None
    } else {
        Some(json!({ "parts": sys_parts }))
    };

    // 2. Tools
    let mut func_decls = Vec::new();
    if let Some(ref tools) = req.tools {
        for t in tools {
            if let Some(obj) = t.as_object() {
                let name = obj.get("name").and_then(|v| v.as_str()).unwrap_or_default();
                let desc = obj
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let params = obj
                    .get("input_schema")
                    .cloned()
                    .unwrap_or(json!({ "type": "object", "properties": {} }));

                func_decls.push(json!({
                    "name": name,
                    "description": desc,
                    "parameters": params,
                }));
            }
        }
    }

    let tools_val = if func_decls.is_empty() {
        None
    } else {
        Some(json!([{ "functionDeclarations": func_decls }]))
    };

    // 3. Contents
    let mut contents = Vec::new();
    for m in &req.messages {
        let role_raw = m.get("role").and_then(|r| r.as_str()).unwrap_or("user");
        let gemini_role = match role_raw {
            "assistant" => "model",
            _ => "user",
        };

        let mut parts = Vec::new();

        if let Some(text) = m.get("content").and_then(|c| c.as_str()) {
            if !text.trim().is_empty() {
                parts.push(json!({ "text": text }));
            }
        } else if let Some(blocks) = m.get("content").and_then(|c| c.as_array()) {
            for block in blocks {
                let b_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match b_type {
                    "text" => {
                        if let Some(txt) = block.get("text").and_then(|t| t.as_str()) {
                            if !txt.is_empty() {
                                parts.push(json!({ "text": txt }));
                            }
                        }
                    }
                    "image" => {
                        if let Some(source) = block.get("source") {
                            let media_type = source
                                .get("media_type")
                                .and_then(|m| m.as_str())
                                .unwrap_or("image/png");
                            let data = source.get("data").and_then(|d| d.as_str()).unwrap_or("");
                            if !data.is_empty() {
                                parts.push(json!({
                                    "inlineData": {
                                        "mimeType": media_type,
                                        "data": data,
                                    }
                                }));
                            }
                        }
                    }
                    "tool_use" => {
                        let name = block
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or_default();
                        let input = block.get("input").cloned().unwrap_or(json!({}));
                        parts.push(json!({
                            "functionCall": {
                                "name": name,
                                "args": input,
                            }
                        }));
                    }
                    "tool_result" => {
                        let id = block
                            .get("tool_use_id")
                            .and_then(|i| i.as_str())
                            .unwrap_or("tool_call");
                        let res_text = if let Some(content_str) =
                            block.get("content").and_then(|c| c.as_str())
                        {
                            content_str.to_string()
                        } else {
                            block.get("content").unwrap_or(&json!("")).to_string()
                        };
                        parts.push(json!({
                            "functionResponse": {
                                "name": id,
                                "response": { "result": res_text }
                            }
                        }));
                    }
                    _ => {}
                }
            }
        }

        if parts.is_empty() {
            parts.push(json!({ "text": "" }));
        }

        contents.push(json!({
            "role": gemini_role,
            "parts": parts,
        }));
    }

    (json!(contents), sys_inst, tools_val)
}

pub fn parse_gemini_to_anthropic(gemini_resp: &Value, model: &str) -> anyhow::Result<Value> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let msg_id = format!("msg_antigravity_{}", now);

    let candidates = gemini_resp
        .get("response")
        .unwrap_or(gemini_resp)
        .get("candidates")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();

    let first = candidates.first();
    let parts = first
        .and_then(|c| c.get("content"))
        .and_then(|c| c.get("parts"))
        .and_then(|p| p.as_array())
        .cloned()
        .unwrap_or_default();

    let mut content_blocks = Vec::new();
    let mut has_tool_use = false;

    for part in &parts {
        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
            if !text.is_empty() {
                content_blocks.push(json!({
                    "type": "text",
                    "text": text,
                }));
            }
        } else if let Some(fc) = part.get("functionCall") {
            has_tool_use = true;
            let name = fc.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = fc.get("args").cloned().unwrap_or(json!({}));
            let call_id = format!("toolu_{}", now);
            content_blocks.push(json!({
                "type": "tool_use",
                "id": call_id,
                "name": name,
                "input": args,
            }));
        }
    }

    if content_blocks.is_empty() {
        content_blocks.push(json!({
            "type": "text",
            "text": "",
        }));
    }

    let usage = gemini_resp
        .get("response")
        .unwrap_or(gemini_resp)
        .get("usageMetadata")
        .cloned()
        .unwrap_or(json!({}));

    let prompt_tokens = usage
        .get("promptTokenCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = usage
        .get("candidatesTokenCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let stop_reason = if has_tool_use { "tool_use" } else { "end_turn" };

    Ok(json!({
        "id": msg_id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content_blocks,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {
            "input_tokens": prompt_tokens,
            "output_tokens": output_tokens,
        }
    }))
}

pub fn transform_gemini_anthropic_sse_stream(
    mut stream: impl Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
    model: String,
    signatures: SignatureStore,
) -> impl Stream<Item = Result<Bytes, axum::Error>> {
    stream! {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let msg_id = format!("msg_antigravity_{}", now);

        // 1. message_start
        let msg_start = json!({
            "type": "message_start",
            "message": {
                "id": msg_id,
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": null,
                "stop_sequence": null,
                "usage": { "input_tokens": 0, "output_tokens": 0 }
            }
        });
        yield Ok(Bytes::from(format!("event: message_start\ndata: {}\n\n", msg_start)));

        // 2. content_block_start
        let block_start = json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": { "type": "text", "text": "" }
        });
        yield Ok(Bytes::from(format!("event: content_block_start\ndata: {}\n\n", block_start)));

        let mut buffer = Vec::new();

        while let Some(chunk_res) = stream.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(_) => break,
            };
            buffer.extend_from_slice(&chunk);

            while let Some(nl_idx) = buffer.iter().position(|&b| b == b'\n') {
                let line_bytes: Vec<u8> = buffer.drain(..=nl_idx).collect();
                let line_str = match std::str::from_utf8(&line_bytes) {
                    Ok(s) => s.trim(),
                    Err(e) => {
                        if e.error_len().is_none() {
                            buffer.splice(0..0, line_bytes);
                            break;
                        } else {
                            continue;
                        }
                    }
                };

                if !line_str.starts_with("data:") {
                    continue;
                }

                let data_raw = line_str.trim_start_matches("data:").trim();
                if data_raw.is_empty() || data_raw == "[DONE]" {
                    continue;
                }

                if let Ok(ev_json) = serde_json::from_str::<Value>(data_raw) {
                    let resp_obj = ev_json.get("response").unwrap_or(&ev_json);
                    let (thought_part, text_part, _tools, _finish) = crate::sse::extract_parts(resp_obj, &signatures);
                    if let Some(th) = thought_part {
                        if !th.is_empty() {
                            let delta = json!({
                                "type": "content_block_delta",
                                "index": 0,
                                "delta": {
                                    "type": "thinking_delta",
                                    "thinking": th,
                                }
                            });
                            yield Ok(Bytes::from(format!("event: content_block_delta\ndata: {}\n\n", delta)));
                        }
                    }
                    if let Some(text) = text_part {
                        if !text.is_empty() {
                            let delta = json!({
                                "type": "content_block_delta",
                                "index": 0,
                                "delta": {
                                    "type": "text_delta",
                                    "text": text,
                                }
                            });
                            yield Ok(Bytes::from(format!("event: content_block_delta\ndata: {}\n\n", delta)));
                        }
                    }
                }
            }
        }

        // 3. content_block_stop
        let block_stop = json!({
            "type": "content_block_stop",
            "index": 0
        });
        yield Ok(Bytes::from(format!("event: content_block_stop\ndata: {}\n\n", block_stop)));

        // 4. message_delta
        let msg_delta = json!({
            "type": "message_delta",
            "delta": { "stop_reason": "end_turn", "stop_sequence": null },
            "usage": { "output_tokens": 0 }
        });
        yield Ok(Bytes::from(format!("event: message_delta\ndata: {}\n\n", msg_delta)));

        // 5. message_stop
        let msg_stop = json!({ "type": "message_stop" });
        yield Ok(Bytes::from(format!("event: message_stop\ndata: {}\n\n", msg_stop)));
    }
}

pub(crate) async fn handle_anthropic_messages(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(mut req): Json<AnthropicRequest>,
) -> Response {
    // 1. Авторизация
    let auth_key_name = match crate::authenticate_request(&headers, &state).await {
        crate::AuthResult::Allowed { key_name, .. } => key_name,
        crate::AuthResult::Unauthorized(msg) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "type": "error",
                    "error": { "type": "authentication_error", "message": msg }
                })),
            )
                .into_response();
        }
        crate::AuthResult::Forbidden(msg) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "type": "error",
                    "error": { "type": "permission_error", "message": msg }
                })),
            )
                .into_response();
        }
        crate::AuthResult::RateLimited(msg) => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "type": "error",
                    "error": { "type": "rate_limit_error", "message": msg }
                })),
            )
                .into_response();
        }
    };

    let session_key = auth_key_name.as_ref();

    // 1. Anchor Cache Stabilizer (freeze system prompt, move dynamic date/time to messages.last())
    crate::anchor_stabilizer::AnchorStabilizer::stabilize_anthropic(
        &mut req.system,
        &mut req.messages,
    );

    // 2. Anti-Loop Circuit Breaker
    state
        .circuit_breaker
        .inspect_and_trip_anthropic(session_key, &mut req.messages);

    // 3. Hydra Antidote Self-Healing Interceptor
    state
        .antidote
        .inspect_and_inject_anthropic(&mut req.messages)
        .await;

    // 2. Трансляция Anthropic -> Gemini CloudCode
    let (contents, sys_inst, tools) = convert_anthropic_to_gemini(&req, &state.signatures);
    let mut inner_req = json!({ "contents": contents });
    if let Some(sys) = sys_inst {
        inner_req["systemInstruction"] = sys;
    }
    if let Some(t) = tools {
        inner_req["tools"] = t;
    }

    let mut gen_config = json!({
        "temperature": req.temperature.unwrap_or(0.7),
        "topP": req.top_p.unwrap_or(0.95),
    });
    if let Some(max_t) = req.max_tokens {
        gen_config["maxOutputTokens"] = json!(max_t);
    }
    if req.model.contains("thinking") {
        gen_config["thinkingConfig"] = json!({
            "thinkingBudget": 65535,
            "includeThoughts": true
        });
    }
    inner_req["generationConfig"] = gen_config;

    let upstream_model = match req.model.as_str() {
        "nano-banana" | "nano-banana-pro" | "nano-banana-2" => "gemini-3.1-flash-image",
        "gemini-3.8-flash"
        | "gemini-3.8-flash-high"
        | "gemini-3.8-flash-medium"
        | "gemini-3.8-flash-low" => "gemini-3.8-flash-tiered",
        "gemini-2.5-flash-high" => "gemini-2.5-flash",
        m if m.contains("sonnet-5-5") || m.contains("sonnet-5.5") => "claude-sonnet-5-5",
        m if m.contains("opus-5-5") || m.contains("opus-5.5") => "claude-opus-5-5",
        m if m.contains("sonnet") => "claude-sonnet-4-6",
        m if m.contains("opus") => "claude-opus-4-6-thinking",
        m if m.contains("argon") => "gemini-4-argon",
        other => other,
    };

    let mut gemini_payload = json!({
        "project": "",
        "model": upstream_model,
        "request": inner_req,
        "userAgent": "antigravity/hub/2.23.1 darwin/arm64",
        "requestType": "agent",
        "requestId": format!("antigravity-{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis())
    });

    // Zero-loss нормализация и минификация контекста (экономия 25-35% токенов)
    crate::minifier::minify_gemini_payload(&mut gemini_payload);

    // 3. Выбор аккаунта (HRW affinity)
    let accounts = match state.db.load_accounts().await {
        Ok(a) => a,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "type": "error",
                    "error": { "type": "api_error", "message": format!("Database error: {}", e) }
                })),
            )
                .into_response();
        }
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let cfg = state.config.read().clone();
    let eligible: Vec<_> = accounts
        .into_iter()
        .filter(|a| {
            a.enabled && a.cooldown_until <= now && a.claude_remaining >= cfg.quota.min_fraction
        })
        .collect();

    if eligible.is_empty() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "type": "error",
                "error": { "type": "rate_limit_error", "message": "All accounts quota exhausted" }
            })),
        )
            .into_response();
    }

    // HRW affinity hashing
    let mut hasher = Sha256::new();
    hasher.update(req.model.as_bytes());
    if let Some(first_msg) = req.messages.first() {
        hasher.update(first_msg.to_string().as_bytes());
    }
    let affinity_hash = hasher.finalize();

    let mut ranked = eligible;
    ranked.sort_by(|a, b| {
        let mut h_a = Sha256::new();
        h_a.update(&affinity_hash);
        h_a.update(a.id.to_le_bytes());
        let score_a = h_a.finalize();

        let mut h_b = Sha256::new();
        h_b.update(&affinity_hash);
        h_b.update(b.id.to_le_bytes());
        let score_b = h_b.finalize();

        score_b.cmp(&score_a)
    });

    let chosen_acc = &ranked[0];
    let token = chosen_acc.access_token.clone().unwrap_or_default();

    // 4. Отправка (стриминг / синк)
    if req.stream {
        match state
            .client
            .stream_generate_content(&token, &gemini_payload)
            .await
        {
            Ok(stream) => {
                let transformed = transform_gemini_anthropic_sse_stream(
                    stream,
                    req.model.clone(),
                    state.signatures.clone(),
                );
                Response::builder()
                    .header("Content-Type", "text/event-stream")
                    .header("Cache-Control", "no-cache")
                    .header("Connection", "keep-alive")
                    .body(Body::from_stream(transformed))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "Stream error").into_response()
                    })
            }
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "type": "error",
                    "error": { "type": "api_error", "message": format!("Stream failed: {}", e) }
                })),
            )
                .into_response(),
        }
    } else {
        match state.client.generate_content(&token, &gemini_payload).await {
            Ok(resp) => match parse_gemini_to_anthropic(&resp, &req.model) {
                Ok(anthropic_msg) => Json(anthropic_msg).into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "type": "error",
                        "error": { "type": "api_error", "message": format!("Response translation failed: {}", e) }
                    })),
                ).into_response(),
            },
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "type": "error",
                    "error": { "type": "api_error", "message": format!("Upstream failed: {}", e) }
                })),
            ).into_response(),
        }
    }
}
