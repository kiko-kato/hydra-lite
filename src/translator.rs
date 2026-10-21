use crate::signatures::SignatureStore;
use crate::types::{
    ChatCompletionRequest, ChatCompletionResponse, Choice, PromptTokensDetails, Usage,
};
use regex::Regex;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

static ANSI_REGEX: OnceLock<Regex> = OnceLock::new();

pub fn strip_ansi(input: &str) -> String {
    let re = ANSI_REGEX.get_or_init(|| {
        Regex::new(r"(\x1b\[[0-9;?]*[a-zA-Z]|\x1b\].*?\x07|\x1b[PX^_].*?\x1b\\)").unwrap()
    });
    re.replace_all(input, "").to_string()
}

pub fn normalize_text(input: &str) -> String {
    let stripped = strip_ansi(input);
    let mut lines = Vec::new();
    let mut empty_streak = 0;
    for line in stripped.lines() {
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            empty_streak += 1;
            if empty_streak <= 2 {
                lines.push("");
            }
        } else {
            empty_streak = 0;
            lines.push(trimmed);
        }
    }
    lines.join("\n")
}

pub fn estimate_tokens(text: &str) -> usize {
    text.chars().count() / 3 + 1
}

fn parse_data_url(url: &str) -> Option<(String, String)> {
    if !url.starts_with("data:") {
        return None;
    }
    let rest = &url["data:".len()..];
    let (mime_part, b64_part) = rest.split_once(";base64,")?;
    let mime = if mime_part.is_empty() {
        "image/png"
    } else {
        mime_part
    };
    Some((mime.to_string(), b64_part.to_string()))
}

pub fn compute_exact_cache_key(req: &ChatCompletionRequest) -> String {
    let mut hasher = Sha256::new();
    hasher.update(req.model.as_bytes());
    hasher.update(b"|");

    for m in &req.messages {
        let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("");
        hasher.update(role.as_bytes());
        hasher.update(b":");
        if let Some(content) = m.get("content") {
            if let Some(s) = content.as_str() {
                hasher.update(strip_ansi(s).as_bytes());
            } else {
                hasher.update(content.to_string().as_bytes());
            }
        }
        if let Some(tc) = m.get("tool_calls") {
            hasher.update(tc.to_string().as_bytes());
        }
        hasher.update(b"\n");
    }

    if let Some(tools) = &req.tools {
        hasher.update(b"|tools:");
        hasher.update(serde_json::to_string(tools).unwrap_or_default().as_bytes());
    }

    let temp_str = req
        .temperature
        .map(|t| format!("{:.4}", t))
        .unwrap_or_else(|| "0.0".to_string());
    hasher.update(b"|temp:");
    hasher.update(temp_str.as_bytes());

    if let Some(mt) = req.max_tokens {
        hasher.update(format!("|max_tokens:{}", mt).as_bytes());
    }

    let out = hasher.finalize();
    out.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

pub fn compute_session_affinity_key(req: &ChatCompletionRequest) -> String {
    let mut hasher = Sha256::new();
    hasher.update(req.model.as_bytes());
    hasher.update(b"|");

    let mut count = 0;
    for m in &req.messages {
        let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("");
        if role == "system" {
            if let Some(c) = m.get("content").and_then(|c| c.as_str()) {
                hasher.update(b"sys:");
                hasher.update(normalize_text(c).as_bytes());
                hasher.update(b"\n");
            }
        } else if count < 1 {
            if let Some(c) = m.get("content").and_then(|c| c.as_str()) {
                hasher.update(role.as_bytes());
                hasher.update(b":");
                hasher.update(strip_ansi(c).as_bytes());
                hasher.update(b"\n");
                count += 1;
            }
        }
    }

    let out = hasher.finalize();
    out.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

pub fn sanitize_json_schema(val: &mut Value) {
    match val {
        Value::Object(map) => {
            if let Some(any_of) = map.remove("anyOf").or_else(|| map.remove("oneOf")) {
                if let Some(arr) = any_of.as_array() {
                    for item in arr {
                        if let Some(t) = item.get("type").and_then(|v| v.as_str()) {
                            if t != "null" {
                                map.insert("type".to_string(), json!(t));
                                if let Some(items) = item.get("items") {
                                    map.insert("items".to_string(), items.clone());
                                }
                                break;
                            }
                        }
                    }
                }
            }
            for (_, v) in map.iter_mut() {
                sanitize_json_schema(v);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                sanitize_json_schema(v);
            }
        }
        _ => {}
    }
}

pub fn convert_tools(tools_opt: &Option<Vec<Value>>) -> Option<Value> {
    let tools = tools_opt.as_ref()?;
    let mut decls = Vec::new();

    for t in tools {
        if let Some(t_obj) = t.as_object() {
            if t_obj.get("type").and_then(|v| v.as_str()) == Some("function") {
                if let Some(fn_obj) = t_obj.get("function").and_then(|f| f.as_object()) {
                    let mut decl = json!({
                        "name": fn_obj.get("name").unwrap_or(&Value::Null),
                        "description": fn_obj.get("description").unwrap_or(&json!("")),
                    });

                    let mut p_clean = if let Some(params) = fn_obj.get("parameters") {
                        if params.is_null() || params == &json!({}) {
                            json!({ "type": "OBJECT", "properties": {} })
                        } else {
                            params.clone()
                        }
                    } else {
                        json!({ "type": "OBJECT", "properties": {} })
                    };

                    sanitize_json_schema(&mut p_clean);

                    decl["parameters"] = p_clean;
                    decls.push(decl);
                }
            }
        }
    }

    if decls.is_empty() {
        None
    } else {
        Some(json!([{ "functionDeclarations": decls }]))
    }
}

pub fn convert_messages_to_gemini(
    req: &ChatCompletionRequest,
    signatures: &SignatureStore,
) -> (Value, Option<Value>) {
    let mut contents: Vec<Value> = Vec::new();
    let mut sys_parts = Vec::new();
    let mut call_id_queues: HashMap<String, std::collections::VecDeque<(String, String)>> =
        HashMap::new();
    let mut cid_seen_counts: HashMap<String, usize> = HashMap::new();

    for m in &req.messages {
        let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("user");
        let content_val = m.get("content").unwrap_or(&Value::Null);

        let mut text_str = String::new();
        let mut clean_blocks = Vec::new();

        if let Some(s) = content_val.as_str() {
            text_str = s.to_string();
        } else if let Some(arr) = content_val.as_array() {
            for block in arr {
                if let Some(t) = block.get("text").and_then(|x| x.as_str()) {
                    text_str.push_str(t);
                    text_str.push('\n');
                } else if let Some(s) = block.as_str() {
                    text_str.push_str(s);
                    text_str.push('\n');
                }

                // Если клиент прислал блок с cache_control - удаляем это поле перед отправкой в Google Protobuf
                if let Some(obj) = block.as_object() {
                    let mut cleaned_obj = obj.clone();
                    cleaned_obj.remove("cache_control");
                    clean_blocks.push(Value::Object(cleaned_obj));
                } else {
                    clean_blocks.push(block.clone());
                }
            }
        }

        match role {
            "system" => {
                if !text_str.is_empty() {
                    let norm = normalize_text(&text_str);
                    if !norm.is_empty() {
                        let part = json!({ "text": norm });
                        sys_parts.push(part);
                    }
                }
            }
            "user" => {
                let stripped = strip_ansi(&text_str);
                let mut parts = Vec::new();

                // Поддержка изображений: {"type": "image_url", "image_url": {"url": "data:image/png;base64,..."}}
                let source_blocks = if !clean_blocks.is_empty() {
                    clean_blocks
                } else {
                    content_val.as_array().cloned().unwrap_or_default()
                };

                for block in source_blocks {
                    if let Some(img_obj) = block.get("image_url") {
                        let url_str = img_obj.get("url").and_then(|u| u.as_str()).unwrap_or("");
                        if let Some((mime, b64_data)) = parse_data_url(url_str) {
                            parts.push(json!({
                                "inlineData": {
                                    "mimeType": mime,
                                    "data": b64_data
                                }
                            }));
                        }
                    }
                }

                if !stripped.is_empty() {
                    parts.push(json!({ "text": stripped }));
                } else if parts.is_empty() {
                    parts.push(json!({ "text": "" }));
                }

                let mut appended = false;
                if let Some(last) = contents.last_mut() {
                    if last.get("role").and_then(|r| r.as_str()) == Some("user") {
                        if let Some(parts_arr) =
                            last.get_mut("parts").and_then(|p| p.as_array_mut())
                        {
                            parts_arr.extend(parts.clone());
                            appended = true;
                        }
                    }
                }

                if !appended {
                    contents.push(json!({
                        "role": "user",
                        "parts": parts,
                    }));
                }
            }
            "assistant" => {
                let mut parts = Vec::new();
                let stripped = strip_ansi(&text_str);
                let trimmed = stripped.trim();
                let is_filter_blocked = trimmed.contains("This request was blocked by Gemini's filters");
                let is_empty_marker = trimmed.is_empty()
                    || trimmed == "(empty)"
                    || trimmed == "[response interrupted]"
                    || is_filter_blocked;

                let has_text = !trimmed.is_empty() && !is_empty_marker;
                if has_text {
                    parts.push(json!({ "text": stripped }));
                }

                let mut has_tools = false;
                if let Some(tcs) = m.get("tool_calls").and_then(|tc| tc.as_array()) {
                    for tc in tcs {
                        let cid = tc.get("id").and_then(|i| i.as_str()).unwrap_or("");
                        let fn_obj = tc.get("function").unwrap_or(&Value::Null);
                        let fname = fn_obj.get("name").and_then(|n| n.as_str()).unwrap_or("");
                        let args_val = fn_obj.get("arguments").unwrap_or(&Value::Null);

                        let parsed_args: Value = if let Some(s) = args_val.as_str() {
                            serde_json::from_str(s).unwrap_or(json!({}))
                        } else if args_val.is_object() {
                            args_val.clone()
                        } else {
                            json!({})
                        };

                        let unique_cid = if !cid.is_empty() {
                            let count = cid_seen_counts.entry(cid.to_string()).or_insert(0);
                            let ucid = if *count == 0 {
                                cid.to_string()
                            } else {
                                format!("{}_{}", cid, *count)
                            };
                            *count += 1;
                            if !fname.is_empty() {
                                call_id_queues
                                    .entry(cid.to_string())
                                    .or_default()
                                    .push_back((ucid.clone(), fname.to_string()));
                            }
                            ucid
                        } else {
                            String::new()
                        };

                        let mut fc_part = json!({
                            "functionCall": {
                                "name": fname,
                                "args": parsed_args,
                            }
                        });

                        if !unique_cid.is_empty() {
                            fc_part["functionCall"]["id"] = json!(unique_cid);
                        }
                        let key = format!("{}:{}", fname, parsed_args);
                        let sig = signatures.get_with_fallback(cid, fname, &key);
                        fc_part["thoughtSignature"] = json!(sig);
                        parts.push(fc_part);
                        has_tools = true;
                    }
                }

                if !has_text && !has_tools {
                    // Исключение пустых ответов ассистента из истории контекста
                    continue;
                }

                let mut appended = false;
                if let Some(last) = contents.last_mut() {
                    if last.get("role").and_then(|r| r.as_str()) == Some("model") {
                        if let Some(parts_arr) =
                            last.get_mut("parts").and_then(|p| p.as_array_mut())
                        {
                            parts_arr.extend(parts.clone());
                            appended = true;
                        }
                    }
                }

                if !appended {
                    contents.push(json!({
                        "role": "model",
                        "parts": parts,
                    }));
                }
            }
            "tool" => {
                let cid = m.get("tool_call_id").and_then(|c| c.as_str()).unwrap_or("");
                let (unique_cid, fname) = if !cid.is_empty() {
                    if let Some(queue) = call_id_queues.get_mut(cid) {
                        if let Some((ucid, name)) = queue.pop_front() {
                            (ucid, name)
                        } else {
                            (
                                cid.to_string(),
                                m.get("name")
                                    .and_then(|n| n.as_str())
                                    .unwrap_or("tool_result")
                                    .to_string(),
                            )
                        }
                    } else {
                        (
                            cid.to_string(),
                            m.get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("tool_result")
                                .to_string(),
                        )
                    }
                } else {
                    (
                        String::new(),
                        m.get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("tool_result")
                            .to_string(),
                    )
                };

                let mut resp_obj = if let Some(s) = content_val.as_str() {
                    let cleaned = strip_ansi(s);
                    serde_json::from_str(&cleaned).unwrap_or_else(|_| json!({ "result": cleaned }))
                } else if content_val.is_object() {
                    content_val.clone()
                } else {
                    json!({ "result": content_val })
                };

                if let Some(obj) = resp_obj.as_object_mut() {
                    obj.remove("cache_control");
                }

                let mut fn_resp = json!({
                    "name": fname,
                    "response": resp_obj,
                });
                if !unique_cid.is_empty() {
                    fn_resp["id"] = json!(unique_cid);
                }

                // Добавление к последней реплике пользователя при возможности
                let mut appended = false;
                if let Some(last) = contents.last_mut() {
                    if last.get("role").and_then(|r| r.as_str()) == Some("user") {
                        if let Some(parts_arr) =
                            last.get_mut("parts").and_then(|p| p.as_array_mut())
                        {
                            parts_arr.push(json!({ "functionResponse": fn_resp }));
                            appended = true;
                        }
                    }
                }

                if !appended {
                    contents.push(json!({
                        "role": "user",
                        "parts": [{ "functionResponse": fn_resp }],
                    }));
                }
            }
            _ => {}
        }
    }

    // Строгая нормализация чередования ролей (user -> model -> user -> model)
    let mut normalized_contents: Vec<Value> = Vec::new();
    for item in contents {
        let role = item.get("role").and_then(|r| r.as_str()).unwrap_or("user");
        let parts = item
            .get("parts")
            .and_then(|p| p.as_array())
            .cloned()
            .unwrap_or_default();
        if parts.is_empty() {
            continue;
        }

        if let Some(last) = normalized_contents.last_mut() {
            let last_role = last.get("role").and_then(|r| r.as_str()).unwrap_or("");
            if last_role == role {
                if let Some(last_parts) = last.get_mut("parts").and_then(|p| p.as_array_mut()) {
                    last_parts.extend(parts);
                }
                continue;
            }
        }

        normalized_contents.push(json!({
            "role": role,
            "parts": parts,
        }));
    }

    if let Some(first) = normalized_contents.first() {
        if first.get("role").and_then(|r| r.as_str()) == Some("model") {
            normalized_contents.insert(
                0,
                json!({
                    "role": "user",
                    "parts": [{ "text": "..." }]
                }),
            );
        }
    }

    if normalized_contents.is_empty() {
        normalized_contents.push(json!({ "role": "user", "parts": [{ "text": "hi" }] }));
    }

    let contents = Value::Array(normalized_contents);

    // Директива антимонолита и приоритета выполнения действий
    let anti_monolith_directive = "SYSTEM DIRECTIVE (ANTI-MONOLITH & ACTION-FIRST):\n\
        1. Break down large files or code outputs (>120 lines) into smaller, focused modules/chunks instead of dumping a massive monolith in one shot.\n\
        2. In autonomous tool mode, after receiving tool results or continuation prompts ('Продолжи', 'continue'), NEVER output idle conversational chatter ('Создаю...', 'Сейчас сделаю...'). Execute the concrete tool call immediately.";
    sys_parts.push(json!({ "text": anti_monolith_directive }));

    let sys_inst = if sys_parts.is_empty() {
        None
    } else {
        Some(json!({ "parts": sys_parts }))
    };

    (json!(contents), sys_inst)
}

pub fn parse_gemini_response(
    raw_json: &Value,
    model: &str,
    signatures: &SignatureStore,
) -> anyhow::Result<ChatCompletionResponse> {
    let resp = raw_json.get("response").unwrap_or(raw_json);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let candidates = resp.get("candidates").and_then(|c| c.as_array());
    let mut thought_parts = Vec::new();
    let mut text_parts = Vec::new();
    let mut tool_calls = Vec::new();
    let mut finish_reason = "stop".to_string();

    if let Some(cands) = candidates {
        if let Some(cand) = cands.first() {
            if let Some(fr) = cand.get("finishReason").and_then(|f| f.as_str()) {
                finish_reason = fr.to_lowercase();
            }

            if let Some(parts) = cand
                .get("content")
                .and_then(|c| c.get("parts"))
                .and_then(|p| p.as_array())
            {
                for p in parts {
                    let is_thought = p.get("thought").and_then(|v| v.as_bool()).unwrap_or(false);
                    if let Some(t) = p.get("text").and_then(|txt| txt.as_str()) {
                        if is_thought {
                            thought_parts.push(t.to_string());
                        } else {
                            text_parts.push(t.to_string());
                        }
                    }
                    // Извлечение изображений inlineData от визуальных моделей (Nano Banana и генерация картинок)
                    if let Some(inline) = p.get("inlineData").and_then(|i| i.as_object()) {
                        let mime = inline
                            .get("mimeType")
                            .and_then(|m| m.as_str())
                            .unwrap_or("image/png");
                        let b64 = inline.get("data").and_then(|d| d.as_str()).unwrap_or("");
                        if !b64.is_empty() {
                            text_parts.push(format!("data:{};base64,{}", mime, b64));
                        }
                    }
                    if let Some(fc) = p.get("functionCall").and_then(|f| f.as_object()) {
                        let fname = fc.get("name").and_then(|n| n.as_str()).unwrap_or("");
                        let default_args = json!({});
                        let args_val = fc.get("args").unwrap_or(&default_args);
                        let call_id = fc
                            .get("id")
                            .and_then(|i| i.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4().simple()));

                        let sig_val = p
                            .get("thoughtSignature")
                            .or_else(|| p.get("thought_signature"))
                            .or_else(|| fc.get("thoughtSignature"))
                            .or_else(|| fc.get("thought_signature"))
                            .and_then(|s| s.as_str());

                        if let Some(sig) = sig_val {
                            signatures.insert(call_id.clone(), sig.to_string());
                            let arg_key = format!("{}:{}", fname, args_val);
                            signatures.insert(arg_key, sig.to_string());
                            signatures.insert(fname.to_string(), sig.to_string());
                        }

                        tool_calls.push(json!({
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": fname,
                                "arguments": serde_json::to_string(args_val).unwrap_or_else(|_| "{}".to_string()),
                            }
                        }));
                    }
                }
            }
        }
    }

    let mut message = json!({ "role": "assistant" });
    if !thought_parts.is_empty() {
        message["reasoning_content"] = json!(thought_parts.join(""));
    }
    if !text_parts.is_empty() {
        message["content"] = json!(text_parts.join(""));
    } else if tool_calls.is_empty() {
        message["content"] = json!("");
    } else {
        message["content"] = Value::Null;
    }

    if !tool_calls.is_empty() {
        finish_reason = "tool_calls".to_string();
        message["tool_calls"] = json!(tool_calls);
    }

    // Usage
    let usage_obj = resp.get("usageMetadata").unwrap_or(&Value::Null);
    log::info!("[USAGE:RAW] usageMetadata: {}", usage_obj);
    let prompt_cnt = usage_obj
        .get("promptTokenCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let completion_tokens = usage_obj
        .get("candidatesTokenCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let cached_tokens = usage_obj
        .get("cachedContentTokenCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;

    let net_prompt = if prompt_cnt > cached_tokens {
        prompt_cnt - cached_tokens
    } else {
        prompt_cnt
    };
    let total = net_prompt + completion_tokens + cached_tokens;

    Ok(ChatCompletionResponse {
        id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
        object: "chat.completion",
        created: now,
        model: model.to_string(),
        choices: vec![Choice {
            index: 0,
            message,
            finish_reason: Some(finish_reason),
        }],
        usage: Usage {
            prompt_tokens: net_prompt,
            completion_tokens,
            total_tokens: total,
            prompt_tokens_details: if cached_tokens > 0 {
                Some(PromptTokensDetails { cached_tokens })
            } else {
                None
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_response_followed_by_user_merged() {
        let signatures = SignatureStore::new();
        let req = ChatCompletionRequest {
            model: "gemini-3.8-flash-high".to_string(),
            messages: vec![
                json!({ "role": "user", "content": "hello" }),
                json!({
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_123",
                        "type": "function",
                        "function": { "name": "terminal", "arguments": "{\"command\":\"dir\"}" }
                    }]
                }),
                json!({
                    "role": "tool",
                    "tool_call_id": "call_123",
                    "content": "file1.txt"
                }),
                json!({
                    "role": "user",
                    "content": "stop and do something else"
                }),
            ],
            stream: false,
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
        };

        let (contents, _) = convert_messages_to_gemini(&req, &signatures);
        let arr = contents.as_array().expect("contents must be array");

        // Проверяем строгое чередование ролей
        for i in 1..arr.len() {
            let prev_role = arr[i - 1]["role"].as_str().unwrap();
            let curr_role = arr[i]["role"].as_str().unwrap();
            assert_ne!(
                prev_role, curr_role,
                "Roles must strictly alternate! Found consecutive at index {}: {}",
                i, curr_role
            );
        }

        // Последний элемент должен быть user и содержать и functionResponse, и text
        let last = arr.last().unwrap();
        assert_eq!(last["role"], "user");
        let parts = last["parts"].as_array().unwrap();
        assert_eq!(parts.len(), 2);
        assert!(parts[0].get("functionResponse").is_some());
        assert_eq!(parts[1]["text"], "stop and do something else");
    }

    #[test]
    fn test_consecutive_user_messages_merged() {
        let signatures = SignatureStore::new();
        let req = ChatCompletionRequest {
            model: "gemini-3.8-flash-high".to_string(),
            messages: vec![
                json!({ "role": "user", "content": "message 1" }),
                json!({ "role": "user", "content": "message 2" }),
            ],
            stream: false,
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
        };

        let (contents, _) = convert_messages_to_gemini(&req, &signatures);
        let arr = contents.as_array().expect("contents must be array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["role"], "user");
        let parts = arr[0]["parts"].as_array().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["text"], "message 1");
        assert_eq!(parts[1]["text"], "message 2");
    }

    #[test]
    fn test_duplicate_tool_call_id_resolution() {
        let signatures = SignatureStore::new();
        let req = ChatCompletionRequest {
            model: "gemini-3.8-flash-high".to_string(),
            messages: vec![
                json!({ "role": "user", "content": "run step 1" }),
                json!({
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_collision",
                        "type": "function",
                        "function": { "name": "search_files", "arguments": "{}" }
                    }]
                }),
                json!({
                    "role": "tool",
                    "tool_call_id": "call_collision",
                    "content": "found results"
                }),
                json!({ "role": "user", "content": "run step 2" }),
                json!({
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_collision",
                        "type": "function",
                        "function": { "name": "patch", "arguments": "{}" }
                    }]
                }),
                json!({
                    "role": "tool",
                    "tool_call_id": "call_collision",
                    "content": "patch applied"
                }),
                json!({ "role": "user", "content": "all done" }),
            ],
            stream: false,
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
        };

        let (contents, _) = convert_messages_to_gemini(&req, &signatures);
        let arr = contents.as_array().expect("contents must be array");

        let mut fr_names = Vec::new();
        let mut fr_ids = Vec::new();
        for item in arr {
            for part in item["parts"].as_array().unwrap() {
                if let Some(fr) = part.get("functionResponse") {
                    fr_names.push(fr["name"].as_str().unwrap().to_string());
                    fr_ids.push(fr["id"].as_str().unwrap().to_string());
                }
            }
        }

        assert_eq!(fr_names, vec!["search_files", "patch"]);
        assert_ne!(fr_ids[0], fr_ids[1], "IDs must be deduplicated!");
    }
}
