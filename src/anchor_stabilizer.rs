use log::info;
use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

static DATE_TIME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:Conversation started:\s*)?(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}(?:\s+\([^\r\n]+\))?|Current time:\s*[^\n\r]+").unwrap()
});

pub struct AnchorStabilizer;

impl AnchorStabilizer {
    pub fn stabilize_system_str(system: &mut String) -> Option<String> {
        if let Some(mat) = DATE_TIME_REGEX.find(system) {
            let extracted = mat.as_str().to_string();
            let start = mat.start();
            let end = mat.end();
            let mut new_sys = String::with_capacity(system.len());
            new_sys.push_str(&system[..start]);
            new_sys.push_str("[SYSTEM_ANCHOR_TIME_NORMALIZED]");
            new_sys.push_str(&system[end..]);
            *system = new_sys;
            Some(extracted)
        } else {
            None
        }
    }

    pub fn stabilize_openai(messages: &mut [Value]) -> bool {
        if messages.is_empty() {
            return false;
        }

        // 1. Look for system message
        let mut extracted_time = None;
        for msg in messages.iter_mut() {
            if let Some(obj) = msg.as_object_mut() {
                if obj.get("role").and_then(|r| r.as_str()) == Some("system") {
                    if let Some(Value::String(s)) = obj.get_mut("content") {
                        if let Some(ext) = Self::stabilize_system_str(s) {
                            extracted_time = Some(ext);
                            break;
                        }
                    }
                }
            }
        }

        // 2. If extracted, relocate to the last message content
        if let Some(ext_time) = extracted_time {
            if let Some(last_msg) = messages.last_mut() {
                if let Some(obj) = last_msg.as_object_mut() {
                    let addition = format!("\n\n[CONTEXT TIMESTAMP: {}]", ext_time);
                    match obj.get_mut("content") {
                        Some(Value::String(s)) => {
                            s.push_str(&addition);
                        }
                        Some(Value::Array(arr)) => {
                            // If array of blocks, append to the last text block or create one
                            let mut appended = false;
                            for block in arr.iter_mut().rev() {
                                if let Some(b_obj) = block.as_object_mut() {
                                    if b_obj.get("type").and_then(|t| t.as_str()) == Some("text") {
                                        if let Some(Value::String(s)) = b_obj.get_mut("text") {
                                            s.push_str(&addition);
                                            appended = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            if !appended {
                                arr.push(serde_json::json!({ "type": "text", "text": addition.trim_start() }));
                            }
                        }
                        _ => {}
                    }
                }
            }
            info!("[ANCHOR_STABILIZER] Successfully stabilized system prompt and shifted timestamp to last message");
            true
        } else {
            false
        }
    }

    pub fn stabilize_anthropic(system: &mut Option<Value>, messages: &mut [Value]) -> bool {
        let mut extracted_time = None;

        if let Some(ref mut sys_val) = system {
            match sys_val {
                Value::String(ref mut s) => {
                    if let Some(ext) = Self::stabilize_system_str(s) {
                        extracted_time = Some(ext);
                    }
                }
                Value::Array(ref mut arr) => {
                    for item in arr.iter_mut() {
                        if let Some(obj) = item.as_object_mut() {
                            if let Some(Value::String(ref mut s)) = obj.get_mut("text") {
                                if let Some(ext) = Self::stabilize_system_str(s) {
                                    extracted_time = Some(ext);
                                    break;
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if let Some(ext_time) = extracted_time {
            if let Some(last_msg) = messages.last_mut() {
                if let Some(obj) = last_msg.as_object_mut() {
                    let addition = format!("\n\n[CONTEXT TIMESTAMP: {}]", ext_time);
                    match obj.get_mut("content") {
                        Some(Value::String(s)) => {
                            s.push_str(&addition);
                        }
                        Some(Value::Array(arr)) => {
                            let mut appended = false;
                            for block in arr.iter_mut().rev() {
                                if let Some(b_obj) = block.as_object_mut() {
                                    if b_obj.get("type").and_then(|t| t.as_str()) == Some("text") {
                                        if let Some(Value::String(s)) = b_obj.get_mut("text") {
                                            s.push_str(&addition);
                                            appended = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            if !appended {
                                arr.push(serde_json::json!({ "type": "text", "text": addition.trim_start() }));
                            }
                        }
                        _ => {}
                    }
                }
            }
            info!("[ANCHOR_STABILIZER] Successfully stabilized Anthropic system prompt and shifted timestamp");
            true
        } else {
            false
        }
    }
}
