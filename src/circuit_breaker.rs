use log::warn;
use parking_lot::Mutex;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ToolCallRecord {
    pub tool_name: String,
    pub args_hash: [u8; 32],
}

pub struct SessionCircuitBreaker {
    ring_buffer: VecDeque<ToolCallRecord>,
    max_history: usize,
    trip_threshold: usize,
}

impl SessionCircuitBreaker {
    pub fn new(trip_threshold: usize) -> Self {
        Self {
            ring_buffer: VecDeque::with_capacity(8),
            max_history: 6,
            trip_threshold,
        }
    }

    pub fn record_and_check(&mut self, tool_name: &str, args: &Value) -> bool {
        let canonical_json = serde_json::to_string(args).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(tool_name.as_bytes());
        hasher.update(canonical_json.as_bytes());
        let hash: [u8; 32] = hasher.finalize().into();

        let repeat_count = self
            .ring_buffer
            .iter()
            .filter(|r| r.tool_name == tool_name && r.args_hash == hash)
            .count();

        if self.ring_buffer.len() >= self.max_history {
            self.ring_buffer.pop_front();
        }

        self.ring_buffer.push_back(ToolCallRecord {
            tool_name: tool_name.to_string(),
            args_hash: hash,
        });

        repeat_count + 1 >= self.trip_threshold
    }

    pub fn reset(&mut self) {
        self.ring_buffer.clear();
    }
}

#[derive(Clone)]
pub struct CircuitBreakerManager {
    sessions: Arc<Mutex<HashMap<String, SessionCircuitBreaker>>>,
    trip_threshold: usize,
}

impl CircuitBreakerManager {
    pub fn new(trip_threshold: usize) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            trip_threshold,
        }
    }

    pub fn check_openai_tool_call(&self, session_key: &str, tool_name: &str, args: &Value) -> bool {
        let mut lock = self.sessions.lock();
        let breaker = lock
            .entry(session_key.to_string())
            .or_insert_with(|| SessionCircuitBreaker::new(self.trip_threshold));

        let tripped = breaker.record_and_check(tool_name, args);
        if tripped {
            warn!(
                "[CIRCUIT_BREAKER] Tripped on tool '{}' for session '{}'",
                tool_name, session_key
            );
        }
        tripped
    }

    pub fn inspect_and_trip_openai(&self, session_key: &str, messages: &mut [Value]) -> bool {
        let (last_msg, rest) = match messages.split_last_mut() {
            Some(s) => s,
            None => return false,
        };

        let obj = match last_msg.as_object_mut() {
            Some(o) => o,
            None => return false,
        };

        if obj.get("role").and_then(|r| r.as_str()) != Some("tool") {
            return false;
        }

        let prev_assistant = match rest.last() {
            Some(a) => a,
            None => return false,
        };

        let tool_calls = match prev_assistant
            .get("tool_calls")
            .and_then(|tc| tc.as_array())
        {
            Some(tc) => tc,
            None => return false,
        };

        for tc in tool_calls {
            let func = match tc.get("function") {
                Some(f) => f,
                None => continue,
            };
            let name = func
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();
            let args_raw = func
                .get("arguments")
                .and_then(|a| a.as_str())
                .unwrap_or("{}");
            let args_val = serde_json::from_str::<Value>(args_raw).unwrap_or(Value::Null);

            if self.check_openai_tool_call(session_key, name, &args_val) {
                let alert = Self::trip_alert(name, self.trip_threshold);
                if let Some(Value::String(s)) = obj.get_mut("content") {
                    s.push_str(&alert);
                }
                return true;
            }
        }

        false
    }

    pub fn inspect_and_trip_anthropic(&self, session_key: &str, messages: &mut [Value]) -> bool {
        let (last_msg, rest) = match messages.split_last_mut() {
            Some(s) => s,
            None => return false,
        };

        let obj = match last_msg.as_object_mut() {
            Some(o) => o,
            None => return false,
        };

        let content_arr = match obj.get_mut("content").and_then(|c| c.as_array_mut()) {
            Some(arr) => arr,
            None => return false,
        };

        let prev_assistant = match rest.last() {
            Some(a) => a,
            None => return false,
        };

        let prev_content = match prev_assistant.get("content").and_then(|c| c.as_array()) {
            Some(arr) => arr,
            None => return false,
        };

        let mut tripped_any = false;

        for block in content_arr.iter_mut() {
            let b_obj = match block.as_object_mut() {
                Some(o) => o,
                None => continue,
            };

            if b_obj.get("type").and_then(|t| t.as_str()) != Some("tool_result") {
                continue;
            }

            let tool_use_id = b_obj
                .get("tool_use_id")
                .and_then(|id| id.as_str())
                .unwrap_or_default()
                .to_string();

            for call in prev_content {
                if call.get("type").and_then(|t| t.as_str()) == Some("tool_use") {
                    if call.get("id").and_then(|id| id.as_str()) == Some(&tool_use_id) {
                        let name = call
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or_default();
                        let input = call.get("input").unwrap_or(&Value::Null);

                        if self.check_openai_tool_call(session_key, name, input) {
                            let alert = Self::trip_alert(name, self.trip_threshold);
                            if let Some(Value::String(s)) = b_obj.get_mut("content") {
                                s.push_str(&alert);
                            }
                            tripped_any = true;
                        }
                    }
                }
            }
        }

        tripped_any
    }

    pub fn trip_alert(tool_name: &str, count: usize) -> String {
        format!(
            "\n\n[HYDRA CIRCUIT BREAKER: PREVENTED REPETITIVE LOOP]\n\
            Внимание: Зафиксировано {} идентичных вызовов инструмента '{}' с одинаковыми аргументами без прогресса.\n\
            ПРЕДПИСАНИЕ:\n\
            1. Запрещено повторять идентичный вызов с теми же параметрами.\n\
            2. Смени стратегию, путь поиска или используй альтернативный инструмент.\n\
            3. Если задача зашла в тупик — прямо сообщи об этом Стёпе.",
            count, tool_name
        )
    }
}
