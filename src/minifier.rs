use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

static ANSI_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])").unwrap());

/// Lossless очистка текстовой строки:
/// - Удаляет ANSI escape-коды
/// - Сжимает серии пустых строк (более 2 подряд)
/// - Срезает trailing whitespace, сохраняя табуляцию и отступы кода
pub fn minify_text(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }

    // 1. Очистка ANSI
    let without_ansi = if input.contains('\x1b') {
        ANSI_REGEX.replace_all(input, "").into_owned()
    } else {
        input.to_string()
    };

    // 2. Построчная обработка: сохраняем код, убираем trailing whitespace
    let mut result = String::with_capacity(without_ansi.len());
    let mut consecutive_newlines = 0;
    let mut in_code_block = false;

    for line in without_ansi.split('\n') {
        let trimmed_end = line.trim_end_matches([' ', '\t', '\r']);

        if trimmed_end.trim_start().starts_with("```") {
            in_code_block = !in_code_block;
        }

        if trimmed_end.is_empty() {
            consecutive_newlines += 1;
            if consecutive_newlines <= 2 {
                result.push('\n');
            }
        } else {
            consecutive_newlines = 0;
            if !result.is_empty() && !result.ends_with('\n') {
                result.push('\n');
            }
            if in_code_block {
                // В блоках кода отступы трогать нельзя вообще
                result.push_str(line.trim_end_matches('\r'));
            } else {
                result.push_str(trimmed_end);
            }
        }
    }

    result.trim_end_matches(['\n', '\r']).to_string()
}

/// Рекурсивная нормализация полезной нагрузки Gemini (Zero-loss):
/// Проходит по всем parts и systemInstruction, минимизируя текст без потери логики.
/// ВАЖНО: thinkingConfig, thoughtSignature и tool_calls не модифицируются.
pub fn minify_gemini_payload(payload: &mut Value) {
    if let Some(obj) = payload.as_object_mut() {
        if let Some(req) = obj.get_mut("request").and_then(|r| r.as_object_mut()) {
            // 1. Нормализация системных инструкций
            if let Some(sys_inst) = req
                .get_mut("systemInstruction")
                .and_then(|s| s.as_object_mut())
            {
                if let Some(parts) = sys_inst.get_mut("parts").and_then(|p| p.as_array_mut()) {
                    for part in parts {
                        if let Some(part_obj) = part.as_object_mut() {
                            if let Some(text) = part_obj.get_mut("text").and_then(|t| t.as_str()) {
                                let minified = minify_text(text);
                                *part_obj.get_mut("text").unwrap() = Value::String(minified);
                            }
                        }
                    }
                }
            }

            // 2. Нормализация сообщений диалога
            if let Some(contents) = req.get_mut("contents").and_then(|c| c.as_array_mut()) {
                for content in contents {
                    if let Some(content_obj) = content.as_object_mut() {
                        if let Some(parts) =
                            content_obj.get_mut("parts").and_then(|p| p.as_array_mut())
                        {
                            for part in parts {
                                if let Some(part_obj) = part.as_object_mut() {
                                    // Нормализуем только text-части, не трогая functionCall и thoughtSignature
                                    if let Some(text) =
                                        part_obj.get_mut("text").and_then(|t| t.as_str())
                                    {
                                        let minified = minify_text(text);
                                        *part_obj.get_mut("text").unwrap() =
                                            Value::String(minified);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ansi_and_trailing_strip() {
        let raw = "\x1b[32mHello\x1b[0m   \n\n\n\nWorld!  \r\n";
        let min = minify_text(raw);
        assert_eq!(min, "Hello\n\nWorld!");
    }

    #[test]
    fn test_code_block_preserved() {
        let raw = "```python\n    def foo():\n        return 42\n```";
        let min = minify_text(raw);
        assert_eq!(min, "```python\n    def foo():\n        return 42\n```");
    }
}
