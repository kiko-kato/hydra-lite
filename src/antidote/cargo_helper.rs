use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CargoMissingDiagnostic {
    pub target: String,
    pub required_import: Option<&'static str>,
    pub required_feature: Option<&'static str>,
    pub explanation: &'static str,
}

struct TraitFeatureRule {
    pattern: &'static str,
    target: &'static str,
    required_import: Option<&'static str>,
    required_feature: Option<&'static str>,
    explanation: &'static str,
}

static RULES: &[TraitFeatureRule] = &[
    TraitFeatureRule {
        pattern: "no method named 'read_exact' found",
        target: "AsyncReadExt::read_exact",
        required_import: Some("use tokio::io::AsyncReadExt;"),
        required_feature: Some("tokio = { features = [\"io-util\"] }"),
        explanation: "Метод асинхронного чтения требует трейта AsyncReadExt в области видимости.",
    },
    TraitFeatureRule {
        pattern: "no method named 'read' found",
        target: "AsyncReadExt::read",
        required_import: Some("use tokio::io::AsyncReadExt;"),
        required_feature: Some("tokio = { features = [\"io-util\"] }"),
        explanation: "Метод .read() для сокетов/потоков Tokio требует импорта AsyncReadExt.",
    },
    TraitFeatureRule {
        pattern: "no method named 'write_all' found",
        target: "AsyncWriteExt::write_all",
        required_import: Some("use tokio::io::AsyncWriteExt;"),
        required_feature: Some("tokio = { features = [\"io-util\"] }"),
        explanation: "Метод .write_all() для сокетов Tokio требует импорта AsyncWriteExt.",
    },
    TraitFeatureRule {
        pattern: "no method named 'write' found",
        target: "AsyncWriteExt::write",
        required_import: Some("use tokio::io::AsyncWriteExt;"),
        required_feature: Some("tokio = { features = [\"io-util\"] }"),
        explanation: "Метод .write() требует подключения трейта AsyncWriteExt.",
    },
    TraitFeatureRule {
        pattern: "no method named 'next' found",
        target: "StreamExt::next",
        required_import: Some("use futures::StreamExt; // или tokio_stream::StreamExt;"),
        required_feature: None,
        explanation: "Итерация по Stream через .next() требует трейта StreamExt.",
    },
    TraitFeatureRule {
        pattern: "cannot find derive macro 'Serialize' in this scope",
        target: "serde::Serialize",
        required_import: Some("use serde::Serialize;"),
        required_feature: Some("serde = { version = \"1.0\", features = [\"derive\"] }"),
        explanation: "Для макроса #[derive(Serialize)] необходимо включить фичу 'derive' у serde.",
    },
    TraitFeatureRule {
        pattern: "cannot find derive macro 'Deserialize' in this scope",
        target: "serde::Deserialize",
        required_import: Some("use serde::Deserialize;"),
        required_feature: Some("serde = { version = \"1.0\", features = [\"derive\"] }"),
        explanation: "Для макроса #[derive(Deserialize)] требуется фича 'derive' у serde.",
    },
    TraitFeatureRule {
        pattern: "cannot find attribute 'main' in crate 'tokio'",
        target: "tokio::main",
        required_import: None,
        required_feature: Some("tokio = { version = \"1\", features = [\"macros\", \"rt-multi-thread\"] }"),
        explanation: "Атрибут #[tokio::main] требует активации фич 'macros' и 'rt-multi-thread' в Cargo.toml.",
    },
    TraitFeatureRule {
        pattern: "cannot find macro 'select' in this scope",
        target: "tokio::select!",
        required_import: None,
        required_feature: Some("tokio = { version = \"1\", features = [\"macros\"] }"),
        explanation: "Макрос tokio::select! требует фичи 'macros' у крейта tokio.",
    },
    TraitFeatureRule {
        pattern: "no method named 'json' found for struct 'reqwest::RequestBuilder'",
        target: "reqwest::RequestBuilder::json",
        required_import: None,
        required_feature: Some("reqwest = { version = \"0.12\", features = [\"json\"] }"),
        explanation: "Метод отправки/парсинга JSON в reqwest требует включения фичи 'json'.",
    },
];

static SCAN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(no method named '[a-z0-9_]+' found|cannot find derive macro|cannot find attribute '[a-z0-9_]+' in crate 'tokio'|cannot find macro 'select')")
        .expect("Valid regex")
});

pub fn diagnose_cargo_missing(text: &str) -> Option<CargoMissingDiagnostic> {
    if !SCAN_REGEX.is_match(text) {
        return None;
    }

    let lower = text.to_lowercase();
    for rule in RULES {
        if lower.contains(&rule.pattern.to_lowercase()) {
            return Some(CargoMissingDiagnostic {
                target: rule.target.to_string(),
                required_import: rule.required_import,
                required_feature: rule.required_feature,
                explanation: rule.explanation,
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_async_read_ext() {
        let msg = "error[E0599]: no method named 'read_exact' found for struct 'tokio::net::TcpStream' in the current scope";
        let diag = diagnose_cargo_missing(msg).expect("Must match");
        assert_eq!(diag.target, "AsyncReadExt::read_exact");
        assert_eq!(diag.required_import, Some("use tokio::io::AsyncReadExt;"));
    }

    #[test]
    fn test_serde_derive() {
        let msg = "error: cannot find derive macro 'Serialize' in this scope";
        let diag = diagnose_cargo_missing(msg).expect("Must match");
        assert_eq!(diag.target, "serde::Serialize");
    }
}
