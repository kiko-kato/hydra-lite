use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WinErrorDiagnostic {
    pub code: String,
    pub name: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub recommended_action: &'static str,
}

struct KnownError {
    patterns: &'static [&'static str],
    code: &'static str,
    name: &'static str,
    category: &'static str,
    description: &'static str,
    recommended_action: &'static str,
}

static KNOWN_ERRORS: &[KnownError] = &[
    KnownError {
        patterns: &["0xc00000fd", "c00000fd", "status_stack_overflow"],
        code: "0xC00000FD",
        name: "STATUS_STACK_OVERFLOW",
        category: "NTSTATUS",
        description: "Переполнение стека рабочего потока (дефолтный лимит Windows для потока составляет 1 МБ).",
        recommended_action: "Увеличьте размер стека потока в Builder рантайма: .thread_stack_size(16 * 1024 * 1024) или используйте Box::pin / вынесение тяжелых структур в кучу.",
    },
    KnownError {
        patterns: &["0xc0000005", "c0000005", "status_access_violation"],
        code: "0xC0000005",
        name: "STATUS_ACCESS_VIOLATION",
        category: "NTSTATUS",
        description: "Нарушение прав доступа к памяти (Null-pointer dereference, use-after-free или порча памяти в FFI/unsafe).",
        recommended_action: "Проверьте корректность сырых указателей в FFI, границы буферов в unsafe-блоках и избегайте передачи неинициализированных дескрипторов WinAPI.",
    },
    KnownError {
        patterns: &["0xc000013a", "c000013a", "status_control_c_exit"],
        code: "0xC000013A",
        name: "STATUS_CONTROL_C_EXIT",
        category: "NTSTATUS",
        description: "Процесс аварийно завершен сигналом Ctrl+C или закрытием родительской консоли Windows.",
        recommended_action: "Запустите процесс в фоне (background / detached) через WinAPI CREATE_NO_WINDOW / DETACHED_PROCESS или VBS-враппер.",
    },
    KnownError {
        patterns: &["0xc0000142", "c0000142", "status_dll_init_failed"],
        code: "0xC0000142",
        name: "STATUS_DLL_INIT_FAILED",
        category: "NTSTATUS",
        description: "Сбой инициализации динамической библиотеки (DLL). Отсутствуют зависимости среды выполнения или поврежден манифест.",
        recommended_action: "Проверьте наличие VC++ Redistributable (msvcp140.dll, vcruntime140.dll) или корректность разрядности DLL (x64 vs x86).",
    },
    KnownError {
        patterns: &["0x80070005", "os error 5", "error_access_denied", "access is denied"],
        code: "0x80070005",
        name: "ERROR_ACCESS_DENIED",
        category: "Win32 / HRESULT",
        description: "Отказано в доступе. Файл/ресурс заблокирован другим процессом либо отсутствуют административные привилегии.",
        recommended_action: "Проверьте, не открыт ли целевой .exe файл работающей службой/демоном, либо запустите операцию с соответствующими правами.",
    },
    KnownError {
        patterns: &["0x80070020", "os error 32", "error_sharing_violation"],
        code: "0x80070020",
        name: "ERROR_SHARING_VIOLATION",
        category: "Win32",
        description: "Конфликт совместного доступа к файлу. Процесс не может получить доступ, так как файл занят другим процессом.",
        recommended_action: "Завершите процесс, удерживающий дескриптор файла, либо используйте альтернативное имя временного файла.",
    },
    KnownError {
        patterns: &["10054", "wsaeconnreset", "connection reset by peer"],
        code: "10054",
        name: "WSAECONNRESET",
        category: "Winsock",
        description: "Существующее подключение принудительно разорвано удаленным узлом или сетевым фильтром (ТСПУ / DPI).",
        recommended_action: "При работе через ТСПУ используйте сегментацию SNI (Ghost-Tunnel / midsld) либо резервный шифрованный прокси-канал.",
    },
    KnownError {
        patterns: &["10048", "wsaeaddrinuse", "address already in use"],
        code: "10048",
        name: "WSAEADDRINUSE",
        category: "Winsock",
        description: "Адрес сокета (порт) уже занят другим активным процессом.",
        recommended_action: "Определите PID процесса, занимающего локальный порт, и освободите сокет перед повторным запуском.",
    },
    KnownError {
        patterns: &["10060", "wsaetimedout", "connection timed out"],
        code: "10060",
        name: "WSAETIMEDOUT",
        category: "Winsock",
        description: "Таймаут подключения к сокету. Удаленный узел не ответил на SYN-пакет.",
        recommended_action: "Проверьте доступность хоста, параметры маршрутизации и отсутствие блокировки IP/порта локальным брандмауэром.",
    },
    KnownError {
        patterns: &["10061", "wsaeconnrefused", "connection refused"],
        code: "10061",
        name: "WSAECONNREFUSED",
        category: "Winsock",
        description: "Подключение отклонено. Целевой сервис не слушает указанный порт или привязан исключительно к другому интерфейсу.",
        recommended_action: "Убедитесь, что демон запущен и слушает ожидаемый IP (например, 127.0.0.1 вместо 0.0.0.0).",
    },
];

static SCAN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(0x[0-9a-f]{8}|os error \d+|wsae[a-z]+|status_[a-z_]+|access is denied|connection reset by peer|address already in use)")
        .expect("Valid regex")
});

pub fn decode_windows_error(text: &str) -> Option<WinErrorDiagnostic> {
    if !SCAN_REGEX.is_match(text) {
        return None;
    }

    let lower = text.to_lowercase();
    for entry in KNOWN_ERRORS {
        for pat in entry.patterns {
            if lower.contains(pat) {
                return Some(WinErrorDiagnostic {
                    code: entry.code.to_string(),
                    name: entry.name,
                    category: entry.category,
                    description: entry.description,
                    recommended_action: entry.recommended_action,
                });
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stack_overflow() {
        let text = "fatal runtime error: failed to initiate panic, error 0xc00000fd";
        let diag = decode_windows_error(text).expect("Must match");
        assert_eq!(diag.code, "0xC00000FD");
        assert_eq!(diag.name, "STATUS_STACK_OVERFLOW");
    }

    #[test]
    fn test_wsaeaddrinuse() {
        let text = "failed to bind: os error 10048 (WSAEADDRINUSE)";
        let diag = decode_windows_error(text).expect("Must match");
        assert_eq!(diag.code, "10048");
        assert_eq!(diag.name, "WSAEADDRINUSE");
    }

    #[test]
    fn test_clean_text() {
        let text = "cargo build finished successfully in 2.1s";
        assert!(decode_windows_error(text).is_none());
    }
}
