use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RustcDiagnostic {
    pub code: String,
    pub title: &'static str,
    pub explanation: &'static str,
    pub common_fix: &'static str,
}

struct RustcEntry {
    code: &'static str,
    title: &'static str,
    explanation: &'static str,
    common_fix: &'static str,
}

static RUSTC_ENTRIES: &[RustcEntry] = &[
    RustcEntry {
        code: "E0382",
        title: "use of moved value",
        explanation: "Значение переменной уже было перемещено (move) в другое место и больше недоступно.",
        common_fix: "Используйте заимствование (&val), добавьте #[derive(Clone)] и вызовите .clone(), либо передавайте владение явно.",
    },
    RustcEntry {
        code: "E0597",
        title: "borrowed value does not live long enough",
        explanation: "Ссылка указывает на данные, которые уничтожаются раньше, чем прекращается использование ссылки.",
        common_fix: "Продлите время жизни исходных данных (вынесите let наружу), возвращайте владеющий тип (String / Vec) вместо ссылки, либо оберните в Arc.",
    },
    RustcEntry {
        code: "E0502",
        title: "cannot borrow as mutable because it is also borrowed as immutable",
        explanation: "Нарушение алиасинга: одновременное наличие активной неизменяемой ссылки (&) и попытка создать изменяемую ссылку (&mut).",
        common_fix: "Ограничьте скоуп неизменяемого заимствования через блок { ... }, завершите чтение до мутации, либо скопируйте нужные поля.",
    },
    RustcEntry {
        code: "E0277",
        title: "the trait bound is not satisfied",
        explanation: "Тип не реализует обязательный трейт, требуемый функцией или интерфейсом (например, Send, Sync, Clone, Serialize, Default).",
        common_fix: "Добавьте derive-макрос (#[derive(Clone, Serialize, Deserialize)]) или реализуйте трейт impl Trait for Type.",
    },
    RustcEntry {
        code: "E0308",
        title: "mismatched types",
        explanation: "Несовпадение типов: ожидался один тип, но передан другой (например, &str вместо String, Result вместо Option, u32 вместо usize).",
        common_fix: "Используйте .as_str(), .to_string(), .into(), явное приведение as usize или распаковку через ? / match.",
    },
    RustcEntry {
        code: "E0432",
        title: "unresolved import",
        explanation: "Не удалось разрешить импорт модуля или типа в директиве use.",
        common_fix: "Проверьте имя крейта в Cargo.toml, наличие pub mod в корне крейта или используйте crate:: вместо super::.",
    },
    RustcEntry {
        code: "E0599",
        title: "no method named in current scope",
        explanation: "Метод не найден у структуры или типа в текущей области видимости.",
        common_fix: "Метод может принадлежать трейту (например, AsyncReadExt, StreamExt, Iterator). Импортируйте нужный трейт в область видимости: use crate::Trait;",
    },
    RustcEntry {
        code: "E0425",
        title: "cannot find value in this scope",
        explanation: "Переменная, константа или функция с таким именем не найдена в текущей области видимости.",
        common_fix: "Проверьте опечатку в идентификаторе, видимость через pub, либо добавьте use path::to::item;",
    },
    RustcEntry {
        code: "E0061",
        title: "this function takes N arguments but M arguments were supplied",
        explanation: "Количество переданных параметров не совпадает с объявленной сигнатурой функции.",
        common_fix: "Сверьтесь с сигнатурой вызываемой функции и передайте корректное число аргументов.",
    },
    RustcEntry {
        code: "E0106",
        title: "missing lifetime specifier",
        explanation: "Компилятор не может автоматически вывести время жизни ссылки в структуре или сигнатуре функции.",
        common_fix: "Объявите параметр времени жизни явно: struct Foo<'a> { bar: &'a str } или fn foo<'a>(x: &'a str) -> &'a str.",
    },
];

static RUSTC_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"error\[E(?P<code>\d{4})\]").expect("Valid regex"));

pub fn diagnose_rustc_error(text: &str) -> Option<RustcDiagnostic> {
    let caps = RUSTC_REGEX.captures(text)?;
    let num_str = caps.name("code")?.as_str();
    let full_code = format!("E{}", num_str);

    for entry in RUSTC_ENTRIES {
        if entry.code == full_code {
            return Some(RustcDiagnostic {
                code: full_code,
                title: entry.title,
                explanation: entry.explanation,
                common_fix: entry.common_fix,
            });
        }
    }

    Some(RustcDiagnostic {
        code: full_code.clone(),
        title: "Rust compiler diagnostic",
        explanation: "Обнаружена ошибка компилятора Rustc.",
        common_fix: "Запустите `rustc --explain` для подробной справки по коду ошибки.",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_e0382_move() {
        let msg = "error[E0382]: use of moved value: `config`\n  --> src/main.rs:14:5";
        let diag = diagnose_rustc_error(msg).expect("Must find E0382");
        assert_eq!(diag.code, "E0382");
        assert_eq!(diag.title, "use of moved value");
    }

    #[test]
    fn test_clean() {
        let msg = "running 12 tests... test result: ok. 12 passed";
        assert!(diagnose_rustc_error(msg).is_none());
    }
}
