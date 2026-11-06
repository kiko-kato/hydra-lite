# План внедрения Zero-Knowledge Vault Logger в Hydra-Lite

## 1. Концепция и безопасность
- **Цель:** Полное логирование всех входящих запросов, тулзов и ответов модели в Hydra-Lite на VPS (`185.242.2.14`) без возможности расшифровки владельцем сервера (Ваней).
- **Криптография:** Асимметричное шифрование `age` (X25519 + ChaCha20-Poly1305). На сервере лежит ТОЛЬКО публичный ключ (`age1...`). Приватный ключ (`AGE-SECRET-KEY-...`) хранится исключительно локально на ноутбуке Стёпы.
- **Сжатие:** `zstd` (level 3) перед шифрованием — уменьшение размера на диске в 8-10 раз (~10-15 МБ в сутки при активном использовании).
- **ОЗУ и стабильность:** Никаких висячих сессий в памяти. Асинхронный bounded-канал `tokio::sync::mpsc::channel(64)`. Каждый запрос пишется append-only потоком и сразу вымывается из памяти (Zero-Trace ОЗУ). Потребление RAM: ~2-3 МБ.

## 2. Структура изменений в кодовой базе (/home/kiko/hydra-lite)
1. **`Cargo.toml`**:
   - Добавить `age = "0.10"` (или `age = { version = "0.10", default-features = false, features = ["cli-common"] }`).
   - Добавить `zstd = "0.13"`.
2. **`src/config.rs` & `config.example.toml`**:
   - Поля:
     ```toml
     [vault]
     enabled = true
     public_key = "age1..."
     vault_dir = "/opt/hydra/vault"
     ```
3. **`src/vault_logger.rs` (Новый модуль)**:
   - Структура `VaultEvent`:
     - `timestamp`: RFC3339
     - `api_key_name`: String
     - `model`: String
     - `messages`: serde_json::Value (полный ввод)
     - `response`: serde_json::Value (текст/тулколы)
     - `usage`: токены, latency
   - `VaultLogger`: фоновый воркер Tokio, слушающий `mpsc::Receiver<VaultEvent>`.
   - Запись чанками в ротируемый посуточный файл `/opt/hydra/vault/YYYY-MM-DD.vault.age`.
4. **Интеграция в пайплайн**:
   - `src/lib.rs` / `src/anthropic.rs` / `src/main.rs`: проброс `vault_sender` в обработчики запросов (как для OpenAI формата `/v1/chat/completions`, так и для Anthropic `/v1/messages`).

## 3. Генерация ключей и хранение
- Сгенерировать пару ключей на ноутбуке:
  - Публичный ключ (`age1...`) -> в `config.toml` на VPS.
  - Приватный ключ (`AGE-SECRET-KEY-...`) -> в `~/.hermes/keys.db` (сервис `hydra_vault_private_key`) + бэкап в защищенный локальный файл `~/.hermes/hydra_vault.key`.

## 4. Сборка, деплой и верификация
1. Локальная компиляция: `cargo build --release`.
2. Подготовка каталога на VPS: `/opt/hydra/vault` с правами 0700.
3. Доставка бинарника и конфига на `root@185.242.2.14`.
4. Перезапуск службы `systemctl restart hydra.service`.
5. Тестовый прогон боевого запроса.
6. Выгрузка зашифрованного файла на ноутбук и расшифровка тестовой утилитой для верификации целостности данных.
