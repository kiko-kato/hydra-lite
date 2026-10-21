# Hydra-Lite API Gateway (v2.0.0)

Высокопроизводительный асинхронный API-шлюз на Rust (Tokio / Axum / Reqwest) с поддержкой OpenAI и Anthropic API протоколов, самоисцелением Antidote, адаптивной синхронизацией моделей и встроенным темным веб-интерфейсом управления.

---

## ⚡ Ключевые возможности

1. **Zero-Proxy Direct Core**
   - Прямой сетевой I/O к дата-центрам Google Cloud Code PA / Gemini без сторонних прокси-цепочек.
   - Минимальный оверхед: потребление памяти 15–25 МБ, время отклика TTFT ~40–60 мс при размещении в США/ЕС.
2. **Адаптивный Model Manager**
   - Автоматический опрос `fetchAvailableModels` каждые 5 минут.
   - Автогенерация slug-алиасов из DisplayName (например: `Gemini 3.8 Flash (High)` -> `gemini-3.8-flash-high`, `gemini-3.8-flash`, `flash-high`).
   - Принцип устойчивости: если модель не подошла под правила генерации алиасов, шлюз отдает её оригинальное "голое название" без отсечений.
3. **Hydra Antidote (Self-Healing)**
   - Встроенный перехватчик компиляционных и системных инцидентов для AI-агентов (инжекция проверенных решений в контекст).
4. **Circuit Breaker & Anchor Stabilizer**
   - Защита от бесконечных циклов вызова инструментов.
   - Фиксация системного промпта для эффективного серверного префикс-кэширования Google/Anthropic (HRW Affinity).
5. **Встроенный Web-интерфейс (Linear Dark)**
   - Доступен по адресу `/admin` и `/`.
   - Вход строго по административным API-ключам.
   - Мониторинг квот аккаунтов (Gemini / Claude %), логи последних 100 запросов, журнал ошибок upstream, таблица моделей с мгновенным переключением активности (свитчи ON/OFF) и управление клиентскими API-ключами.

---

## 🔐 Аутентификация и API Ключи

Шлюз поддерживает разграничение прав доступа через SQLite таблицу `api_keys`:

| Имя | Токен ключа | Роль | Доступ к /admin |
| :--- | :--- | :--- | :--- |
| **Admin (Owner)** | `hydra-adm-demo-master-key-01` | `ADMIN` | Полный |
| **Developer** | `hydra-adm-demo-developer-key` | `ADMIN` | Полный |
| **Client App** | `hydra-usr-demo-user-key-001` | `USER` | Только API инференса |

Запросы к API отправляются со стандартным заголовком:
```http
Authorization: Bearer <API_KEY>
```
или
```http
x-api-key: <API_KEY>
```

---

## 🛠 Структура конфигурации (`config.toml`)

```toml
[server]
bind = "127.0.0.1:1024"
api_key = "hydra-adm-demo-master-key-01" # Мастер-ключ
request_body_limit_mb = 64
cors_allow_all = true

[oauth]
client_id = "your-app-id.apps.googleusercontent.com"
client_secret = "GOCSPX-your-google-oauth-secret"
token_endpoint = "https://oauth2.googleapis.com/token"
token_refresh_skew_sec = 300.0
token_error_cooldown_sec = 60.0

[upstream]
proxy = "" # Пустая строка = прямой коннект без прокси
connect_timeout_ms = 15000
total_timeout_ms = 300000
stream_idle_timeout_ms = 300000
max_retries = 3
backoff_initial_ms = 500
backoff_multiplier = 2.0
generate_endpoints = ["https://cloudcode-pa.googleapis.com/v1internal:generateContent"]
stream_endpoints = ["https://cloudcode-pa.googleapis.com/v1internal:streamGenerateContent?alt=sse"]

[quota]
poll_interval_sec = 1800
min_fraction = 0.02
rate_limit_cooldown_sec = 30.0
load_balancing_strategy = "fraction_desc"

[generation_defaults]
default_model = "gemini-3.8-flash-high"
max_output_tokens = 65536
default_temperature = 0.7
top_p = 0.95
top_k = 40

[database]
path = "data/antigravity.db"
logs_db_path = "data/logs.db"
wal_autocheckpoint_pages = 1000
busy_timeout_ms = 5000
```

---

## 🌐 Сетевые эндпоинты

- `GET /` и `GET /admin` — Веб-интерфейс панели управления.
- `POST /api/auth/login` — Проверка административного ключа.
- `GET /v1/models` — Список всех доступных моделей (включая автоалиасы и голые ID).
- `POST /v1/chat/completions` — OpenAI-совместимый инференс (streaming SSE + non-streaming).
- `POST /v1/messages` — Anthropic-совместимый инференс.
- `GET /api/accounts` — Пул аккаунтов Google и текущие остатки квот.
- `PATCH /api/accounts/:id/toggle` — Включение/выключение аккаунта в пуле.
- `GET /api/models/config` — Каталог моделей с текущими алиасами и статусами.
- `PATCH /api/models/config/:id` — Обновление алиаса или активности модели (`{"alias": "...", "enabled": true}`).
- `GET /api/telemetry/requests` — Журнал последних запросов.
- `GET /api/telemetry/errors` — Журнал ошибок upstream.
- `GET /api/keys` — Список API ключей.
- `POST /api/keys` — Выпуск нового API ключа (`{"name": "...", "is_admin": false}`).
- `GET /health` — Healthcheck шлюза.

---

## 📦 Развертывание под Systemd и Caddy (с авто-SSL)

### 1. Сервис Systemd (`/etc/systemd/system/hydra.service`)
```ini
[Unit]
Description=Hydra-Lite Gateway
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/hydra
ExecStart=/opt/hydra/hydra-lite
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

### 2. Конфигурация Caddy (`/etc/caddy/Caddyfile`)
```caddy
hydra-project.ru, api.hydra-project.ru {
    reverse_proxy 127.0.0.1:1024 {
        flush_interval -1
    }
}
```

---

## 🤖 Для AI-агентов (Инструкция по сопровождению)

1. **База данных `data/antigravity.db`**:
   - Используется SQLite в режиме WAL.
   - Таблица `accounts`: хранит `refresh_token` и текущий `access_token`. При истечении срока OAuth-воркер сам обновляет токены в фоне.
   - Таблица `model_configs`: хранит `original_id`, `alias`, `enabled`. При ручном изменении модели срабатывает триггер `ModelManager::sync_models()`.
2. **Логирование `data/logs.db`**:
   - Все метрики запросов пишутся асинхронно через отдельный поток и MPSC-канал в таблицу `request_analytics`. База запросов изолирована от базы авторизации во избежание блокировок транзакций.
3. **Безопасность**:
   - Не коммитить файлы с расширениями `.db`, `.db-shm`, `.db-wal` и реальные секреты в публичные ветки.
