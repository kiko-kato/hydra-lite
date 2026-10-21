use anyhow::Result;
use log::info;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024)
        .build()?;
    rt.block_on(async_main())
}

async fn async_main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    info!("===============================================================");
    info!("   HYDRA-LITE API GATEWAY v2.0.0");
    info!("   * Pure direct upstream (Zero Proxy overhead)");
    info!("   * Adaptive model manager with auto-alias generation");
    info!("   * Self-healing Antidote engine + Circuit Breaker");
    info!("===============================================================");

    let mut config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
    if env::var("CONFIG_PATH").is_err() {
        if let Ok(exe) = env::current_exe() {
            if let Some(parent) = exe.parent() {
                let candidate = parent.join("config.toml");
                if candidate.exists() {
                    config_path = candidate.to_string_lossy().to_string();
                }
            }
        }
    }

    let path = Path::new(&config_path);
    if !path.exists() {
        let example = Path::new("config.example.toml");
        if example.exists() {
            info!("No config.toml found, creating default from config.example.toml...");
            let _ = fs::copy(example, "config.toml");
        }
    }

    let config = hydra_lite::Config::load(&config_path)
        .or_else(|_| hydra_lite::Config::load("config.example.toml"))
        .map_err(|e| anyhow::anyhow!("Failed to load configuration file {}: {}", config_path, e))?;

    info!("Loaded configuration from {}", config_path);

    // Подготовка каталогов для БД
    let db_path = PathBuf::from(&config.database.path);
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = fs::create_dir_all(parent);
        }
    }

    let logs_db_path = PathBuf::from(&config.database.logs_db_path);
    if let Some(parent) = logs_db_path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = fs::create_dir_all(parent);
        }
    }

    let config_arc = Arc::new(config);

    // Запуск HTTP/SSE роутера Hydra-Lite
    let router_cfg = Arc::clone(&config_arc);
    let router_handle = tokio::spawn(async move {
        if let Err(e) = hydra_lite::run_router(router_cfg).await {
            log::error!("[ROUTER] Fatal error: {}", e);
        }
    });

    info!("Hydra-Lite Gateway is running!");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal. Stopping Hydra-Lite...");
        }
    }

    router_handle.abort();
    info!("Hydra-Lite stopped cleanly. Bye!");
    Ok(())
}
