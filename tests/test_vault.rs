use hydra_lite::vault_logger::{decrypt_vault_records, VaultEvent, VaultUsage};
use serde_json::json;
use std::fs;
use std::io::Read;
use std::str::FromStr;

#[test]
fn test_age_zstd_roundtrip() {
    let pubkey = "age1mpplfge35234hc5skryp46at9rawrm2xg72uvkcw0grlny24nv0s04secu";
    let privkey = "AGE-SECRET-KEY-1W0N6VZH32EZ3ZQDPL5UNTZXZDR6RTR6Q22WQNNEHER9DQE2WZ96SXPWP9W";

    let raw_data = b"{\"event\": \"test\", \"content\": \"Zero-Knowledge Vault Logger for Hydra-Lite\"}";
    let compressed = zstd::encode_all(&raw_data[..], 3).expect("zstd compress");

    // Encrypt
    let recipient = age::x25519::Recipient::from_str(pubkey).expect("recipient parse");
    let encryptor = age::Encryptor::with_recipients(vec![Box::new(recipient)]).expect("encryptor");
    let mut encrypted = Vec::new();
    let mut writer = encryptor.wrap_output(&mut encrypted).expect("wrap_output");
    std::io::Write::write_all(&mut writer, &compressed).expect("write");
    writer.finish().expect("finish");

    assert!(!encrypted.is_empty());

    // Decrypt
    let identity = age::x25519::Identity::from_str(privkey).expect("identity parse");
    let decryptor = match age::Decryptor::new(&encrypted[..]).expect("decryptor new") {
        age::Decryptor::Recipients(d) => d,
        _ => panic!("unexpected decryptor type"),
    };

    let mut decrypted_compressed = Vec::new();
    let mut reader = decryptor
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .expect("decrypt");
    reader.read_to_end(&mut decrypted_compressed).expect("read");

    let decompressed = zstd::decode_all(&decrypted_compressed[..]).expect("zstd decompress");
    assert_eq!(decompressed, raw_data);
}

#[tokio::test]
async fn test_vault_logger_integration() {
    let temp_dir = std::env::temp_dir().join("hydra_vault_test_dir");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).unwrap();

    let pubkey = "age1mpplfge35234hc5skryp46at9rawrm2xg72uvkcw0grlny24nv0s04secu";
    let privkey = "AGE-SECRET-KEY-1W0N6VZH32EZ3ZQDPL5UNTZXZDR6RTR6Q22WQNNEHER9DQE2WZ96SXPWP9W";

    let config = hydra_lite::config::VaultConfig {
        enabled: true,
        public_key: pubkey.to_string(),
        vault_dir: temp_dir.to_string_lossy().to_string(),
    };

    let logger = hydra_lite::vault_logger::VaultLogger::init(&config).expect("init logger");

    let event = VaultEvent {
        timestamp: "2026-11-06T21:00:00Z".to_string(),
        endpoint: "openai".to_string(),
        api_key_name: "kiko".to_string(),
        model: "gemini-3.8-flash-high".to_string(),
        stream: false,
        status_code: 200,
        request_payload: json!({
            "messages": [{"role": "user", "content": "Hello Hydra"}]
        }),
        response_payload: json!({
            "content": "Hello Kiko! Everything is encrypted."
        }),
        usage: Some(VaultUsage {
            prompt_tokens: 10,
            completion_tokens: 8,
            cached_tokens: 0,
            total_tokens: 18,
            latency_ms: 120,
        }),
    };

    logger.log(event.clone());

    // Give worker time to flush
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Check files in temp_dir
    let entries: Vec<_> = fs::read_dir(&temp_dir)
        .unwrap()
        .map(|r| r.unwrap().path())
        .collect();
    assert_eq!(entries.len(), 1, "Expected 1 vault file");

    let vault_file_data = fs::read(&entries[0]).unwrap();
    assert!(!vault_file_data.is_empty());

    // Verify decryption
    let decrypted_events = decrypt_vault_records(&vault_file_data, privkey).expect("decryption failed");
    assert_eq!(decrypted_events.len(), 1);
    assert_eq!(decrypted_events[0].api_key_name, "kiko");
    assert_eq!(decrypted_events[0].model, "gemini-3.8-flash-high");
    assert_eq!(decrypted_events[0].response_payload["content"], "Hello Kiko! Everything is encrypted.");

    let _ = fs::remove_dir_all(&temp_dir);
}
