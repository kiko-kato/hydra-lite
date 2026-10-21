use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

const MAX_SIGNATURES: usize = 5000;
const DEFAULT_FALLBACK_SIGNATURE: &str = "EnEKbwERTTIPYQ9OCnkX9Vplopq5KYtn8oYGQaWaSq5suQNWgs/KGB7EgbHpEKqm1nbgqeVJ1DEJaUHxWRDBEL1MY/4XeCmAHS+tlarTfDJbt97uZpk0gOhkac+wz5fPS2IPJnHbR/QV5BHwENCfXwO2aQ==";

#[derive(Clone)]
pub struct SignatureStore {
    inner: Arc<RwLock<SignatureStoreInner>>,
}

struct SignatureStoreInner {
    map: HashMap<String, String>,
    order: VecDeque<String>,
    latest: Option<String>,
}

impl SignatureStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SignatureStoreInner {
                map: HashMap::with_capacity(MAX_SIGNATURES),
                order: VecDeque::with_capacity(MAX_SIGNATURES),
                latest: None,
            })),
        }
    }

    pub fn insert(&self, key: String, signature: String) {
        if signature.trim().is_empty() {
            return;
        }
        let mut inner = self.inner.write();
        inner.latest = Some(signature.clone());
        if inner.map.insert(key.clone(), signature).is_none() {
            inner.order.push_back(key);
            if inner.order.len() > MAX_SIGNATURES {
                if let Some(old_key) = inner.order.pop_front() {
                    inner.map.remove(&old_key);
                }
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let inner = self.inner.read();
        inner.map.get(key).cloned()
    }

    pub fn get_with_fallback(&self, cid: &str, fname: &str, args_key: &str) -> String {
        let inner = self.inner.read();

        // 1. Поиск по call_id
        if !cid.is_empty() {
            if let Some(sig) = inner.map.get(cid) {
                return sig.clone();
            }
        }

        // 2. Поиск по ключу args_key (fname:args)
        if !args_key.is_empty() {
            if let Some(sig) = inner.map.get(args_key) {
                return sig.clone();
            }
        }

        // 3. Поиск по имени функции fname
        if !fname.is_empty() {
            if let Some(sig) = inner.map.get(fname) {
                return sig.clone();
            }
        }

        // 4. Fallback к последней сохраненной сигнатуре
        if let Some(ref sig) = inner.latest {
            return sig.clone();
        }

        // 5. Гарантированная базовая сигнатура, предотвращающая 400 Bad Request от Google
        DEFAULT_FALLBACK_SIGNATURE.to_string()
    }
}
