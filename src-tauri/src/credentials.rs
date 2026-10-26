use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use keyring::Entry;

const SERVICE_NAME: &str = "com.veloxdb.app";
const OPENROUTER_API_KEY_ACCOUNT: &str = "openrouter-api-key";

/// Serialises every keychain call. On Linux each call opens its own encrypted
/// Secret Service session, and a burst of parallel calls (one per connection
/// when the sidebar and health checks load) can crash gnome-keyring-daemon.
static KEYCHAIN_LOCK: Mutex<()> = Mutex::new(());

/// Secrets already read or written this session, so repeat lookups (health
/// checks, reconnects) don't go back to the keychain at all.
fn cache() -> &'static Mutex<HashMap<String, String>> {
    static CACHE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn store_secret(account: &str, secret: &str) -> Result<(), keyring::Error> {
    let _guard = lock(&KEYCHAIN_LOCK);
    Entry::new(SERVICE_NAME, account)?.set_password(secret)?;
    lock(cache()).insert(account.to_string(), secret.to_string());
    Ok(())
}

fn get_secret(account: &str) -> Result<Option<String>, keyring::Error> {
    if let Some(secret) = lock(cache()).get(account).cloned() {
        return Ok(Some(secret));
    }
    let _guard = lock(&KEYCHAIN_LOCK);
    // Another caller may have filled the cache while this one waited.
    if let Some(secret) = lock(cache()).get(account).cloned() {
        return Ok(Some(secret));
    }
    match Entry::new(SERVICE_NAME, account)?.get_password() {
        Ok(secret) => {
            lock(cache()).insert(account.to_string(), secret.clone());
            Ok(Some(secret))
        }
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(e),
    }
}

fn delete_secret(account: &str) -> Result<(), keyring::Error> {
    let _guard = lock(&KEYCHAIN_LOCK);
    lock(cache()).remove(account);
    match Entry::new(SERVICE_NAME, account)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn store_password(connection_id: &str, password: &str) -> Result<(), String> {
    store_secret(connection_id, password)
        .map_err(|e| format!("Failed to store password in keychain: {}", e))
}

pub fn get_password(connection_id: &str) -> Result<Option<String>, String> {
    get_secret(connection_id).map_err(|e| format!("Failed to read password from keychain: {}", e))
}

pub fn delete_password(connection_id: &str) -> Result<(), String> {
    delete_secret(connection_id).map_err(|e| format!("Failed to delete password from keychain: {}", e))
}

pub fn store_openrouter_api_key(api_key: &str) -> Result<(), String> {
    store_secret(OPENROUTER_API_KEY_ACCOUNT, api_key)
        .map_err(|e| format!("Failed to store API key in keychain: {}", e))
}

pub fn get_openrouter_api_key() -> Result<Option<String>, String> {
    get_secret(OPENROUTER_API_KEY_ACCOUNT).map_err(|e| format!("Failed to read API key from keychain: {}", e))
}

pub fn delete_openrouter_api_key() -> Result<(), String> {
    delete_secret(OPENROUTER_API_KEY_ACCOUNT).map_err(|e| format!("Failed to delete API key from keychain: {}", e))
}
