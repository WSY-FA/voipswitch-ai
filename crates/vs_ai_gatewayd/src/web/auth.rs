use axum::http::{HeaderMap, header};
use rand::RngCore;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const SESSION_COOKIE: &str = "vs_ai_session";
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);

#[derive(Clone, Default)]
pub struct SessionStore {
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}

#[derive(Clone)]
struct Entry {
    username: String,
    expires_at: Instant,
}

impl SessionStore {
    pub fn issue(&self, username: String) -> String {
        let mut bytes = [0_u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let token = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.entries.lock().unwrap().insert(
            token.clone(),
            Entry {
                username,
                expires_at: Instant::now() + SESSION_TTL,
            },
        );
        token
    }

    pub fn username(&self, token: &str) -> Option<String> {
        let mut entries = self.entries.lock().unwrap();
        if entries
            .get(token)
            .is_some_and(|entry| entry.expires_at <= Instant::now())
        {
            entries.remove(token);
            return None;
        }
        entries.get(token).map(|entry| entry.username.clone())
    }

    pub fn revoke(&self, token: &str) {
        self.entries.lock().unwrap().remove(token);
    }
}

pub fn cookie_value(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| (name == SESSION_COOKIE).then(|| value.to_string()))
}

pub fn session_cookie(token: &str) -> String {
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
        SESSION_TTL.as_secs()
    )
}

pub fn expired_session_cookie() -> String {
    format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0")
}
