mod auth;
mod handlers;

use ai_gateway::Gateway;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone)]
pub struct WebState {
    pub gateway: Arc<Gateway>,
    pub sessions: auth::SessionStore,
    pub ai_ops_tasks: Arc<Mutex<BTreeMap<String, serde_json::Value>>>,
}

pub use handlers::router;
