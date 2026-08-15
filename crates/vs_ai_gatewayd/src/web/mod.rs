mod auth;
mod handlers;

use ai_gateway::Gateway;
use std::sync::Arc;

#[derive(Clone)]
pub struct WebState {
    pub gateway: Arc<Gateway>,
    pub sessions: auth::SessionStore,
}

pub use handlers::router;
