use crate::web::WebState;
use crate::web::auth::{cookie_value, expired_session_cookie, session_cookie};
use ai_gateway::{GatewayCatalog, GatewayProfileConfig, ProviderUpsertRequest};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tracing::warn;

type SharedState = Arc<WebState>;

const INDEX_HTML: &str = include_str!("../../static/index.html");
const APP_CSS: &str = include_str!("../../static/app.css");
const APP_JS: &str = include_str!("../../static/app.js");
const ZH_CN: &str = include_str!("../../static/i18n/zh-CN.json");
const EN_US: &str = include_str!("../../static/i18n/en-US.json");

pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/", get(index))
        .route("/static/app.css", get(styles))
        .route("/static/app.js", get(app_js))
        .route("/static/i18n/zh-CN.json", get(zh_cn))
        .route("/static/i18n/en-US.json", get(en_us))
        .route("/api/health", get(health))
        .route("/api/auth/login", axum::routing::post(login))
        .route("/api/auth/logout", axum::routing::post(logout))
        .route("/api/auth/me", get(current_user))
        .route("/api/catalog", get(catalog))
        .route("/api/providers", put(upsert_provider))
        .route("/api/providers/:provider_id", delete(delete_provider))
        .route("/api/profiles", put(upsert_profile))
        .route(
            "/api/profiles/:profile_id",
            put(update_profile).delete(delete_profile),
        )
}

async fn index() -> impl IntoResponse {
    content("text/html; charset=utf-8", INDEX_HTML)
}

async fn styles() -> impl IntoResponse {
    content("text/css; charset=utf-8", APP_CSS)
}

async fn app_js() -> impl IntoResponse {
    content("application/javascript; charset=utf-8", APP_JS)
}

async fn zh_cn() -> impl IntoResponse {
    content("application/json; charset=utf-8", ZH_CN)
}

async fn en_us() -> impl IntoResponse {
    content("application/json; charset=utf-8", EN_US)
}

async fn health(State(state): State<SharedState>) -> Response {
    match state.gateway.gateway_catalog() {
        Ok(catalog) => Json(json!({
            "ok": true,
            "catalog_version": catalog.version,
        }))
        .into_response(),
        Err(error) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "AI_GATEWAY_UNAVAILABLE",
            &error.to_string(),
        ),
    }
}

#[derive(Deserialize)]
struct LoginPayload {
    username: String,
    password: String,
}

async fn login(State(state): State<SharedState>, Json(payload): Json<LoginPayload>) -> Response {
    let username = payload.username;
    let authenticated = state
        .gateway
        .authenticate_admin(&username, &payload.password)
        .unwrap_or(false);
    if !authenticated {
        warn!(username = %username, "AI gateway web login failed");
        return api_error(
            StatusCode::UNAUTHORIZED,
            "AUTH_FAILED",
            "invalid username or password",
        );
    }
    let token = state.sessions.issue(username.clone());
    let mut response = Json(json!({ "ok": true, "username": username })).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&session_cookie(&token)).expect("valid session cookie"),
    );
    response
}

async fn logout(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Some(token) = cookie_value(&headers) {
        state.sessions.revoke(&token);
    }
    let mut response = Json(json!({ "ok": true })).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&expired_session_cookie()).expect("valid expired cookie"),
    );
    response
}

async fn current_user(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    match authenticated(&state, &headers) {
        Some(username) => Json(json!({ "ok": true, "username": username })).into_response(),
        None => auth_required(),
    }
}

async fn catalog(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if authenticated(&state, &headers).is_none() {
        return auth_required();
    }
    match state.gateway.gateway_catalog() {
        Ok(catalog) => Json(json!({ "ok": true, "catalog": catalog })).into_response(),
        Err(error) => api_error(
            StatusCode::BAD_GATEWAY,
            "AI_GATEWAY_UNAVAILABLE",
            &error.to_string(),
        ),
    }
}

async fn upsert_provider(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(provider): Json<ProviderUpsertRequest>,
) -> Response {
    if authenticated(&state, &headers).is_none() {
        return auth_required();
    }
    match state.gateway.upsert_provider(provider) {
        Ok(catalog) => catalog_response(catalog),
        Err(error) => api_error(
            StatusCode::BAD_REQUEST,
            "AI_ADMIN_REJECTED",
            &error.to_string(),
        ),
    }
}

async fn upsert_profile(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(profile): Json<GatewayProfileConfig>,
) -> Response {
    if authenticated(&state, &headers).is_none() {
        return auth_required();
    }
    match state.gateway.upsert_profile(profile) {
        Ok(catalog) => catalog_response(catalog),
        Err(error) => api_error(
            StatusCode::BAD_REQUEST,
            "AI_ADMIN_REJECTED",
            &error.to_string(),
        ),
    }
}

async fn update_profile(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::extract::Path(profile_id): axum::extract::Path<String>,
    Json(profile): Json<GatewayProfileConfig>,
) -> Response {
    if authenticated(&state, &headers).is_none() {
        return auth_required();
    }
    if profile.profile_id != profile_id {
        return api_error(
            StatusCode::BAD_REQUEST,
            "AI_ADMIN_REJECTED",
            "AI profile ID cannot be changed",
        );
    }
    match state.gateway.upsert_profile(profile) {
        Ok(catalog) => catalog_response(catalog),
        Err(error) => api_error(
            StatusCode::BAD_REQUEST,
            "AI_ADMIN_REJECTED",
            &error.to_string(),
        ),
    }
}

#[derive(Deserialize)]
struct DeleteProviderPayload {
    expected_revision: u64,
}

#[derive(Deserialize)]
struct DeleteProfilePayload {
    expected_revision: u64,
}

async fn delete_provider(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::extract::Path(provider_id): axum::extract::Path<String>,
    Json(payload): Json<DeleteProviderPayload>,
) -> Response {
    if authenticated(&state, &headers).is_none() {
        return auth_required();
    }
    match state
        .gateway
        .delete_provider(&provider_id, payload.expected_revision)
    {
        Ok(catalog) => catalog_response(catalog),
        Err(error) => api_error(
            StatusCode::BAD_REQUEST,
            "AI_ADMIN_REJECTED",
            &error.to_string(),
        ),
    }
}

async fn delete_profile(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::extract::Path(profile_id): axum::extract::Path<String>,
    Json(payload): Json<DeleteProfilePayload>,
) -> Response {
    if authenticated(&state, &headers).is_none() {
        return auth_required();
    }
    match state
        .gateway
        .delete_profile(&profile_id, payload.expected_revision)
    {
        Ok(catalog) => catalog_response(catalog),
        Err(error) => api_error(
            StatusCode::BAD_REQUEST,
            "AI_ADMIN_REJECTED",
            &error.to_string(),
        ),
    }
}

fn catalog_response(catalog: GatewayCatalog) -> Response {
    Json(json!({ "ok": true, "catalog": catalog })).into_response()
}

fn authenticated(state: &SharedState, headers: &HeaderMap) -> Option<String> {
    state.sessions.username(&cookie_value(headers)?)
}

fn auth_required() -> Response {
    api_error(StatusCode::UNAUTHORIZED, "AUTH_REQUIRED", "login required")
}

fn content(content_type: &str, body: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

fn api_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "ok": false,
            "error": { "code": code, "message": message },
        })),
    )
        .into_response()
}
