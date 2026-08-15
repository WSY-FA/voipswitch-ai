mod logging;
mod web;

use ai_gateway::GatewayConfig;
use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::info;
use vs_ai_gatewayd::server;

#[derive(Debug, Parser)]
#[command(name = "vs_ai_gatewayd")]
#[command(about = "VoIPSwitch PBX-neutral AI gateway")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    control_socket: Option<PathBuf>,

    #[arg(long)]
    media_socket: Option<PathBuf>,

    #[arg(long, default_value = "0.0.0.0:18082")]
    web_bind: SocketAddr,

    #[arg(long)]
    bootstrap_admin_password_file: Option<PathBuf>,

    #[arg(long)]
    log_dir: Option<PathBuf>,

    #[arg(long, default_value = "local")]
    instance_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut config = load_config(args.config.as_deref())?;
    if let Some(data_dir) = args.data_dir {
        config.data_dir = data_dir;
    }
    config.validate()?;
    let log_dir = args.log_dir.unwrap_or_else(|| PathBuf::from("logs"));
    let _log_guard = logging::init(&log_dir)?;
    let control_socket = args
        .control_socket
        .unwrap_or_else(|| default_socket_path("ai-control.sock"));
    let media_socket = args
        .media_socket
        .unwrap_or_else(|| default_socket_path("ai-media.sock"));
    let gateway = ai_gateway::Gateway::open_configured(config.clone(), args.instance_id.clone())?;
    let password = match args.bootstrap_admin_password_file.as_deref() {
        Some(path) => std::fs::read_to_string(path)
            .with_context(|| format!("read gateway bootstrap password {}", path.display()))?
            .trim_end_matches(['\r', '\n'])
            .to_string(),
        None => std::env::var("AI_GATEWAY_ADMIN_PASSWORD").unwrap_or_else(|_| "admin".to_string()),
    };
    if gateway.bootstrap_admin(&password, ai_protocol::time::unix_timestamp_ms())? {
        info!("gateway admin account initialized");
    }
    info!(
        instance_id = %args.instance_id,
        data_dir = %config.data_dir.display(),
        control_socket = %control_socket.display(),
        media_socket = %media_socket.display(),
        web_bind = %args.web_bind,
        "vs_ai_gatewayd starting"
    );

    let control = tokio::spawn(server::run_control_socket(
        gateway.clone(),
        control_socket.clone(),
    ));
    let media = tokio::spawn(server::run_media_socket(
        gateway.clone(),
        media_socket.clone(),
    ));
    let web_state = Arc::new(web::WebState {
        gateway: gateway.clone(),
        sessions: Default::default(),
    });
    let web_listener = TcpListener::bind(args.web_bind).await?;
    let web = tokio::spawn(async move {
        axum::serve(web_listener, web::router().with_state(web_state))
            .with_graceful_shutdown(web_shutdown_signal())
            .await
            .context("AI gateway web server")
    });
    tokio::select! {
        _ = signal::ctrl_c() => info!("AI gateway shutdown signal received"),
        result = control => result.context("control socket task join")??,
        result = media => result.context("media socket task join")??,
        result = web => result.context("web task join")??,
    }
    let _ = std::fs::remove_file(control_socket);
    let _ = std::fs::remove_file(media_socket);
    Ok(())
}

async fn web_shutdown_signal() {
    let _ = signal::ctrl_c().await;
}

fn load_config(path: Option<&Path>) -> Result<GatewayConfig> {
    let Some(path) = path else {
        return Ok(GatewayConfig::default());
    };
    let bytes =
        std::fs::read(path).with_context(|| format!("read gateway config {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse gateway config {}", path.display()))
}

fn default_socket_path(file_name: &str) -> PathBuf {
    if let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime_dir)
            .join("voipswitch")
            .join(file_name);
    }
    PathBuf::from("/tmp/voipswitch").join(file_name)
}
