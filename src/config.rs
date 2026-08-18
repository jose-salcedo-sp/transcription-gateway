use std::{
    env,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: SocketAddr,
    pub database_url: String,
    pub data_dir: PathBuf,
    pub gateway_api_key: String,
    pub gateway_worker_key: String,
    pub whisperx_base_url: String,
    pub whisperx_public_base_url: String,
    pub whisperx_api_key: String,
    pub whisperx_upload_secret: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let data_dir = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "./data".into()));
        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| format!("sqlite://{}", data_dir.join("lookup.db").display()));
        let host = env::var("GATEWAY_HOST")
            .unwrap_or_else(|_| "0.0.0.0".into())
            .parse::<IpAddr>()
            .context("GATEWAY_HOST must be an IP address")?;
        let port = env::var("GATEWAY_PORT")
            .unwrap_or_else(|_| "8080".into())
            .parse()
            .context("GATEWAY_PORT must be a port number")?;
        let whisperx_base_url = env::var("WHISPERX_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8000".into())
            .trim_end_matches('/')
            .to_owned();

        let gateway_api_key = required("GATEWAY_API_KEY")?;
        let gateway_worker_key = required("GATEWAY_WORKER_KEY")?;
        if gateway_api_key == gateway_worker_key {
            anyhow::bail!("GATEWAY_API_KEY and GATEWAY_WORKER_KEY must be different");
        }

        Ok(Self {
            bind: SocketAddr::new(host, port),
            database_url,
            data_dir,
            gateway_api_key,
            gateway_worker_key,
            whisperx_public_base_url: env::var("WHISPERX_PUBLIC_BASE_URL")
                .unwrap_or_else(|_| whisperx_base_url.clone())
                .trim_end_matches('/')
                .to_owned(),
            whisperx_base_url,
            whisperx_api_key: required("WHISPERX_API_KEY")?,
            whisperx_upload_secret: required("WHISPERX_UPLOAD_SECRET")?,
        })
    }
}

fn required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.is_empty() {
        anyhow::bail!("{name} cannot be empty");
    }
    Ok(value)
}
