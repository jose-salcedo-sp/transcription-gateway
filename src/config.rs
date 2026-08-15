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
    pub whisperx_base_url: String,
    pub whisperx_api_key: String,
    pub max_upload_bytes: usize,
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
        let max_upload_mb = env::var("MAX_UPLOAD_MB")
            .unwrap_or_else(|_| "1024".into())
            .parse::<usize>()
            .context("MAX_UPLOAD_MB must be a positive integer")?;

        Ok(Self {
            bind: SocketAddr::new(host, port),
            database_url,
            data_dir,
            gateway_api_key: required("GATEWAY_API_KEY")?,
            whisperx_base_url: env::var("WHISPERX_BASE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8000".into())
                .trim_end_matches('/')
                .to_owned(),
            whisperx_api_key: required("WHISPERX_API_KEY")?,
            max_upload_bytes: max_upload_mb
                .checked_mul(1024 * 1024)
                .context("MAX_UPLOAD_MB is too large")?,
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
