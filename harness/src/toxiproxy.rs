use std::{
    env,
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use serde_json::json;

#[derive(Debug, Clone, Copy)]
pub enum ProxyReplica {
    A,
    B,
    C,
}

pub fn set_enabled(replica: ProxyReplica, enabled: bool) -> Result<()> {
    request(
        "POST",
        &format!("/proxies/{}", proxy_name(replica)),
        Some(json!({ "enabled": enabled })),
    )
}

pub fn add_latency(replica: ProxyReplica, latency_ms: u64, jitter_ms: u64) -> Result<()> {
    let proxy = proxy_name(replica);
    for (name, stream) in [
        ("overmesh-latency-downstream", "downstream"),
        ("overmesh-latency-upstream", "upstream"),
    ] {
        let path = format!("/proxies/{proxy}/toxics/{name}");
        let _ = request("DELETE", &path, None);
        request(
            "POST",
            &format!("/proxies/{proxy}/toxics"),
            Some(json!({
                "name": name,
                "type": "latency",
                "stream": stream,
                "toxicity": 1.0,
                "attributes": {
                    "latency": latency_ms,
                    "jitter": jitter_ms
                }
            })),
        )?;
    }
    Ok(())
}

pub fn reset() -> Result<()> {
    request("POST", "/reset", None)
}

fn proxy_name(replica: ProxyReplica) -> &'static str {
    match replica {
        ProxyReplica::A => "storage-a",
        ProxyReplica::B => "storage-b",
        ProxyReplica::C => "storage-c",
    }
}

fn request(method: &str, path: &str, body: Option<serde_json::Value>) -> Result<()> {
    let port = env::var("HARNESS_TOXIPROXY_PORT")
        .unwrap_or_else(|_| "18474".to_owned())
        .parse::<u16>()
        .context("HARNESS_TOXIPROXY_PORT must be a valid TCP port")?;
    let url = format!("http://127.0.0.1:{port}{path}");
    let mut command = Command::new("curl");
    command
        .args(["--fail", "--silent", "--show-error", "-X", method, &url])
        .stdout(Stdio::null());
    if let Some(body) = body {
        command
            .args(["-H", "Content-Type: application/json", "--data"])
            .arg(body.to_string());
    }
    let output = command
        .output()
        .with_context(|| format!("failed to invoke curl for {method} {url}"))?;
    if !output.status.success() {
        bail!(
            "Toxiproxy request {method} {url} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}
