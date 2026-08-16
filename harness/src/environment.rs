use std::{
    env,
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    pub name: &'static str,
    pub address: SocketAddr,
    pub reachable: bool,
}

pub fn local_service_statuses() -> Result<Vec<ServiceStatus>> {
    [
        ("toxiproxy-api", "HARNESS_TOXIPROXY_PORT", 18_474),
        ("storage-a-proxy", "HARNESS_PROXY_A_PORT", 12_100),
        ("storage-b-proxy", "HARNESS_PROXY_B_PORT", 12_101),
        ("storage-c-proxy", "HARNESS_PROXY_C_PORT", 12_102),
    ]
    .into_iter()
    .map(|(name, variable, default_port)| {
        let port = env::var(variable)
            .unwrap_or_else(|_| default_port.to_string())
            .parse::<u16>()
            .with_context(|| format!("{variable} must be a valid TCP port"))?;
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let reachable = TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_ok();
        Ok(ServiceStatus {
            name,
            address,
            reachable,
        })
    })
    .collect()
}
