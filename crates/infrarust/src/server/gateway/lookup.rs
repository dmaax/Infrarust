use std::sync::Arc;

use infrarust_config::{ServerConfig, models::logging::LogType};
use tracing::{debug, instrument};

use super::Gateway;

impl Gateway {
    #[instrument(skip(self), fields(domain = %domain), level = "debug")]
    pub(crate) async fn find_server(&self, domain: &str) -> Option<Arc<ServerConfig>> {
        debug!(
            log_type = LogType::Authentication.as_str(),
            "Finding server by domain: {}", domain
        );
        let configs = self
            .shared
            .configuration_service()
            .get_all_configurations()
            .await;
        debug!(
            log_type = LogType::Authentication.as_str(),
            "Got {} total server configurations",
            configs.len()
        );

        let result = self
            .shared
            .configuration_service()
            .find_server_by_domain(domain)
            .await;

        debug!(
            domain = %domain,
            found = result.is_some(),
            "Domain lookup result"
        );

        if result.is_some() {
            debug!(
                log_type = LogType::Authentication.as_str(),
                "Found server for domain {}", domain
            );
        } else {
            debug!(
                log_type = LogType::Authentication.as_str(),
                "No server found for domain {}", domain
            );
        }

        result
    }

    /// Like `find_server` but ignores configurations that have no TCP
    /// addresses.  This is used when handling a TCP connection (status/login)
    /// to avoid selecting an UDP-only backend.
    pub(crate) async fn find_server_for_tcp(&self, domain: &str) -> Option<Arc<ServerConfig>> {
        // first, perform the normal lookup
        if let Some(cfg) = self.find_server(domain).await {
            if !cfg.addresses.is_empty() {
                return Some(cfg);
            }
            // the matched config has no addresses; look for another matching
            // config that does
            let configs = self
                .shared
                .configuration_service()
                .get_all_configurations()
                .await;
            for sc in configs.values() {
                if sc.addresses.is_empty() {
                    continue;
                }
                if sc
                    .domains
                    .iter()
                    .any(|pattern| wildmatch::WildMatch::new(pattern).matches(domain))
                {
                    return Some(sc.clone());
                }
            }
            // no suitable TCP-capable config
            None
        } else {
            None
        }
    }

    pub async fn get_server_from_ip(&self, ip: &str) -> Option<Arc<ServerConfig>> {
        self.shared
            .configuration_service()
            .find_server_by_ip(ip)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infrarust_config::models::server::ServerConfig;
    use std::sync::Arc;

    async fn make_gateway_with_configs(configs: Vec<ServerConfig>) -> Gateway {
        // construct a minimal Infrarust instance which provides a SharedComponent
        let shutdown = crate::cli::ShutdownController::new();
        let ir = Arc::new(
            crate::Infrarust::new(
                infrarust_config::InfrarustConfig::default(),
                shutdown.clone(),
            )
            .unwrap(),
        );
        let shared = ir.get_shared();
        for cfg in configs {
            shared
                .configuration_service()
                .update_configurations(vec![cfg])
                .await;
        }
        Gateway::new(shared)
    }

    #[tokio::test]
    async fn test_find_server_for_tcp_prefers_with_addresses() {
        let cfg1 = ServerConfig {
            domains: vec!["foo".into()],
            addresses: vec![],
            udp_addresses: Some(vec!["1.1.1.1:1".into()]),
            ..Default::default()
        };
        let cfg2 = ServerConfig {
            domains: vec!["foo".into()],
            addresses: vec!["2.2.2.2:2".into()],
            udp_addresses: None,
            ..Default::default()
        };

        let gw = make_gateway_with_configs(vec![cfg1, cfg2]).await;
        let found = gw.find_server_for_tcp("foo").await.unwrap();
        assert_eq!(
            found.addresses.first().map(String::as_str),
            Some("2.2.2.2:2")
        );
    }

    #[tokio::test]
    async fn test_find_server_for_tcp_none_if_only_udp() {
        let cfg = ServerConfig {
            domains: vec!["bar".into()],
            addresses: vec![],
            udp_addresses: Some(vec!["3.3.3.3:3".into()]),
            ..Default::default()
        };

        let gw = make_gateway_with_configs(vec![cfg]).await;
        assert!(gw.find_server_for_tcp("bar").await.is_none());
    }
}
