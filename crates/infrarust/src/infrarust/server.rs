use std::{io, sync::Arc};

use infrarust_config::models::logging::LogType;
use infrarust_protocol::minecraft::java::handshake::ServerBoundHandshake;
use infrarust_protocol::version::Version;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{Instrument, Span, debug, debug_span, error, info, instrument, warn};
use uuid::Uuid;

use crate::{Connection, Infrarust, core::error::SendError, server::ServerRequest};

impl Infrarust {
    pub async fn run(self: Arc<Self>) -> Result<(), SendError> {
        debug!(
            log_type = LogType::Supervisor.as_str(),
            "Starting Infrarust server"
        );
        let bind_addr = self.shared.config().bind.clone().unwrap_or_default();

        // Create TCP listener
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(
                    log_type = LogType::TcpConnection.as_str(),
                    "Failed to bind to {}: {}", bind_addr, e
                );
                self.shared
                    .shutdown_controller()
                    .trigger_shutdown(&format!("Failed to bind: {}", e))
                    .await;
                return Err(SendError::new(e));
            }
        };

        info!(
            log_type = LogType::TcpConnection.as_str(),
            "Listening on {}", bind_addr
        );

        // optionally open a UDP socket for Bedrock/Geyser
        if let Some(udp_bind) = &self.shared.config().udp_bind {
            match tokio::net::UdpSocket::bind(udp_bind).await {
                Ok(socket) => {
                    info!(
                        log_type = LogType::TcpConnection.as_str(),
                        "UDP listener bound to {}", udp_bind
                    );
                    let self_clone = Arc::clone(&self);
                    tokio::spawn(async move {
                        self_clone.run_udp(socket).await;
                    });
                }
                Err(e) => {
                    error!(
                        log_type = LogType::TcpConnection.as_str(),
                        "Failed to bind UDP socket {}: {}", udp_bind, e
                    );
                    // not fatal; continue running tcp listener
                }
            }
        }

        // Get a shutdown receiver
        let mut shutdown_rx = self.shared.shutdown_controller().subscribe().await;
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!(log_type = LogType::Supervisor.as_str(), "Received shutdown signal, stopping server");
                    break;
                }

                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let self_guard = Arc::clone(&self);
                            let session_id = Uuid::new_v4();
                            let _span = debug_span!("TCP Connection", %addr, %session_id, log_type = LogType::TcpConnection.as_str());
                            debug!(log_type = LogType::TcpConnection.as_str(), "New TCP connection accepted : ({})[{}]", addr, session_id);

                            tokio::spawn(async move {
                                debug!(log_type = LogType::TcpConnection.as_str(), "Starting connection processing for ({})[{}]", addr, session_id);

                                match async {

                                    self_guard.shared.filter_registry().filter(&stream).await?;


                                    debug!(log_type = LogType::Filter.as_str(), "Connection passed filters for ({})[{}]", addr, session_id);

                                 let conn = if self_guard.shared.config().proxy_protocol.is_some() {
                                        debug!(log_type = LogType::ProxyProtocol.as_str(), "Using proxy protocol sent by client");
                                        Connection::with_proxy_protocol(
                                            stream,
                                            session_id,
                                            self_guard.shared.config().proxy_protocol.as_ref()
                                        )
                                        .instrument(debug_span!("New connection with proxy protocol", log_type = LogType::ProxyProtocol.as_str()))
                                        .await?
                                    } else {
                                        debug!(log_type = LogType::TcpConnection.as_str(), "Not using proxy protocol");
                                        Connection::new(stream, session_id)
                                            .instrument(debug_span!("New connection", log_type = LogType::TcpConnection.as_str()))
                                            .await?
                                    };
                                    debug!(log_type = LogType::TcpConnection.as_str(), "Connection established for ({})[{}]", addr, session_id);

                                    self_guard.handle_connection(conn).await
                                }.await {
                                    Ok(_) => debug!(log_type = LogType::TcpConnection.as_str(), "Connection from {} completed successfully", addr),
                                    Err(e) => error!(log_type = LogType::TcpConnection.as_str(), "Connection error from {}: {}", addr, e),
                                }
                            });
                        }
                        Err(e) => {
                            error!(log_type = LogType::TcpConnection.as_str(), "Accept error: {}", e);
                            if e.kind() == io::ErrorKind::Interrupted {
                                break;
                            }
                        }
                    }
                },
            }
        }

        info!(
            log_type = LogType::Supervisor.as_str(),
            "Server stopped accepting new connections"
        );
        Ok(())
    }

    #[instrument(name = "connection_flow", skip(client), fields(log_type = LogType::TcpConnection.as_str()))]
    pub(crate) async fn handle_connection(&self, mut client: Connection) -> io::Result<()> {
        let peer_addr = client.peer_addr().await?;
        Span::current().record("peer_addr", format!("{}", peer_addr));

        debug!(
            log_type = LogType::TcpConnection.as_str(),
            "Starting to process new connection from {}", peer_addr
        );

        let handshake_timeout_secs = self.shared.config().handshake_timeout_secs.unwrap_or(10);

        // Peek the first byte to detect legacy protocol before reading any packets
        let first_byte = match tokio::time::timeout(
            tokio::time::Duration::from_secs(handshake_timeout_secs),
            client.peek_first_byte(),
        )
        .await
        {
            Ok(Ok(byte)) => byte,
            Ok(Err(e)) => {
                debug!(
                    log_type = LogType::TcpConnection.as_str(),
                    "Failed to peek first byte: {}", e
                );
                let _ = client.close().await;
                return Err(e);
            }
            Err(_) => {
                debug!(
                    log_type = LogType::TcpConnection.as_str(),
                    "Timeout waiting for first byte after {}s", handshake_timeout_secs
                );
                let _ = client.close().await;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "First byte timeout",
                ));
            }
        };

        let client_addr = peer_addr;
        let session_id = client.session_id;

        match first_byte {
            0xFE => {
                // Legacy server list ping (Beta 1.8 through 1.6)
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Detected legacy ping (0xFE) from {}", client_addr
                );
                crate::server::legacy_handler::handle_legacy_ping(
                    &mut client,
                    &self.gateway,
                    session_id,
                    client_addr,
                )
                .await
            }
            0x02 => {
                // Legacy login handshake (Beta 1.8 through 1.6)
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Detected legacy login handshake (0x02) from {}", client_addr
                );
                crate::server::legacy_handler::handle_legacy_login(
                    client,
                    &self.gateway,
                    session_id,
                    client_addr,
                )
                .await
            }
            _ => {
                // Modern protocol (1.7+) — VarInt-prefixed packets
                self.handle_modern_connection(client, handshake_timeout_secs)
                    .await
            }
        }
    }

    /// Handle a modern (1.7+) Minecraft connection with VarInt-framed packets.
    async fn run_udp(self: Arc<Self>, socket: tokio::net::UdpSocket) {
        use std::{collections::HashMap, net::SocketAddr, sync::Arc as StdArc};
        use uuid::Uuid;

        let socket_arc = StdArc::new(socket);
        let mut buf = [0u8; 2048];

        // mapping from client address -> (backend address, session id)
        let mut sessions: HashMap<SocketAddr, (SocketAddr, Uuid)> = HashMap::new();
        // reverse lookup: backend address -> client address
        let mut reverse: HashMap<SocketAddr, SocketAddr> = HashMap::new();

        let mut shutdown_rx = self.shared.shutdown_controller().subscribe().await;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!(log_type = LogType::Supervisor.as_str(), "UDP listener shutting down");
                    break;
                },
                result = socket_arc.recv_from(&mut buf) => {
                    if let Ok((len, addr)) = result {
                        debug!(log_type = LogType::TcpConnection.as_str(), "Received {} UDP bytes from {}", len, addr);
                        let mut data = &buf[..len];
                        let mut _original_addr: Option<SocketAddr> = None;

                        // proxy-protocol header is handled only once, on the first packet
                        if let Some(proxy_config) = &self.shared.config().proxy_protocol {
                            if proxy_config.receive_enabled {
                                let reader = crate::network::proxy_protocol::reader::ProxyProtocolReader::new(
                                    proxy_config.receive_enabled,
                                    proxy_config.receive_timeout_secs.unwrap_or(5),
                                    proxy_config.receive_allowed_versions.clone(),
                                );
                                if let Ok((addr_opt, consumed)) = reader.parse_header_bytes(data) {
                                    _original_addr = addr_opt;
                                    data = &data[consumed..];
                                }
                            }
                        }

                        // apply filters before doing any session work
                        if let Err(e) = self.shared.filter_registry().filter_udp(&addr, data).await {
                            warn!(log_type = LogType::Filter.as_str(), "UDP packet from {} blocked by filter: {}", addr, e);
                            continue;
                        }

                        if let Err(e) = self
                            .process_udp_packet(&socket_arc, &mut sessions, &mut reverse, addr, data)
                            .await
                        {
                            debug!(log_type = LogType::PacketProcessing.as_str(), "Dropping UDP packet from {}: {}", addr, e);
                        }
                    } else if let Err(e) = result {
                        warn!(log_type = LogType::TcpConnection.as_str(), "UDP recv error: {}", e);
                        break;
                    }
                },
            }
        }
    }

    async fn process_udp_packet(
        &self,
        socket: &Arc<tokio::net::UdpSocket>,
        sessions: &mut HashMap<SocketAddr, (SocketAddr, Uuid)>,
        reverse: &mut HashMap<SocketAddr, SocketAddr>,
        peer: SocketAddr,
        data: &[u8],
    ) -> io::Result<()> {
        debug!(
            log_type = LogType::PacketProcessing.as_str(),
            "Processing UDP packet from {} ({} bytes)",
            peer,
            data.len()
        );

        // if packet originates from a backend that we previously forwarded to,
        // send it back to the associated client
        if let Some(client) = reverse.get(&peer) {
            debug!(
                log_type = LogType::PacketProcessing.as_str(),
                "UDP packet from backend {} -> forward to client {}", peer, client
            );
            socket.send_to(data, client).await?;
            return Ok(());
        }

        // packet came from a client
        let backend_addr = if let Some((backend, _)) = sessions.get(&peer) {
            *backend
        } else {
            // new session, figure out which backend server should handle this
            let lookup_peer = peer; // could use original_addr if we tracked it
            let backend = self.determine_backend_for_peer(lookup_peer, data).await?;
            let session_id = Uuid::new_v4();
            sessions.insert(peer, (backend, session_id));
            reverse.insert(backend, peer);
            backend
        };

        debug!(
            log_type = LogType::PacketProcessing.as_str(),
            "Forwarding {} bytes from client {} to backend {}",
            data.len(),
            peer,
            backend_addr
        );
        socket.send_to(data, backend_addr).await?;
        Ok(())
    }

    /// Determine which backend address should handle traffic from `peer`.
    ///
    /// This currently uses a very naive domain extractor; the first whitespace
    /// separated token containing a `.` is considered a hostname and is looked up
    /// via the gateway.  If that fails, the first configured server is used.
    async fn determine_backend_for_peer(
        &self,
        _peer: std::net::SocketAddr,
        data: &[u8],
    ) -> io::Result<std::net::SocketAddr> {
        let domain_opt = Self::extract_domain_from_payload(data);

        if let Some(domain) = domain_opt {
            if let Some(gw) = self.shared.gateway() {
                if let Some(cfg) = gw.find_server(&domain).await {
                    // prefer udp_addresses if defined
                    if let Some(addrs) = cfg.udp_addresses.as_ref() {
                        if let Some(addr_str) = addrs.first() {
                            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                                return Ok(addr);
                            }
                        }
                    }
                    if let Some(addr_str) = cfg.addresses.first() {
                        if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                            return Ok(addr);
                        }
                    }
                }
            }
        }

        // fallback: try every configuration for a UDP address first
        let all = self
            .shared
            .configuration_service()
            .get_all_configurations()
            .await;

        for cfg in all.values() {
            if let Some(addrs) = cfg.udp_addresses.as_ref() {
                if let Some(addr_str) = addrs.first() {
                    if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                        return Ok(addr);
                    }
                }
            }
        }

        // if none of the configs exposed an UDP address, fall back to any
        // generic address just in case
        for cfg in all.values() {
            if let Some(addr_str) = cfg.addresses.first() {
                if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                    return Ok(addr);
                }
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no backend available",
        ))
    }

    /// Very simple extractor that returns the first token containing a dot.
    fn extract_domain_from_payload(data: &[u8]) -> Option<String> {
        if let Ok(s) = std::str::from_utf8(data) {
            if let Some(token) = s.split_whitespace().next() {
                if token.contains('.') {
                    return Some(token.to_string());
                }
            }
        }
        None
    }

    async fn handle_modern_connection(
        &self,
        mut client: Connection,
        handshake_timeout_secs: u64,
    ) -> io::Result<()> {
        debug!(
            log_type = LogType::PacketProcessing.as_str(),
            "Reading handshake packet (with {}s timeout)", handshake_timeout_secs
        );
        let handshake_packet = match tokio::time::timeout(
            tokio::time::Duration::from_secs(handshake_timeout_secs),
            client.read_packet(),
        )
        .await
        {
            Ok(Ok(packet)) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Successfully read handshake packet"
                );
                packet
            }
            Ok(Err(e)) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Failed to read handshake packet: {}", e
                );
                if let Err(close_err) = client.close().await {
                    warn!(
                        log_type = LogType::TcpConnection.as_str(),
                        "Error closing client connection: {}", close_err
                    );
                }
                return Err(e.into());
            }
            Err(_) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Timeout reading handshake packet after {}s", handshake_timeout_secs
                );
                if let Err(close_err) = client.close().await {
                    warn!(
                        log_type = LogType::TcpConnection.as_str(),
                        "Error closing client connection: {}", close_err
                    );
                }
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Handshake packet timeout",
                ));
            }
        };

        debug!(
            log_type = LogType::PacketProcessing.as_str(),
            "Parsing handshake packet"
        );
        let handshake = match ServerBoundHandshake::from_packet(&handshake_packet) {
            Ok(handshake) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Successfully parsed handshake: {:?}", handshake
                );
                handshake
            }
            Err(e) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Failed to parse handshake packet: {}", e
                );
                if let Err(close_err) = client.close().await {
                    warn!(
                        log_type = LogType::TcpConnection.as_str(),
                        "Error closing client connection: {}", close_err
                    );
                }
                return Err(io::Error::new(io::ErrorKind::InvalidData, e));
            }
        };

        let domain: Arc<str> = handshake.parse_server_address().into();
        debug!(domain = %domain, log_type = LogType::PacketProcessing.as_str(), "Processing connection for domain");

        debug!(
            log_type = LogType::PacketProcessing.as_str(),
            "Reading second packet (with {}s timeout)", handshake_timeout_secs
        );
        let second_packet = match tokio::time::timeout(
            tokio::time::Duration::from_secs(handshake_timeout_secs),
            client.read_packet(),
        )
        .await
        {
            Ok(Ok(packet)) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Successfully read second packet"
                );
                packet
            }
            Ok(Err(e)) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Failed to read second packet: {}", e
                );
                if let Err(close_err) = client.close().await {
                    warn!(
                        log_type = LogType::TcpConnection.as_str(),
                        "Error closing client connection: {}", close_err
                    );
                }
                return Err(e.into());
            }
            Err(_) => {
                debug!(
                    log_type = LogType::PacketProcessing.as_str(),
                    "Timeout reading second packet after {}s", handshake_timeout_secs
                );
                if let Err(close_err) = client.close().await {
                    warn!(
                        log_type = LogType::TcpConnection.as_str(),
                        "Error closing client connection: {}", close_err
                    );
                }
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Second packet timeout",
                ));
            }
        };

        let protocol_version = Version::from(handshake.protocol_version.0);
        let is_login = handshake.is_login_request();

        debug!(
            domain = %domain,
            protocol_version = ?protocol_version,
            is_login = is_login,
            log_type = LogType::ServerManager.as_str(),
            "Preparing server request"
        );

        let client_addr = client.peer_addr().await?;
        let session_id = client.session_id;
        let original_client_addr = client.original_client_addr;

        let _handle_client_span = debug_span!(
            "handle_client_flow",
            domain = %domain,
            is_login = %is_login,
            log_type = LogType::ServerManager.as_str()
        );
        let domain_clone = domain.clone();

        let gateway_ref = self.gateway.clone();
        tokio::spawn(async move {
            // let _guard = handle_client_span.entered();
            debug!(
                log_type = LogType::ServerManager.as_str(),
                "Processing client in separate task"
            );

            if let Some(original_addr) = &original_client_addr {
                debug!(
                    log_type = LogType::ProxyProtocol.as_str(),
                    "Using original client address from proxy protocol: {}", original_addr
                );
            }

            gateway_ref
                .handle_client_connection(
                    client,
                    ServerRequest {
                        client_addr,
                        original_client_addr,
                        domain: domain.clone(),
                        is_login,
                        protocol_version,
                        read_packets: Arc::new([handshake_packet, second_packet]),
                        session_id,
                    },
                )
                .await;

            debug!(domain = %domain, log_type = LogType::ServerManager.as_str(), "Client processing task completed");
        });

        debug!(domain = %domain_clone, log_type = LogType::TcpConnection.as_str(), "Connection handler completed");
        Ok(())
    }

    pub async fn shutdown(self: &Arc<Self>) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let self_guard = self.clone();
        tokio::spawn(async move {
            let config_service = self_guard.shared.configuration_service_arc();
            let configs = config_service.get_all_configurations().await;

            for (config_id, _) in configs {
                self_guard
                    .shared
                    .actor_supervisor()
                    .shutdown_actors(&config_id)
                    .await;
            }

            let _ = tx.send(());
        });

        rx
    }

    pub fn get_shared(&self) -> Arc<crate::core::shared_component::SharedComponent> {
        self.shared.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ShutdownController;
    use infrarust_config::models::server::ProxyModeEnum;
    use infrarust_config::{InfrarustConfig, ServerConfig};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::UdpSocket;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_udp_receives_packet_and_exits() {
        let config = InfrarustConfig::default();
        let shutdown = ShutdownController::new();
        let server = Arc::new(Infrarust::new(config, shutdown.clone()).unwrap());

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let srv = Arc::clone(&server);
        let handle = tokio::spawn(async move {
            // move the socket into the UDP task; we don't need it afterward
            srv.run_udp(socket).await;
        });

        // send a packet with proxy header
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let header = b"PROXY UDP4 1.2.3.4 5.6.7.8 12345 19132\r\nhello";
        client.send_to(header, addr).await.unwrap();

        // let the server process it
        tokio::time::sleep(Duration::from_millis(100)).await;

        // trigger shutdown to end run_udp
        shutdown.trigger_shutdown("test").await;
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_udp_forwarding() {
        let config = InfrarustConfig::default();
        let shutdown = ShutdownController::new();
        let server = Arc::new(Infrarust::new(config, shutdown.clone()).unwrap());

        // prepare a backend socket and register it in the configuration service
        let backend_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_socket.local_addr().unwrap();
        // our config explicitly supplies udp_addresses to show the new path
        let server_cfg = ServerConfig {
            domains: vec!["example.com".to_string()],
            addresses: vec!["1.2.3.4:25565".to_string()], // dummy tcp entry
            udp_addresses: Some(vec![backend_addr.to_string()]),
            send_proxy_protocol: Some(false),
            proxy_mode: Some(ProxyModeEnum::Passthrough),
            ..Default::default()
        };
        server
            .get_shared()
            .configuration_service()
            .update_configurations(vec![server_cfg])
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        let srv = Arc::clone(&server);
        let handle = tokio::spawn(async move {
            srv.run_udp(socket).await;
        });

        // send a payload that begins with our domain token
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let payload = b"example.com handshake data";
        client.send_to(payload, server_addr).await.unwrap();

        // backend should receive it
        let mut buf = [0u8; 1024];
        let (size, _) =
            tokio::time::timeout(Duration::from_secs(1), backend_socket.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buf[..size], payload);

        // test reply path: backend -> server -> client
        backend_socket
            .send_to(b"response", server_addr)
            .await
            .unwrap();
        let mut buf2 = [0u8; 1024];
        let (size2, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf2[..size2], b"response");

        shutdown.trigger_shutdown("test").await;
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_udp_fallback_to_addresses() {
        let config = InfrarustConfig::default();
        let shutdown = ShutdownController::new();
        let server = Arc::new(Infrarust::new(config, shutdown.clone()).unwrap());

        let backend_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_socket.local_addr().unwrap();
        let server_cfg = ServerConfig {
            domains: vec!["fallback.example.com".to_string()],
            addresses: vec![backend_addr.to_string()],
            send_proxy_protocol: Some(false),
            proxy_mode: Some(ProxyModeEnum::Passthrough),
            ..Default::default()
        };
        server
            .get_shared()
            .configuration_service()
            .update_configurations(vec![server_cfg])
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        let srv = Arc::clone(&server);
        let handle = tokio::spawn(async move {
            srv.run_udp(socket).await;
        });

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let payload = b"fallback.example.com hello";
        client.send_to(payload, server_addr).await.unwrap();

        let mut buf = [0u8; 1024];
        let (size, _) =
            tokio::time::timeout(Duration::from_secs(1), backend_socket.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buf[..size], payload);

        shutdown.trigger_shutdown("test").await;
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_udp_no_domain_uses_udp_addresses() {
        let config = InfrarustConfig::default();
        let shutdown = ShutdownController::new();
        let server = Arc::new(Infrarust::new(config, shutdown.clone()).unwrap());

        let backend_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_socket.local_addr().unwrap();
        let server_cfg = ServerConfig {
            domains: vec!["example.com".to_string()],
            udp_addresses: Some(vec![backend_addr.to_string()]),
            addresses: vec!["1.2.3.4:25565".to_string()],
            send_proxy_protocol: Some(false),
            proxy_mode: Some(ProxyModeEnum::Passthrough),
            ..Default::default()
        };
        server
            .get_shared()
            .configuration_service()
            .update_configurations(vec![server_cfg])
            .await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        let srv = Arc::clone(&server);
        let handle = tokio::spawn(async move {
            srv.run_udp(socket).await;
        });

        // payload contains no dot/domain, forcing fallback logic
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"nope", server_addr).await.unwrap();

        let mut buf = [0u8; 1024];
        let (size, _) =
            tokio::time::timeout(Duration::from_secs(1), backend_socket.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buf[..size], b"nope");

        shutdown.trigger_shutdown("test").await;
        let _ = handle.await;
    }

    #[test]
    fn test_extract_domain_from_payload() {
        assert_eq!(
            Infrarust::extract_domain_from_payload(b"example.com hello world"),
            Some("example.com".to_string())
        );
        assert_eq!(
            Infrarust::extract_domain_from_payload(b"192.168.0.1:19132"),
            None
        );
        assert_eq!(Infrarust::extract_domain_from_payload(b"justastring"), None);
    }
}
