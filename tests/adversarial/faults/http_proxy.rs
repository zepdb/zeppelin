use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{FaultKind, HttpFaultAction};

#[derive(Debug)]
pub struct HttpFaultInjector {
    addr: SocketAddr,
    armed: Arc<Mutex<Option<HttpFaultAction>>>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

#[derive(Clone, Debug)]
pub struct HttpFaultRequestHandle {
    addr: SocketAddr,
    armed: Arc<Mutex<Option<HttpFaultAction>>>,
}

impl HttpFaultInjector {
    pub async fn start(upstream: SocketAddr) -> io::Result<Self> {
        let listener =
            TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
        let addr = listener.local_addr()?;
        let armed = Arc::new(Mutex::new(None));
        let armed_for_task = Arc::clone(&armed);
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut connections = Vec::new();
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let (downstream, _) = accepted
                            .unwrap_or_else(|error| panic!("HTTP fault proxy accept failed: {error}"));
                        let action = armed_for_task
                            .lock()
                            .expect("HTTP fault proxy armed mutex poisoned")
                            .take();
                        connections.push(tokio::spawn(async move {
                            handle_connection(downstream, upstream, action).await
                        }));
                    }
                }
            }
            for connection in connections {
                connection
                    .await
                    .unwrap_or_else(|error| panic!("HTTP fault proxy task failed: {error}"))
                    .unwrap_or_else(|error| panic!("HTTP fault proxy connection failed: {error}"));
            }
        });
        Ok(Self {
            addr,
            armed,
            shutdown,
            task,
        })
    }

    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    #[must_use]
    pub fn request_handle(&self) -> HttpFaultRequestHandle {
        HttpFaultRequestHandle {
            addr: self.addr,
            armed: Arc::clone(&self.armed),
        }
    }

    pub fn arm(&self, action: HttpFaultAction) {
        self.request_handle().arm(action);
    }

    pub fn disarm(&self) {
        self.request_handle().disarm();
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        self.task
            .await
            .unwrap_or_else(|error| panic!("HTTP fault proxy task failed: {error}"));
    }
}

impl HttpFaultRequestHandle {
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn arm(&self, action: HttpFaultAction) {
        assert!(
            matches!(
                action.kind,
                FaultKind::DropResponse
                    | FaultKind::TruncateResponse { .. }
                    | FaultKind::ResetAfterRequest
            ),
            "only proxy-side HTTP actions may be armed: {:?}",
            action.kind
        );
        let mut armed = self
            .armed
            .lock()
            .expect("HTTP fault proxy armed mutex poisoned");
        assert!(
            armed.is_none(),
            "HTTP fault proxy already has an armed action"
        );
        *armed = Some(action);
    }

    pub fn disarm(&self) {
        self.armed
            .lock()
            .expect("HTTP fault proxy armed mutex poisoned")
            .take();
    }
}

async fn handle_connection(
    mut downstream: TcpStream,
    upstream_addr: SocketAddr,
    action: Option<HttpFaultAction>,
) -> io::Result<()> {
    let request = read_http_request(&mut downstream).await?;
    let mut upstream = TcpStream::connect(upstream_addr).await?;
    upstream.write_all(&request).await?;

    match action.map(|action| action.kind) {
        None => relay_response(&mut upstream, &mut downstream, None).await,
        Some(FaultKind::DropResponse) => {
            let mut discarded = Vec::new();
            upstream.read_to_end(&mut discarded).await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(())
        }
        Some(FaultKind::TruncateResponse { at_bytes }) => {
            relay_response(&mut upstream, &mut downstream, Some(at_bytes)).await
        }
        Some(FaultKind::ResetAfterRequest) => {
            #[allow(deprecated)]
            downstream.set_linger(Some(Duration::ZERO))?;
            Ok(())
        }
        Some(other) => panic!("client-side HTTP action reached proxy connection: {other:?}"),
    }
}

async fn relay_response(
    upstream: &mut TcpStream,
    downstream: &mut TcpStream,
    limit: Option<usize>,
) -> io::Result<()> {
    let mut remaining = limit.unwrap_or(usize::MAX);
    let mut buffer = [0u8; 8192];
    loop {
        let read = upstream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        let write = read.min(remaining);
        if write > 0 {
            downstream.write_all(&buffer[..write]).await?;
            remaining -= write;
        }
        if remaining == 0 {
            downstream.shutdown().await?;
            return Ok(());
        }
    }
}

async fn read_http_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
    let mut request = Vec::new();
    let mut buffer = [0u8; 8192];
    let header_end = loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before completing HTTP request headers",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP fault proxy request exceeded size limit",
            ));
        }
        if let Some(index) = find_header_end(&request) {
            break index;
        }
    };
    let content_length = parse_content_length(&request[..header_end])?;
    let total = header_end + 4 + content_length;
    while request.len() < total {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before completing HTTP request body",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP fault proxy request exceeded size limit",
            ));
        }
    }
    request.truncate(total);
    Ok(request)
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &[u8]) -> io::Result<usize> {
    let headers = std::str::from_utf8(headers)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    for line in headers.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<usize>()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use axum::{routing::post, Json, Router};
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_request_content_length() {
        assert_eq!(
            parse_content_length(b"POST / HTTP/1.1\r\nContent-Length: 12").unwrap(),
            12
        );
        assert_eq!(
            parse_content_length(b"GET / HTTP/1.1\r\nHost: x").unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn unarmed_proxy_relays_an_axum_response() {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                upstream,
                Router::new().route("/pass", post(|| async { Json(json!({ "relayed": true })) })),
            )
            .await
            .unwrap();
        });
        let proxy = HttpFaultInjector::start(upstream_addr).await.unwrap();

        let response = reqwest::Client::new()
            .post(format!("{}/pass", proxy.base_url()))
            .header(reqwest::header::CONNECTION, "close")
            .json(&json!({ "request": true }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            json!({ "relayed": true })
        );

        proxy.shutdown().await;
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_accepted_connections() {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                upstream,
                Router::new().route("/drop", post(|| async { Json(json!({ "ok": true })) })),
            )
            .await
            .unwrap();
        });
        let proxy = HttpFaultInjector::start(upstream_addr).await.unwrap();
        proxy.arm(HttpFaultAction {
            event_id: "network-shutdown".to_string(),
            op_index: 0,
            kind: FaultKind::DropResponse,
            window: false,
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let url = format!("{}/drop", proxy.base_url());
        let request = tokio::spawn(async move {
            client
                .post(url)
                .header(reqwest::header::CONNECTION, "close")
                .send()
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while proxy
                .armed
                .lock()
                .expect("HTTP fault proxy armed mutex poisoned")
                .is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("proxy never accepted the armed connection");

        proxy.shutdown().await;

        let response = tokio::time::timeout(Duration::from_millis(100), request)
            .await
            .expect("proxy shutdown returned with a live connection task")
            .unwrap();
        assert!(response.is_err());
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_connections_while_request_handle_is_live() {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_received = Arc::new(tokio::sync::Notify::new());
        let handler_received = Arc::clone(&upstream_received);
        let server = tokio::spawn(async move {
            axum::serve(
                upstream,
                Router::new().route(
                    "/drop",
                    post(move || {
                        let handler_received = Arc::clone(&handler_received);
                        async move {
                            handler_received.notify_one();
                            Json(json!({ "ok": true }))
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let proxy = Arc::new(HttpFaultInjector::start(upstream_addr).await.unwrap());
        let request_handle = proxy.request_handle();
        request_handle.arm(HttpFaultAction {
            event_id: "network-shutdown-handle".to_string(),
            op_index: 0,
            kind: FaultKind::DropResponse,
            window: false,
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let url = format!("{}/drop", request_handle.base_url());
        let request = tokio::spawn(async move {
            client
                .post(url)
                .header(reqwest::header::CONNECTION, "close")
                .send()
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), upstream_received.notified())
            .await
            .expect("proxy never forwarded the accepted request owned by the live handle");

        let proxy = Arc::try_unwrap(proxy)
            .expect("a live request handle must not retain the injector lifecycle owner");
        tokio::time::timeout(Duration::from_secs(1), proxy.shutdown())
            .await
            .expect("proxy shutdown did not drain its accepted request boundedly");

        assert!(request_handle.base_url().starts_with("http://"));
        let response = tokio::time::timeout(Duration::from_millis(100), request)
            .await
            .expect("proxy shutdown returned with a live accepted request")
            .unwrap();
        assert!(response.is_err());
        server.abort();
        let _ = server.await;
    }
}
