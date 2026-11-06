//! Test-only OTLP receiver using the real public pipeline, isolated because its
//! process-wide SDK cannot be reset between unit tests. No application is started.
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub(crate) fn isolated(name: &str, test: impl FnOnce(&Capture)) {
    const CHILD: &str = "PIONEER_SCENARIO_UNIT_TEST_CHILD";
    const COMPLETED: &str = "PIONEER_SCENARIO_UNIT_TEST_COMPLETED";
    if std::env::var(CHILD).ok().as_deref() != Some(name) {
        let completed = tempfile::NamedTempFile::new().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--test-threads=1", "--nocapture"])
            .env(CHILD, name)
            .env(COMPLETED, completed.path())
            .status()
            .unwrap();
        assert!(status.success(), "isolated observation regression failed");
        assert_eq!(
            std::fs::read(completed.path()).unwrap(),
            b"completed",
            "exact child test must actually execute its assertions"
        );
        return;
    }
    let capture = Capture::new();
    test(&capture);
    drop(capture);
    std::fs::write(std::env::var_os(COMPLETED).unwrap(), b"completed").unwrap();
}

pub(crate) struct Capture {
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    stop: Arc<AtomicBool>,
    address: SocketAddr,
    server: Option<std::thread::JoinHandle<()>>,
}
pub(crate) struct Span {
    pub trace: String,
    pub id: String,
    pub parent: String,
    pub links: Vec<(String, String)>,
    pub attributes: HashMap<String, String>,
}
impl Capture {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let captured = bodies.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let server = std::thread::spawn(move || {
            for connection in listener.incoming() {
                let mut stream = connection.unwrap();
                if stopped.load(Ordering::Acquire) {
                    break;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let traces = line.starts_with("POST /traces ");
                let mut size = None;
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            size = Some(value.trim().parse::<usize>().unwrap());
                        }
                    }
                }
                let size = size.expect("SDK sends a bounded protobuf body");
                assert!(size <= 8 * 1024 * 1024);
                let mut body = vec![0; size];
                reader.read_exact(&mut body).unwrap();
                drop(reader);
                if traces {
                    let mut bodies = captured.lock().unwrap();
                    assert!(bodies.len() < 64);
                    bodies.push(body);
                }
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            }
        });
        pioneer_observability::set_telemetry_enabled(true);
        pioneer_observability::init_otlp_observability_for(
            pioneer_observability::TelemetryTarget::Mobile,
            pioneer_observability::OtlpTelemetryConfig {
                metrics_endpoint: format!("http://{address}/metrics"),
                traces_endpoint: format!("http://{address}/traces"),
                export_interval: Duration::from_secs(900),
                export_timeout: Duration::from_secs(5),
                deployment_environment: None,
                service_version: None,
            },
        )
        .unwrap();
        Self {
            bodies,
            stop,
            address,
            server: Some(server),
        }
    }
    pub(crate) fn spans(&self) -> Vec<Span> {
        pioneer_observability::force_flush_observability().unwrap();
        let bodies = std::mem::take(&mut *self.bodies.lock().unwrap());
        let mut spans = Vec::new();
        // ExportTraceServiceRequest.resource_spans -> scope_spans -> spans.
        for body in &bodies {
            for resource in fields(body, 1) {
                for scope in fields(resource, 2) {
                    for span in fields(scope, 2) {
                        if text(span, 5) != "pioneer.scenario.boundary" {
                            continue;
                        }
                        let attributes = fields(span, 9)
                            .into_iter()
                            .map(|attribute| {
                                let key = text(attribute, 1);
                                let value = fields(attribute, 2).into_iter().next().unwrap();
                                (key, text(value, 1))
                            })
                            .collect();
                        spans.push(Span {
                            trace: hex::encode(fields(span, 1)[0]),
                            id: hex::encode(fields(span, 2)[0]),
                            parent: fields(span, 4)
                                .first()
                                .map(|parent| hex::encode(parent))
                                .unwrap_or_default(),
                            links: fields(span, 13)
                                .iter()
                                .map(|link| {
                                    (
                                        hex::encode(fields(link, 1)[0]),
                                        hex::encode(fields(link, 2)[0]),
                                    )
                                })
                                .collect(),
                            attributes,
                        });
                    }
                }
            }
        }
        spans
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        let _ = pioneer_observability::shutdown_observability(Duration::from_secs(5));
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address);
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}
fn varint(bytes: &mut &[u8]) -> u64 {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = bytes[0];
        *bytes = &bytes[1..];
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return value;
        }
    }
    panic!("invalid test OTLP varint");
}
fn fields(mut bytes: &[u8], field: u64) -> Vec<&[u8]> {
    let mut values = Vec::new();
    while !bytes.is_empty() {
        let tag = varint(&mut bytes);
        match tag & 7 {
            0 => {
                varint(&mut bytes);
            }
            1 => bytes = &bytes[8..],
            2 => {
                let size = usize::try_from(varint(&mut bytes)).unwrap();
                let (value, rest) = bytes.split_at(size);
                bytes = rest;
                if tag >> 3 == field {
                    values.push(value);
                }
            }
            5 => bytes = &bytes[4..],
            _ => panic!("unsupported test OTLP wire type"),
        }
    }
    values
}
fn text(bytes: &[u8], field: u64) -> String {
    fields(bytes, field)
        .first()
        .map(|value| String::from_utf8(value.to_vec()).unwrap())
        .unwrap_or_default()
}
