use opentelemetry::InstrumentationScope;
use std::{
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use super::{error::DynError, license::License, Graph, Result};
use crate::{engine::dataflow::monitoring::ProberStats, env::parse_env_var};
use arc_swap::ArcSwapOption;
use itertools::Itertools;
use log::{debug, info};
#[cfg(unix)]
use nix::sys::{
    resource::{getrusage, UsageWho},
    time::TimeValLike,
};

#[cfg(windows)]
use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};
#[cfg(windows)]
use windows_sys::Win32::Foundation::FILETIME;
#[cfg(windows)]
use std::mem;
use opentelemetry::{
    global,
    metrics::{Meter, MeterProvider},
    KeyValue,
};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
    Resource,
};
use opentelemetry_semantic_conventions::resource::{
    SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_NAMESPACE, SERVICE_VERSION,
};
use sysinfo::{get_current_pid, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::mpsc;
use tonic::transport::ClientTlsConfig;
use uuid::Uuid;

const PATHWAY_TELEMETRY_SERVER: &str = "https://usage.pathway.com";
const PERIODIC_READER_INTERVAL: Duration = Duration::from_secs(60);
const OPENTELEMETRY_EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

const PROCESS_MEMORY_USAGE: &str = "process.memory.usage";
const PROCESS_CPU_USER_TIME: &str = "process.cpu.utime";
const PROCESS_CPU_SYSTEM_TIME: &str = "process.cpu.stime";
const INPUT_LATENCY: &str = "latency.input";
const OUTPUT_LATENCY: &str = "latency.output";

const ROOT_TRACE_ID: &str = "root.trace.id";
const RUN_ID: &str = "run.id";
const LICENSE_KEY: &str = "license.key";

const LOCAL_DEV_NAMESPACE: &str = "local-dev";

#[cfg(windows)]
fn filetime_to_seconds(ft: &FILETIME) -> i64 {
    // Convert FILETIME (100-nanosecond intervals) to seconds
    let time_100ns = ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64);
    (time_100ns / 10_000_000) as i64
}

#[cfg(windows)]
fn get_process_cpu_times() -> Result<(i64, i64), &'static str> {
    unsafe {
        let mut creation_time = mem::zeroed::<FILETIME>();
        let mut exit_time = mem::zeroed::<FILETIME>();
        let mut kernel_time = mem::zeroed::<FILETIME>();
        let mut user_time = mem::zeroed::<FILETIME>();
        
        let result = GetProcessTimes(
            GetCurrentProcess(),
            &mut creation_time,
            &mut exit_time,
            &mut kernel_time,
            &mut user_time,
        );
        
        if result == 0 {
            return Err("Failed to get process times");
        }
        
        let user_seconds = filetime_to_seconds(&user_time);
        let kernel_seconds = filetime_to_seconds(&kernel_time);
        
        Ok((user_seconds, kernel_seconds))
    }
}

struct Telemetry {
    pub config: Box<TelemetryEnabled>,
}

impl Telemetry {
    fn new(config: Box<TelemetryEnabled>) -> Self {
        Telemetry { config }
    }

    fn resource(&self) -> Resource {
        let root_trace_id = root_trace_id(self.config.trace_parent.as_deref()).unwrap_or_default();

        Resource::builder()
            .with_attributes([
                KeyValue::new(SERVICE_NAME, self.config.service_name.clone()),
                KeyValue::new(SERVICE_VERSION, self.config.service_version.clone()),
                KeyValue::new(SERVICE_INSTANCE_ID, self.config.service_instance_id.clone()),
                KeyValue::new(SERVICE_NAMESPACE, self.config.service_namespace.clone()),
                KeyValue::new(ROOT_TRACE_ID, root_trace_id.to_string()),
                KeyValue::new(RUN_ID, self.config.run_id.clone()),
                KeyValue::new(LICENSE_KEY, self.config.license_key.clone()),
            ])
            .build()
    }

    fn init_tracer_provider(&self) -> Option<SdkTracerProvider> {
        if self.config.tracing_servers.is_empty() {
            return None;
        }
        global::set_text_map_propagator(TraceContextPropagator::new());

        let mut provider_builder = SdkTracerProvider::builder().with_resource(self.resource());

        for endpoint in &self.config.tracing_servers {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(endpoint)
                .with_timeout(OPENTELEMETRY_EXPORT_TIMEOUT)
                .with_tls_config(ClientTlsConfig::new().with_enabled_roots())
                .build()
                .expect("exporter initialization should not fail");

            provider_builder = provider_builder.with_batch_exporter(exporter);
        }

        let tracer_provider = provider_builder.build();
        global::set_tracer_provider(tracer_provider.clone());
        Some(tracer_provider)
    }

    fn init_meter_provider(&self) -> Option<SdkMeterProvider> {
        if self.config.metrics_servers.is_empty() {
            return None;
        }

        let mut provider_builder = SdkMeterProvider::builder().with_resource(self.resource());

        for endpoint in &self.config.metrics_servers {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(endpoint)
                .with_timeout(OPENTELEMETRY_EXPORT_TIMEOUT)
                .with_tls_config(ClientTlsConfig::new().with_enabled_roots())
                .build()
                .expect("exporter initialization should not fail");

            let reader = PeriodicReader::builder(exporter)
                .with_interval(self.config.periodic_reader_interval)
                .build();

            provider_builder = provider_builder.with_reader(reader);
        }

        let meter_provider = provider_builder.build();
        global::set_meter_provider(meter_provider.clone());
        Some(meter_provider)
    }

    fn init(&self) -> TelemetryGuard {
        let noop_meter_provider = MeterProviderWrapper(global::meter_provider());
        let noop_tracer_provider = SdkTracerProvider::builder().build();

        let meter_provider = self.init_meter_provider();
        let tracer_provider = self.init_tracer_provider();

        TelemetryGuard {
            meter_provider,
            tracer_provider,
            noop_meter_provider,
            noop_tracer_provider,
        }
    }
}

#[derive(Clone)]
struct MeterProviderWrapper(Arc<dyn MeterProvider + Send + Sync>);

impl MeterProvider for MeterProviderWrapper {
    fn meter_with_scope(&self, scope: InstrumentationScope) -> Meter {
        self.0.meter_with_scope(scope)
    }
}

#[must_use]
#[allow(clippy::struct_field_names)]
struct TelemetryGuard {
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
    noop_meter_provider: MeterProviderWrapper,
    noop_tracer_provider: SdkTracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            provider.force_flush().unwrap_or(());
            provider.shutdown().unwrap_or(());
        }
        global::set_meter_provider(self.noop_meter_provider.clone());

        if let Some(provider) = self.tracer_provider.take() {
            provider.force_flush().unwrap_or(());
            provider.shutdown().unwrap_or(());
        }
        global::set_tracer_provider(self.noop_tracer_provider.clone());
    }
}

fn root_trace_id(trace_parent: Option<&str>) -> Option<&str> {
    if let Some(trace_parent) = trace_parent {
        Some(
            trace_parent
                .split('-')
                .nth(1)
                .expect("trace parent should contain the root trace ID"),
        )
    } else {
        None
    }
}

fn deduplicate(input: Vec<Option<String>>) -> Vec<String> {
    input.into_iter().flatten().sorted().dedup().collect()
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TelemetryEnabled {
    pub telemetry_server: Option<String>,
    pub monitoring_server: Option<String>,
    pub logging_servers: Vec<String>,
    pub tracing_servers: Vec<String>,
    pub metrics_servers: Vec<String>,
    pub service_name: String,
    pub service_version: String,
    pub service_namespace: String,
    pub service_instance_id: String,
    pub run_id: String,
    pub trace_parent: Option<String>,
    pub license_key: String,
    pub periodic_reader_interval: Duration,
}

#[derive(Clone, Debug)]
pub enum Config {
    Enabled(Box<TelemetryEnabled>),
    Disabled,
}

impl Config {
    pub fn create(
        license: &License,
        run_id: Option<String>,
        monitoring_server: Option<String>,
        trace_parent: Option<String>,
        periodic_reader_interval: Option<u64>,
    ) -> Result<Self> {
        let run_id = run_id.unwrap_or_else(|| Uuid::new_v4().to_string());

        if monitoring_server.is_some() {
            license
                .check_entitlements(["monitoring"])
                .map_err(DynError::from)?;
        }

        let telemetry_server = if license.telemetry_required() {
            Some(PATHWAY_TELEMETRY_SERVER.to_string())
        } else {
            None
        };

        if monitoring_server.is_none() && telemetry_server.is_none() {
            return Ok(Config::Disabled);
        }

        let periodic_reader_interval = if let Some(interval) = periodic_reader_interval {
            license
                .check_entitlements(["monitoring-internal"])
                .map_err(DynError::from)?;
            Duration::from_secs(interval)
        } else {
            PERIODIC_READER_INTERVAL
        };

        match license {
            License::NoLicenseKey => Ok(Config::Disabled),
            _ => Config::create_enabled(
                run_id,
                telemetry_server,
                monitoring_server,
                trace_parent,
                license,
                periodic_reader_interval,
            ),
        }
    }

    fn create_enabled(
        run_id: String,
        telemetry_server: Option<String>,
        monitoring_server: Option<String>,
        trace_parent: Option<String>,
        license: &License,
        periodic_reader_interval: Duration,
    ) -> Result<Self> {
        let service_instance_id: String = parse_env_var("PATHWAY_SERVICE_INSTANCE_ID")
            .map_err(DynError::from)?
            .unwrap_or(Uuid::new_v4().to_string());
        let service_namespace: String = parse_env_var("PATHWAY_SERVICE_NAMESPACE")
            .map_err(DynError::from)?
            .unwrap_or_else(|| {
                if service_instance_id.ends_with(LOCAL_DEV_NAMESPACE) {
                    LOCAL_DEV_NAMESPACE.to_string()
                } else {
                    format!("external-{}", Uuid::new_v4())
                }
            });
        Ok(Config::Enabled(Box::new(TelemetryEnabled {
            telemetry_server: telemetry_server.clone(),
            monitoring_server: monitoring_server.clone(),
            logging_servers: deduplicate(vec![monitoring_server.clone()]),
            tracing_servers: deduplicate(vec![telemetry_server.clone(), monitoring_server.clone()]),
            metrics_servers: deduplicate(vec![telemetry_server, monitoring_server]),
            service_name: env!("CARGO_PKG_NAME").to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            service_namespace,
            service_instance_id,
            run_id,
            trace_parent,
            license_key: license.shortcut(),
            periodic_reader_interval,
        })))
    }
}

pub struct Runner {
    close_sender: mpsc::Sender<()>,
    telemetry_thread_handle: Option<JoinHandle<()>>,
}

impl Runner {
    fn run(telemetry: Telemetry, stats: Arc<ArcSwapOption<ProberStats>>) -> Runner {
        let (tx, mut rx) = mpsc::channel::<mpsc::Sender<()>>(1);
        let telemetry_thread_handle = start_telemetry_thread(telemetry, tx, stats);
        let close_sender = rx.blocking_recv().expect("expecting return sender");
        Runner {
            close_sender,
            telemetry_thread_handle: Some(telemetry_thread_handle),
        }
    }
}

fn start_telemetry_thread(
    telemetry: Telemetry,
    start_sender: mpsc::Sender<mpsc::Sender<()>>,
    stats: Arc<ArcSwapOption<ProberStats>>,
) -> JoinHandle<()> {
    let handle: JoinHandle<()> = Builder::new()
        .name("pathway:telemetry_thread".to_string())
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async {
                    let (tx, mut rx) = mpsc::channel::<()>(1);
                    let _telemetry_guard = telemetry.init();
                    register_stats_metrics(&stats);
                    register_sys_metrics();
                    start_sender.send(tx).await.expect("should not fail");
                    rx.recv().await;
                });
        })
        .expect("telemetry thread creation failed");
    handle
}

fn register_stats_metrics(stats: &Arc<ArcSwapOption<ProberStats>>) {
    let meter = global::meter("pathway-stats");

    let input_stats = stats.clone();
    meter
        .u64_observable_gauge(INPUT_LATENCY)
        .with_unit("ms")
        .with_callback(move |observer| {
            let now = SystemTime::now();
            if let Some(ref stats) = *input_stats.load() {
                if let Some(latency) = stats.input_stats.latency(now) {
                    observer.observe(latency, &[]);
                }
            }
        })
        .build();

    let output_stats = stats.clone();
    meter
        .u64_observable_gauge(OUTPUT_LATENCY)
        .with_unit("ms")
        .with_callback(move |observer| {
            let now = SystemTime::now();
            if let Some(ref stats) = *output_stats.load() {
                if let Some(latency) = stats.output_stats.latency(now) {
                    observer.observe(latency, &[]);
                }
            }
        })
        .build();
}

fn cpu_refresh(pid: Pid, sys: &mut System) {
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_cpu(),
    );
    std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_cpu(),
    );
}

fn register_sys_metrics() {
    let meter = global::meter("pathway-sys");

    let pid = get_current_pid().expect("Failed to get current PID");

    meter
        .u64_observable_gauge(PROCESS_MEMORY_USAGE)
        .with_unit("byte")
        .with_callback(move |observer| {
            let mut sys: System = System::new();
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
            if let Some(process) = sys.process(pid) {
                observer.observe(process.memory(), &[]);
            }
        })
        .build();

    meter
        .i64_observable_gauge(PROCESS_CPU_USER_TIME)
        .with_unit("s")
        .with_callback(move |observer| {
            let mut sys: System = System::new();
            cpu_refresh(pid, &mut sys);
            
            #[cfg(unix)]
            {
                let usage = getrusage(UsageWho::RUSAGE_SELF).expect("Failed to call getrusage");
                observer.observe(usage.user_time().num_seconds(), &[]);
            }
            
            #[cfg(windows)]
            {
                match get_process_cpu_times() {
                    Ok((user_time, _)) => observer.observe(user_time, &[]),
                    Err(_) => observer.observe(0, &[]),
                }
            }
        })
        .build();

    meter
        .i64_observable_gauge(PROCESS_CPU_SYSTEM_TIME)
        .with_unit("s")
        .with_callback(move |observer| {
            let mut sys: System = System::new();
            cpu_refresh(pid, &mut sys);
            
            #[cfg(unix)]
            {
                let usage = getrusage(UsageWho::RUSAGE_SELF).expect("Failed to call getrusage");
                observer.observe(usage.system_time().num_seconds(), &[]);
            }
            
            #[cfg(windows)]
            {
                match get_process_cpu_times() {
                    Ok((_, system_time)) => observer.observe(system_time, &[]),
                    Err(_) => observer.observe(0, &[]),
                }
            }
        })
        .build();
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.close_sender.blocking_send(()).unwrap();
        self.telemetry_thread_handle
            .take()
            .unwrap()
            .join()
            .expect("telemetry thread drop failed");
    }
}

pub fn maybe_run_telemetry_thread(graph: &dyn Graph, config: Config) -> Option<Runner> {
    match config {
        Config::Enabled(config) => {
            if config.telemetry_server.is_some() {
                info!("Telemetry enabled");
            }
            if let Some(monitoring_server) = config.monitoring_server.clone() {
                info!("Monitoring server: {monitoring_server}");
            }

            let telemetry = Telemetry::new(config.clone());
            let stats_shared = Arc::new(ArcSwapOption::from(None));
            let runner = Runner::run(telemetry, stats_shared.clone());

            graph
                .attach_prober(
                    Box::new(move |prober_stats| stats_shared.store(Some(Arc::new(prober_stats)))),
                    false,
                    false,
                )
                .expect("failed to start telemetry thread");

            Some(runner)
        }
        Config::Disabled => {
            debug!("Telemetry disabled");
            None
        }
    }
}
