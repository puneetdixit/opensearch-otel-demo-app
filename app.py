import time
import os
import psutil
import threading
from multiprocessing import Process, Event as MPEvent
from typing import Dict
import uvicorn
import socket
from fastapi import FastAPI, Response, status, Query

# --- OpenTelemetry Imports ---
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.metrics import CallbackOptions, Observation

# --- Configuration ---
SERVICE_NAME = "python-demo-app"
OTLP_ENDPOINT = "http://10.251.150.7:4318"
REPORTING_INTERVAL_SECONDS = 1

TMP_MOUNTPOINT = "/tmp"

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = os.environ.get("HOST_IP", "10.251.150.8")
    finally:
        s.close()
    return ip

HOST_IP = get_ip_address()

# --- Global CPU Load Control ---
thread_cpu_spike_active = threading.Event()
cpu_spike_thread = None
thread_lock = threading.Lock()

process_cpu_spike_active = MPEvent()
cpu_spike_processes = []
process_lock = threading.Lock()

# --- FastAPI Setup ---
app = FastAPI(title="Python Demo APP")

print("IP Address:", HOST_IP)

# --- CPU Load Functions ---
def cpu_intensive_thread_task(duration=None):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] CPU Spike thread started...")
    start_time = time.time()
    while thread_cpu_spike_active.is_set():
        _ = [i**0.5 for i in range(10000)] * 10
        if duration and (time.time() - start_time) >= duration:
            thread_cpu_spike_active.clear()
            break
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] CPU Spike thread stopped.")


def cpu_intensive_process_task(process_id: int, duration=None):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] CPU Spike process {process_id} started...")
    start_time = time.time()
    while process_cpu_spike_active.is_set():
        _ = [i**0.5 for i in range(10000)] * 10
        if duration and (time.time() - start_time) >= duration:
            process_cpu_spike_active.clear()
            break
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] CPU Spike process {process_id} stopped.")

# --- OpenTelemetry Metrics Setup ---
def collect_cpu_metric(options: CallbackOptions):
    """
    Callback function for the Asynchronous Gauge.
    It is called periodically by the OTel SDK to get the current measurement.
    """
    try:
        cpu_pct = psutil.cpu_percent(interval=None)
        yield Observation(
            value=cpu_pct,
            attributes={"reporter_id": "fastapi-app-monitor-01"}
        )
    except Exception as e:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error collecting CPU metric: {e}")


def collect_tmp_disk_utilization_metric(options: CallbackOptions):
    """
    Collect ONLY /tmp disk utilization metric
    """

    try:

        usage = psutil.disk_usage(TMP_MOUNTPOINT)

        yield Observation(
            value=usage.percent,
            attributes={
                "host.ip": HOST_IP,
                "mountpoint": TMP_MOUNTPOINT,
                "metric.type": "disk.utilization"
            }
        )

    except Exception as e:

        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error collecting TMP disk metric: {e}")



def initialize_otel_metrics() -> None:

    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "reporter.instance.id": "fastapi-app-monitor-01",
        "host.ip": HOST_IP
    })

    otlp_exporter = OTLPMetricExporter(
        endpoint=f"{OTLP_ENDPOINT}/v1/metrics",
        timeout=10,
    )

    metric_reader = PeriodicExportingMetricReader(
        exporter=otlp_exporter,
        export_interval_millis=int(REPORTING_INTERVAL_SECONDS * 1000),
        export_timeout_millis=3000
    )

    provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader]
    )

    metrics.set_meter_provider(provider)

    meter = metrics.get_meter(__name__)

    meter.create_observable_gauge(
        name="system.cpu.utilization",
        callbacks=[collect_cpu_metric],
        description="Current system CPU utilization percentage",
        unit="%"
    )


    meter.create_observable_gauge(
        name="system.disk.utilization",
        callbacks=[collect_tmp_disk_utilization_metric],
        description="TMP disk utilization percentage",
        unit="%"
    )

    print("\n[INFO] OpenTelemetry Metrics initialized.")
    print(f"[INFO] Exporting to OTLP endpoint: {OTLP_ENDPOINT}/v1/metrics")
    print(f"[INFO] Export interval: {REPORTING_INTERVAL_SECONDS} seconds.")
    print(f"[INFO] Reporting Host IP: {HOST_IP}")
    print(f"[INFO] Monitoring mountpoint: {TMP_MOUNTPOINT}")

    # Pre-prime psutil.cpu_percent() call
    psutil.cpu_percent(interval=None)

# --- FastAPI Routes ---
@app.get('/', tags=["status"])
def index() -> Dict[str, str]:

    current_cpu = psutil.cpu_percent(interval=0.1)

    tmp_disk = psutil.disk_usage(TMP_MOUNTPOINT)

    return {
        "status": "FastAPI CPU Reporter is Running (OpenTelemetry Active)",
        "current_cpu_utilization": f"{current_cpu:.2f}%",
        "tmp_disk_usage_percent": f"{tmp_disk.percent:.2f}%",
        "tmp_disk_free_gb": f"{tmp_disk.free / (1024**3):.2f} GB",
        "thread_cpu_spike_status": "Active" if thread_cpu_spike_active.is_set() else "Inactive",
        "process_cpu_spike_status": "Active" if process_cpu_spike_active.is_set() else "Inactive",
        "active_spike_processes": f"{len([p for p in cpu_spike_processes if p.is_alive()])}",
        "otel_metrics_status": f"Active (Exporting every {REPORTING_INTERVAL_SECONDS}s)",
        "exporting_to": f"{OTLP_ENDPOINT}/v1/metrics",
        "host_ip": HOST_IP
    }

# --- Thread Endpoints ---
@app.post('/thread-up', tags=["control"])
def thread_spike_up(response: Response, duration: int = Query(15, description="Duration in seconds")) -> Dict:
    global cpu_spike_thread
    with thread_lock:
        if not thread_cpu_spike_active.is_set():
            thread_cpu_spike_active.set()
            cpu_spike_thread = threading.Thread(target=cpu_intensive_thread_task, args=(duration,))
            cpu_spike_thread.daemon = True
            cpu_spike_thread.start()
            response.status_code = status.HTTP_202_ACCEPTED
            return {"message": f"CPU Spike (thread) initiated for {duration} seconds."}
        else:
            return {"message": "CPU Spike (thread) is already active."}

@app.post('/thread-dn', tags=["control"])
def thread_spike_dn(response: Response) -> Dict:
    """
    Stops the thread-based CPU-intensive task.
    """
    global cpu_spike_thread
    with thread_lock:
        if thread_cpu_spike_active.is_set():
            thread_cpu_spike_active.clear()
            response.status_code = status.HTTP_202_ACCEPTED
            return {
                "message": "CPU Spike (thread) signaled to stop. Usage should return to normal soon.",
                "thread_cpu_spike_status": "Inactive"
            }
        else:
            return {
                "message": "CPU Spike (thread) was already inactive.",
                "thread_cpu_spike_status": "Inactive"
            }


@app.post('/process-up', tags=["control"])
def process_spike_up(response: Response, 
                     duration: int = Query(15, description="Duration in seconds"), 
                     num_of_process: int = Query(2, description="Number of process to start")) -> Dict:
    global cpu_spike_processes
    with process_lock:
        active_processes = [p for p in cpu_spike_processes if p.is_alive()]

        if not process_cpu_spike_active.is_set() and len(active_processes) == 0:
            process_cpu_spike_active.set()

            cpu_spike_processes.clear()

            for i in range(1, num_of_process + 1):
                process = Process(target=cpu_intensive_process_task, args=(i, duration))
                process.daemon = True
                process.start()

                cpu_spike_processes.append(process)

            response.status_code = status.HTTP_202_ACCEPTED
            return {"message": f"{num_of_process} CPU Spike processes initiated for {duration} seconds."}
        else:
            return {
                "message": f"CPU Spike (process) is already active with {len(active_processes)} processes running.",
                "process_cpu_spike_status": "Active"
            }


@app.post('/process-dn', tags=["control"])
def process_spike_dn(response: Response) -> Dict:
    """
    Stops all process-based CPU-intensive tasks.
    """
    global cpu_spike_processes
    with process_lock:
        if process_cpu_spike_active.is_set():
            process_cpu_spike_active.clear()

            cpu_spike_processes = []

            response.status_code = status.HTTP_202_ACCEPTED
            return {
                "message": "CPU Spike (process) stopped.",
                "process_cpu_spike_status": "Inactive"
            }
        else:
            return {
                "message": "CPU Spike already inactive.",
                "process_cpu_spike_status": "Inactive"
            }



if __name__ == '__main__':

    initialize_otel_metrics()
    uvicorn.run("app2:app", host='0.0.0.0', port=5000, log_level="info")
