import time
import subprocess
import psutil
import statistics
from threading import Thread

def monitor_resources():
    cpu_percent = []
    mem_percent = []

    def monitor():
        while getattr(monitor, "running", True):
            cpu_percent.append(psutil.cpu_percent(interval=0.1))
            mem_percent.append(psutil.virtual_memory().percent)

    monitor.running = True
    t = Thread(target=monitor)
    t.start()
    return cpu_percent, mem_percent, t, monitor

def run_process(script_name):
    cpu_data, mem_data, monitor_thread, monitor = monitor_resources()

    start = time.time()
    process = subprocess.Popen(
        ["spark-submit", script_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    end = time.time()

    monitor.running = False
    monitor_thread.join()

    error_output = stderr.decode()
    # Filter out known Spark boilerplate warnings
    filtered = []
    for line in error_output.splitlines():
        if not any(ignore in line for ignore in [
            "ShutdownHookManager", 
            "WARN Utils: Set SPARK_LOCAL_IP", 
            "WARN NativeCodeLoader",
            "WARN Utils: Service 'SparkUI'",
            "resolves to a loopback address",
            "using builtin-java classes"
        ]):
            filtered.append(line)
    
    cleaned_errors = "\n".join(filtered)

    return {
        "time": end - start,
        "avg_cpu": statistics.mean(cpu_data),
        "max_cpu": max(cpu_data),
        "avg_mem": statistics.mean(mem_data),
        "max_mem": max(mem_data),
        "exit_code": process.returncode,
        "output": stdout.decode(),
        "errors": cleaned_errors.strip()[-400:]
    }

def print_metrics(label, metrics):
    print(f"\n{label} Metrics:")
    print(f"- Time Taken: {metrics['time']:.2f} seconds")
    print(f"- CPU Usage: Avg {metrics['avg_cpu']:.1f}%, Max {metrics['max_cpu']:.1f}%")
    print(f"- Memory Usage: Avg {metrics['avg_mem']:.1f}%, Max {metrics['max_mem']:.1f}%")
    if metrics['exit_code'] != 0:
        print(f"! WARNING: {label} exited with code {metrics['exit_code']}")
        if metrics['errors']:
            print("Error (last 400 chars):")
            print(metrics['errors'])
        else:
            print("(No significant error messages)")

if __name__ == "__main__":
    print("Starting Streaming vs Batch Performance Comparison...")

    print("\nRunning Streaming Job...")
    streaming = run_process("spark_streaming.py")

    print("\nRunning Batch Job...")
    batch = run_process("batch_processing.py")

    print_metrics("Streaming", streaming)
    print_metrics("Batch", batch)

    print("\n🧮 Summary:")
    time_diff = abs(streaming['time'] - batch['time'])
    print(f"- Time Difference: {time_diff:.2f} seconds")
    print(f"- {'Batch' if batch['time'] < streaming['time'] else 'Streaming'} was faster.")
