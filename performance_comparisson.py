import time
import subprocess

def run_streaming():
    start = time.time()
    subprocess.run(["spark-submit", "spark_streaming.py"])
    return time.time() - start

def run_batch():
    start = time.time()
    subprocess.run(["spark-submit", "batch_processing.py"])
    return time.time() - start

streaming_time = run_streaming()
batch_time = run_batch()

print("\nPerformance Comparison:")
print(f"Streaming Processing Time: {streaming_time:.2f} seconds")
print(f"Batch Processing Time: {batch_time:.2f} seconds")
print(f"Difference: {abs(streaming_time - batch_time):.2f} seconds")
