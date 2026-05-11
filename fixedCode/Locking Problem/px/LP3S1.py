import threading
import time
import random
import queue
from datetime import datetime
import statistics
from concurrent.futures import ThreadPoolExecutor

class ResourcePool:
    """Simulates a limited pool of resources (e.g., database connections)"""
    def __init__(self, pool_size):
        self.pool_size = pool_size
        self.resources = queue.Queue(pool_size)
        self.lock = threading.Lock()
        self.wait_times = []


        for i in range(pool_size):
            self.resources.put(f"resource-{i}")

    def acquire_resource(self, worker_id):
        """Acquires a resource from the pool, recording wait time"""
        start_wait = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} requesting resource...")


        resource = self.resources.get(block=True)
        resource_wait_duration = time.time() - start_wait


        with self.lock:
            self.wait_times.append(resource_wait_duration)

        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} acquired {resource} after waiting {resource_wait_duration:.6f}s")
        return resource, resource_wait_duration

    def release_resource(self, resource, worker_id):
        """Returns a resource to the pool"""
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} releasing {resource}")
        self.resources.put(resource)

    def get_wait_statistics(self):
        """Returns statistics about lock acquisition wait times"""
        with self.lock:
            if not self.wait_times:
                return {"min": 0, "max": 0, "avg": 0, "median": 0, "total_acquisitions": 0}

            return {
                "min": min(self.wait_times),
                "max": max(self.wait_times),
                "avg": sum(self.wait_times) / len(self.wait_times),
                "median": statistics.median(self.wait_times),
                "total_acquisitions": len(self.wait_times)
            }

def process_request(worker_id, resource_pool, request_complexity):
    try:
        resource, wait_time = resource_pool.acquire_resource(worker_id)

        processing_time = request_complexity * random.uniform(0.1, 0.5)
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} processing request (complexity: {request_complexity}) with {resource}")
        time.sleep(processing_time)

        resource_pool.release_resource(resource, worker_id)

        return {
            "worker_id": worker_id,
            "wait_time": wait_time,
            "processing_time": processing_time,
            "total_time": wait_time + processing_time
        }
    except Exception as e:
        print(f"Error in worker {worker_id}: {e}")
        return None

def simulate_web_server(num_workers, num_resources, num_requests, traffic_pattern="uniform"):
    print(f"\n{'='*80}")
    print(f"SIMULATION: {num_workers} workers, {num_resources} resources, {num_requests} requests, {traffic_pattern} traffic")
    print(f"{'='*80}\n")

    resource_pool = ResourcePool(num_resources)

    requests = []
    if traffic_pattern == "uniform":
        for _ in range(num_requests):
            requests.append(random.uniform(1, 3))
    else:
        for _ in range(num_requests // 3):
            requests.append(random.uniform(1, 2))
        for _ in range(num_requests // 3):
            requests.append(random.uniform(2.5, 4))
        for _ in range(num_requests - len(requests)):
            requests.append(random.uniform(1, 2))

    start_time = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_request, i, resource_pool, c) for i, c in enumerate(requests)]
        for f in futures:
            res = f.result()
            if res:
                results.append(res)

    total_simulation_time = time.time() - start_time

    print(f"\n{'='*80}")
    print(f"SIMULATION COMPLETED in {total_simulation_time:.2f} seconds")
    print(f"{'='*80}")

    stats = resource_pool.get_wait_statistics()
    print("\nLock Acquisition Statistics:")
    print(f" Min: {stats['min']:.6f}s  Max: {stats['max']:.6f}s  Avg: {stats['avg']:.6f}s  Median: {stats['median']:.6f}s  Total: {stats['total_acquisitions']}")

    if results:
        wait_times = [r['wait_time'] for r in results]
        total_times = [r['total_time'] for r in results]
        print(f"\nAverage total time: {statistics.mean(total_times):.6f}s")
        print(f"Throughput: {len(results) / total_simulation_time:.2f} requests/sec")

if __name__ == "__main__":
    simulate_web_server(10, 8, 30, "uniform")
    simulate_web_server(20, 5, 40, "uniform")
    simulate_web_server(25, 4, 50, "burst")
