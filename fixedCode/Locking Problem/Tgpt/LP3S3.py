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
        self.wait_times = []
        self._stats_lock = threading.Lock()


        for i in range(pool_size):
            self.resources.put(f"resource-{i}")

    def acquire_resource(self, worker_id, timeout=None):
        """
        Acquire a resource from the pool, recording wait time.
        IMPORTANT: Do NOT hold any external lock while blocking on the queue.
        """
        start_wait = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} requesting resource...")

        try:

            resource = self.resources.get(block=True, timeout=timeout)
        except queue.Empty:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} timed out waiting for resource")
            raise

        wait_duration = time.time() - start_wait

        with self._stats_lock:
            self.wait_times.append(wait_duration)

        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} acquired {resource} after waiting {wait_duration:.6f}s")
        return resource, wait_duration

    def release_resource(self, resource, worker_id):
        """Return a resource to the pool (Queue is thread-safe)."""
        self.resources.put(resource)
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} released {resource}")

    def get_wait_statistics(self):
        """Return statistics about acquisition wait times"""
        with self._stats_lock:
            if not self.wait_times:
                return {"min": 0, "max": 0, "avg": 0, "median": 0, "total_acquisitions": 0}
            return {
                "min": min(self.wait_times),
                "max": max(self.wait_times),
                "avg": sum(self.wait_times) / len(self.wait_times),
                "median": statistics.median(self.wait_times),
                "total_acquisitions": len(self.wait_times),
            }

def process_request(worker_id, resource_pool, request_complexity):
    """Simulates processing a web request"""
    resource = None
    wait_time = 0.0
    try:

        resource, wait_time = resource_pool.acquire_resource(worker_id, timeout=None)


        processing_time = request_complexity * random.uniform(0.1, 0.5)
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} processing (complexity: {request_complexity}) with {resource}")
        time.sleep(processing_time)

        return {
            "worker_id": worker_id,
            "wait_time": wait_time,
            "processing_time": processing_time,
            "total_time": wait_time + processing_time
        }
    except Exception as e:
        print(f"Error in worker {worker_id}: {e}")
        return None
    finally:

        if resource is not None:
            resource_pool.release_resource(resource, worker_id)

def simulate_web_server(num_workers, num_resources, num_requests, traffic_pattern="uniform"):
    """
    Simulates a web server handling requests
    """
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
            r = f.result()
            if r:
                results.append(r)

    total_simulation_time = time.time() - start_time


    print(f"\n{'='*80}")
    print(f"SIMULATION COMPLETED in {total_simulation_time:.2f} seconds")
    print(f"{'='*80}")

    lock_stats = resource_pool.get_wait_statistics()
    print("\nAcquisition Wait Statistics:")
    print(f"  Minimum wait time: {lock_stats['min']:.6f}s")
    print(f"  Maximum wait time: {lock_stats['max']:.6f}s")
    print(f"  Average wait time: {lock_stats['avg']:.6f}s")
    print(f"  Median wait time:  {lock_stats['median']:.6f}s")
    print(f"  Total acquisitions: {lock_stats['total_acquisitions']}")

    if results:
        wait_times = [r["wait_time"] for r in results]
        processing_times = [r["processing_time"] for r in results]
        total_times = [r["total_time"] for r in results]

        print("\nRequest Processing Statistics:")
        print(f"  Average wait time:      {statistics.mean(wait_times):.6f}s")
        print(f"  Average processing time:{statistics.mean(processing_times):.6f}s")
        print(f"  Average total time:     {statistics.mean(total_times):.6f}s")
        print(f"  Maximum total time:     {max(total_times):.6f}s")
        print(f"  Throughput:             {len(results) / total_simulation_time:.2f} req/s")

    print(f"\nResource Utilization: {len(results) / num_resources / total_simulation_time:.2f} req per resource per second")


if __name__ == "__main__":

    simulate_web_server(
        num_workers=10,
        num_resources=8,
        num_requests=30,
        traffic_pattern="uniform"
    )


    simulate_web_server(
        num_workers=20,
        num_resources=5,
        num_requests=40,
        traffic_pattern="uniform"
    )


    simulate_web_server(
        num_workers=25,
        num_resources=4,
        num_requests=50,
        traffic_pattern="burst"
    )
