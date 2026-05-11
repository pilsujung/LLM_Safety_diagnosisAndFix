import threading
import time
import random
import queue
from datetime import datetime
import statistics
from concurrent.futures import ThreadPoolExecutor

class ResourcePool:
    """Simulates a limited pool of resources (e.g., database connections)"""

    def __init__(self, pool_size, lock_timeout=1.0, resource_timeout=1.0):
        self.pool_size = pool_size
        self.resources = queue.Queue(pool_size)
        self.lock = threading.Lock()
        self.wait_times = []
        self.lock_timeout = lock_timeout
        self.resource_timeout = resource_timeout


        self.failed_lock_acquisitions = 0
        self.failed_resource_acquisitions = 0


        for i in range(pool_size):
            self.resources.put(f"resource-{i}")

    def acquire_resource(self, worker_id):
        """
        Acquire a resource from the pool with bounded waiting.

        Returns (resource, lock_wait_time).
        If a resource could not be obtained in time, resource is None.
        """

        start_wait = time.time()
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}] Worker {worker_id} waiting to acquire resource lock...")

        acquired = self.lock.acquire(timeout=self.lock_timeout)
        lock_wait = time.time() - start_wait

        if not acquired:

            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(
                f"[{ts}] Worker {worker_id} could not acquire a resource within "
                f"{self.lock_timeout:.2f}s (waited {lock_wait:.6f}s), moving on"
            )
            self.failed_lock_acquisitions += 1
            return None, lock_wait

        try:

            self.wait_times.append(lock_wait)
            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] Worker {worker_id} acquired resource lock after {lock_wait:.6f}s")
        finally:

            self.lock.release()


        resource_wait_start = time.time()
        try:

            resource = self.resources.get(timeout=self.resource_timeout)
        except queue.Empty:
            resource_wait = time.time() - resource_wait_start
            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(
                f"[{ts}] Worker {worker_id} could not get a resource in time "
                f"(waited {resource_wait:.6f}s), request dropped"
            )
            self.failed_resource_acquisitions += 1
            return None, lock_wait

        resource_wait = time.time() - resource_wait_start
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(
            f"[{ts}] Worker {worker_id} acquired {resource} after waiting "
            f"{resource_wait:.6f}s"
        )
        return resource, lock_wait

    def release_resource(self, resource, worker_id):
        """Returns a resource to the pool"""
        if resource is None:
            return
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}] Worker {worker_id} releasing {resource}")
        self.resources.put(resource)

    def get_wait_statistics(self):
        """Returns statistics about lock acquisition wait times"""
        if not self.wait_times:
            base_stats = {"min": 0, "max": 0, "avg": 0, "median": 0}
        else:
            base_stats = {
                "min": min(self.wait_times),
                "max": max(self.wait_times),
                "avg": sum(self.wait_times) / len(self.wait_times),
                "median": statistics.median(self.wait_times),
            }

        base_stats["total_acquisitions"] = len(self.wait_times)
        base_stats["failed_lock_acquisitions"] = self.failed_lock_acquisitions
        base_stats["failed_resource_acquisitions"] = self.failed_resource_acquisitions
        return base_stats


def process_request(worker_id, resource_pool, request_complexity):
    """Simulates processing a web request"""
    resource = None
    try:

        resource, wait_time = resource_pool.acquire_resource(worker_id)

        if resource is None:

            return None


        processing_time = request_complexity * random.uniform(0.1, 0.5)
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(
            f"[{ts}] Worker {worker_id} processing request "
            f"(complexity: {request_complexity:.2f}) with {resource}"
        )
        time.sleep(processing_time)

        total_time = wait_time + processing_time
        return {
            "worker_id": worker_id,
            "wait_time": wait_time,
            "processing_time": processing_time,
            "total_time": total_time,
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

    Parameters:
    - num_workers: Size of thread pool
    - num_resources: Number of available resources (e.g., DB connections)
    - num_requests: Total number of incoming requests to process
    - traffic_pattern: "uniform" or "burst"
    """
    print(f"\n{'='*80}")
    print(f"SIMULATION: {num_workers} workers, {num_resources} resources, "
          f"{num_requests} requests, {traffic_pattern} traffic")
    print(f"{'='*80}\n")


    resource_pool = ResourcePool(num_resources)


    requests = []
    if traffic_pattern == "uniform":
        for _ in range(num_requests):
            complexity = random.uniform(1, 3)
            requests.append(complexity)
    else:

        for _ in range(num_requests // 3):
            complexity = random.uniform(1, 2)
            requests.append(complexity)

        for _ in range(num_requests // 3):
            complexity = random.uniform(2.5, 4)
            requests.append(complexity)

        for _ in range(num_requests - len(requests)):
            complexity = random.uniform(1, 2)
            requests.append(complexity)


    start_time = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for req_id, complexity in enumerate(requests):
            future = executor.submit(process_request, req_id, resource_pool, complexity)
            futures.append(future)


        for future in futures:
            result = future.result()
            if result:
                results.append(result)

    total_simulation_time = time.time() - start_time


    print(f"\n{'='*80}")
    print(f"SIMULATION COMPLETED in {total_simulation_time:.2f} seconds")
    print(f"{'='*80}")


    lock_stats = resource_pool.get_wait_statistics()
    print("\nLock Acquisition Statistics:")
    print(f"  Minimum wait time: {lock_stats['min']:.6f}s")
    print(f"  Maximum wait time: {lock_stats['max']:.6f}s")
    print(f"  Average wait time: {lock_stats['avg']:.6f}s")
    print(f"  Median wait time: {lock_stats['median']:.6f}s")
    print(f"  Total lock acquisitions: {lock_stats['total_acquisitions']}")
    print(f"  Failed lock acquisitions: {lock_stats['failed_lock_acquisitions']}")
    print(f"  Failed resource acquisitions: {lock_stats['failed_resource_acquisitions']}")


    if results:
        wait_times = [r["wait_time"] for r in results]
        processing_times = [r["processing_time"] for r in results]
        total_times = [r["total_time"] for r in results]

        print("\nRequest Processing Statistics:")
        print(f"  Average wait time: {statistics.mean(wait_times):.6f}s")
        print(f"  Average processing time: {statistics.mean(processing_times):.6f}s")
        print(f"  Average total time: {statistics.mean(total_times):.6f}s")
        print(f"  Maximum total time: {max(total_times):.6f}s")
        print(f"  Throughput: {len(results) / total_simulation_time:.2f} requests/second")
        print(f"  Completed requests: {len(results)}")
        print(f"  Dropped requests: {num_requests - len(results)}")
    else:
        print("\nNo requests completed; all were dropped.")

    if total_simulation_time > 0:
        print(f"\nResource Utilization: "
              f"{len(results) / num_resources / total_simulation_time:.2f} "
              f"successful requests per resource per second")
    else:
        print("\nResource Utilization: N/A (zero simulation time)")



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
