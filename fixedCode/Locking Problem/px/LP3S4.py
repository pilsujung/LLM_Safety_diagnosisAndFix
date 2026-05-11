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
        self.wait_times_lock = threading.Lock()
        self.wait_times = []

        for i in range(pool_size):
            self.resources.put(f"resource-{i}")

    def acquire_resource(self, worker_id):
        """Acquires a resource from the pool, recording wait time NON-BLOCKING"""
        start_wait = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} attempting to acquire resource...")
        
        try:

            resource = self.resources.get(block=False, timeout=0.01)
            wait_duration = time.time() - start_wait
            

            with self.wait_times_lock:
                self.wait_times.append(wait_duration)
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} acquired {resource} after {wait_duration:.6f}s")
            return resource, wait_duration
            
        except queue.Empty:

            wait_duration = time.time() - start_wait
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} NO RESOURCE AVAILABLE after {wait_duration:.6f}s - RETRYING")
            

            time.sleep(0.001 * random.uniform(1, 3))
            return self.acquire_resource(worker_id)

    def release_resource(self, resource, worker_id):
        """Returns a resource to the pool"""
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} releasing {resource}")
        self.resources.put(resource)

    def get_wait_statistics(self):
        """Returns statistics about wait times"""
        with self.wait_times_lock:
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
    """Simulates processing a web request"""
    max_retries = 50
    retries = 0
    
    while retries < max_retries:
        try:
            resource, wait_time = resource_pool.acquire_resource(worker_id)
            

            processing_time = request_complexity * random.uniform(0.1, 0.5)
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} processing (complexity: {request_complexity}) with {resource}")
            time.sleep(processing_time)
            
            resource_pool.release_resource(resource, worker_id)
            
            return {
                "worker_id": worker_id,
                "wait_time": wait_time,
                "processing_time": processing_time,
                "total_time": wait_time + processing_time,
                "retries": retries
            }
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                print(f"Worker {worker_id} FAILED after {max_retries} retries: {e}")
                return None
            time.sleep(0.001)

def simulate_web_server(num_workers, num_resources, num_requests, traffic_pattern="uniform"):
    print(f"\n{'='*80}")
    print(f"SIMULATION: {num_workers} workers, {num_resources} resources, {num_requests} requests, {traffic_pattern} traffic")
    print(f"{'='*80}\n")

    resource_pool = ResourcePool(num_resources)
    requests = []
    
    if traffic_pattern == "uniform":
        for i in range(num_requests):
            complexity = random.uniform(1, 3)
            requests.append(complexity)
    else:
        for i in range(num_requests // 3):
            complexity = random.uniform(1, 2)
            requests.append(complexity)
        for i in range(num_requests // 3):
            complexity = random.uniform(2.5, 4)
            requests.append(complexity)
        for i in range(num_requests - len(requests)):
            complexity = random.uniform(1, 2)
            requests.append(complexity)

    start_time = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_request, req_id, resource_pool, complexity) 
                  for req_id, complexity in enumerate(requests)]
        
        for future in futures:
            try:
                result = future.result(timeout=10)
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Future timed out or errored: {e}")

    total_simulation_time = time.time() - start_time

    print(f"\n{'='*80}")
    print(f"SIMULATION COMPLETED in {total_simulation_time:.2f} seconds")
    print(f"{'='*80}")

    lock_stats = resource_pool.get_wait_statistics()
    print("\nLock Acquisition Statistics:")
    print(f" Minimum wait time: {lock_stats['min']:.6f}s")
    print(f" Maximum wait time: {lock_stats['max']:.6f}s")
    print(f" Average wait time: {lock_stats['avg']:.6f}s")
    print(f" Median wait time: {lock_stats['median']:.6f}s")
    print(f" Total acquisitions: {lock_stats['total_acquisitions']}")

    if results:
        wait_times = [r["wait_time"] for r in results]
        processing_times = [r["processing_time"] for r in results]
        total_times = [r["total_time"] for r in results]
        
        print("\nRequest Processing Statistics:")
        print(f" Average wait time: {statistics.mean(wait_times):.6f}s")
        print(f" Average processing time: {statistics.mean(processing_times):.6f}s")
        print(f" Average total time: {statistics.mean(total_times):.6f}s")
        print(f" Max total time: {max(total_times):.6f}s")
        print(f" Throughput: {len(results) / total_simulation_time:.2f} req/s")
        print(f" Failed requests: {len(requests) - len(results)}")

if __name__ == "__main__":

    simulate_web_server(10, 8, 30, "uniform")
    simulate_web_server(20, 5, 40, "uniform")
    simulate_web_server(25, 4, 50, "burst")
