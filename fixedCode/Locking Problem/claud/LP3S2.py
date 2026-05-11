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
    
    def acquire_resource(self, worker_id, timeout=1.0):
        """Acquires a resource from the pool with timeout, recording wait time"""
        start_wait = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} waiting to acquire resource lock...")
        

        if self.lock.acquire(timeout=timeout):
            try:
                wait_duration = time.time() - start_wait
                self.wait_times.append(wait_duration)
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} acquired resource lock after {wait_duration:.6f}s")
                

                resource_wait_start = time.time()
                try:
                    resource = self.resources.get(block=True, timeout=timeout)
                    resource_wait_duration = time.time() - resource_wait_start
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} acquired {resource} after waiting {resource_wait_duration:.6f}s")
                    return resource, wait_duration
                except queue.Empty:
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} could not acquire resource within timeout, moving on")
                    return None, wait_duration
            finally:
                self.lock.release()
        else:
            wait_duration = time.time() - start_wait
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} could not acquire lock within timeout, moving on")
            return None, wait_duration
    
    def release_resource(self, resource, worker_id):
        """Returns a resource to the pool"""
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} releasing {resource}")
        self.resources.put(resource)
    
    def get_wait_statistics(self):
        """Returns statistics about lock acquisition wait times"""
        if not self.wait_times:
            return {"min": 0, "max": 0, "avg": 0, "median": 0}
        
        return {
            "min": min(self.wait_times),
            "max": max(self.wait_times),
            "avg": sum(self.wait_times) / len(self.wait_times),
            "median": statistics.median(self.wait_times) if self.wait_times else 0,
            "total_acquisitions": len(self.wait_times)
        }

def process_request(worker_id, resource_pool, request_complexity):
    """Simulates processing a web request"""
    try:

        result = resource_pool.acquire_resource(worker_id, timeout=2.0)
        
        if result[0] is None:

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} failed to acquire resource, request dropped")
            return {
                "worker_id": worker_id,
                "wait_time": result[1],
                "processing_time": 0,
                "total_time": result[1],
                "success": False
            }
        
        resource, wait_time = result
        

        processing_time = request_complexity * random.uniform(0.1, 0.3)
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Worker {worker_id} processing request (complexity: {request_complexity}) with {resource}")
        time.sleep(processing_time)
        

        resource_pool.release_resource(resource, worker_id)
        
        return {
            "worker_id": worker_id,
            "wait_time": wait_time,
            "processing_time": processing_time,
            "total_time": wait_time + processing_time,
            "success": True
        }
    except Exception as e:
        print(f"Error in worker {worker_id}: {e}")
        return None

def simulate_web_server(num_workers, num_resources, num_requests, traffic_pattern="uniform"):
    """
    Simulates a web server handling requests
    
    Parameters:
    - num_workers: Size of thread pool
    - num_resources: Number of available resources (e.g., DB connections)
    - num_requests: Total number of incoming requests to process
    - traffic_pattern: "uniform" or "burst" to simulate different load patterns
    """
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
    

    if results:
        successful_results = [r for r in results if r.get("success", True)]
        failed_results = [r for r in results if not r.get("success", True)]
        
        print(f"\nSuccess Rate: {len(successful_results)}/{len(results)} ({len(successful_results)/len(results)*100:.1f}%)")
        
        if successful_results:
            wait_times = [r["wait_time"] for r in successful_results]
            processing_times = [r["processing_time"] for r in successful_results]
            total_times = [r["total_time"] for r in successful_results]
            
            print("\nRequest Processing Statistics (Successful Requests):")
            print(f"  Average wait time: {statistics.mean(wait_times):.6f}s")
            print(f"  Average processing time: {statistics.mean(processing_times):.6f}s")
            print(f"  Average total time: {statistics.mean(total_times):.6f}s")
            print(f"  Maximum total time: {max(total_times):.6f}s")
            print(f"  Throughput: {len(successful_results) / total_simulation_time:.2f} requests/second")
    
    print(f"\nResource Utilization: {len(results) / num_resources / total_simulation_time:.2f} requests per resource per second")


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