import threading
import time
import random
import heapq
import logging
from dataclasses import dataclass, field
from typing import List, Dict


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass
class ResourceRequest:
    original_priority: int
    current_priority: float
    request_time: float
    thread_id: int
    wait_time: float = field(default=0.0)
    
    def __lt__(self, other):

        if self.current_priority != other.current_priority:
            return self.current_priority < other.current_priority

        return self.request_time < other.request_time

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.in_use = False
        self.current_user = None
        self.lock = threading.Lock()
    
    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        with self.lock:
            self.in_use = True
            self.current_user = thread_id
            
        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        time.sleep(duration)
        
        with self.lock:
            self.in_use = False
            self.current_user = None
            
        logging.info(f"Thread {thread_id} released {self.name}")

class StarvationFreeResourceManager:
    def __init__(self, resource: SharedResource, starvation_threshold: float = 5.0, aging_factor: float = 0.1):
        self.resource = resource
        self.request_heap = []
        self.lock = threading.Lock()
        self.resource_available = threading.Condition(self.lock)
        self.active = True
        self.starvation_threshold = starvation_threshold
        self.aging_factor = aging_factor
        self.wait_times: Dict[int, List[float]] = {}
        self.pending_requests: Dict[int, ResourceRequest] = {}
        

        self.aging_thread = threading.Thread(target=self._age_priorities)
        self.aging_thread.daemon = True
        self.aging_thread.start()
    
    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time"""
        request_time = time.time()
        
        with self.lock:

            request = ResourceRequest(
                original_priority=priority,
                current_priority=float(priority),
                request_time=request_time,
                thread_id=thread_id
            )
            

            heapq.heappush(self.request_heap, request)
            self.pending_requests[thread_id] = request
            
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")
            

            while self.active and thread_id in self.pending_requests:

                if not self.resource.in_use and self.request_heap and self.request_heap[0].thread_id == thread_id:

                    granted_request = heapq.heappop(self.request_heap)
                    del self.pending_requests[thread_id]
                    

                    with self.resource.lock:
                        self.resource.in_use = True
                        self.resource.current_user = thread_id
                    
                    break
                

                self.resource_available.wait(timeout=1.0)
        

        wait_time = time.time() - request_time
        

        if thread_id not in self.wait_times:
            self.wait_times[thread_id] = []
        self.wait_times[thread_id].append(wait_time)
        
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds (aged priority: {request.current_priority:.2f})")
        

        try:
            logging.info(f"Thread {thread_id} is using {self.resource.name} for {use_duration:.2f} seconds")
            time.sleep(use_duration)
        finally:

            with self.resource.lock:
                self.resource.in_use = False
                self.resource.current_user = None
            

            with self.lock:
                self.resource_available.notify_all()
            
            logging.info(f"Thread {thread_id} released {self.resource.name}")
        
        return wait_time
    
    def _age_priorities(self):
        """Thread that ages priorities to prevent starvation"""
        while self.active:
            current_time = time.time()
            
            with self.lock:

                aged_requests = []
                
                for request in self.pending_requests.values():
                    wait_time = current_time - request.request_time
                    


                    aging_improvement = wait_time * self.aging_factor
                    request.current_priority = request.original_priority - aging_improvement
                    

                    request.current_priority = max(0.0, request.current_priority)
                    

                    if wait_time > self.starvation_threshold:
                        logging.warning(f"Thread {request.thread_id} waiting {wait_time:.2f}s (priority aged from {request.original_priority} to {request.current_priority:.2f})")
                

                if self.request_heap:

                    temp_list = list(self.request_heap)
                    self.request_heap.clear()
                    for request in temp_list:
                        heapq.heappush(self.request_heap, request)
            
            time.sleep(0.5)
    
    def stop(self):
        """Stop the resource manager"""
        self.active = False
        if self.aging_thread.is_alive():
            self.aging_thread.join(timeout=1.0)
        

        with self.lock:
            self.resource_available.notify_all()
    
    def get_statistics(self) -> Dict:
        """Get statistics about wait times"""
        stats = {}
        for thread_id, times in self.wait_times.items():
            if times:
                stats[thread_id] = {
                    "min_wait": min(times),
                    "max_wait": max(times),
                    "avg_wait": sum(times) / len(times),
                    "total_accesses": len(times)
                }
        return stats

def worker_thread(thread_id: int, priority: int, manager: StarvationFreeResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource"""
    for i in range(iterations):
        try:

            use_duration = random.uniform(0.1, 0.5)
            

            wait_time = manager.request_resource(thread_id, priority, use_duration)
            

            think_time = random.uniform(0.1, 1.0)
            time.sleep(think_time)
            
        except Exception as e:
            logging.error(f"Thread {thread_id} encountered error: {e}")
            break

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    """Run the starvation-free simulation"""

    resource = SharedResource("Database Connection")
    

    manager = StarvationFreeResourceManager(
        resource, 
        starvation_threshold=3.0,
        aging_factor=0.2
    )
    

    threads = []
    priorities = {}
    
    for i in range(num_threads):

        if i < 2:
            priority = 1
        elif i < 4:
            priority = 3
        else:
            priority = 5
        
        priorities[i] = priority
        thread = threading.Thread(
            target=worker_thread,
            args=(i, priority, manager, 20)
        )
        thread.daemon = True
        threads.append(thread)
    

    logging.info(f"Starting starvation-free simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
    logging.info(f"Aging factor: {manager.aging_factor}, Starvation threshold: {manager.starvation_threshold}s")
    
    start_time = time.time()
    
    for thread in threads:
        thread.start()
    

    try:
        while time.time() - start_time < runtime_seconds:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")
    

    manager.stop()
    

    for thread in threads:
        thread.join(timeout=2.0)
    

    stats = manager.get_statistics()
    logging.info("\n----- Starvation-Free Simulation Results -----")
    
    if not stats:
        logging.info("No statistics available - threads may not have completed any requests")
        return
    
    for thread_id, thread_stats in stats.items():
        priority_name = "High" if priorities[thread_id] == 1 else "Medium" if priorities[thread_id] == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {priority_name}):")
        logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f"  Total accesses: {thread_stats['total_accesses']}")
    

    low_priority_threads = [tid for tid, prio in priorities.items() if prio == 5]
    high_priority_threads = [tid for tid, prio in priorities.items() if prio == 1]
    
    if low_priority_threads and all(tid in stats for tid in low_priority_threads):
        logging.info(f"\n----- Fairness Analysis -----")
        for tid in low_priority_threads:
            if tid in stats:
                logging.info(f"Low priority thread {tid} successfully accessed resource {stats[tid]['total_accesses']} times")
        logging.info("✓ Starvation has been prevented!")
    else:
        logging.warning("⚠ Some low priority threads may still be starved")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=20)