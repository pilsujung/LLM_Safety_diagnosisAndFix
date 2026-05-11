import threading
import time
import random
import queue
import logging
from dataclasses import dataclass, field
from typing import List, Dict


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass(order=True)
class ResourceRequest:
    effective_priority: float
    request_time: float
    original_priority: int = field(compare=False)
    thread_id: int = field(compare=False)
    wait_time: float = field(default=0.0, compare=False)

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.in_use = False
        self.lock = threading.Lock()
    
    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        with self.lock:
            self.in_use = True
            
        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        time.sleep(duration)
        
        with self.lock:
            self.in_use = False
            
        logging.info(f"Thread {thread_id} released {self.name}")

class FairResourceManager:
    def __init__(self, resource: SharedResource, aging_factor: float = 0.1):
        self.resource = resource
        self.request_queue = queue.PriorityQueue()
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True
        self.aging_factor = aging_factor
        self.wait_times: Dict[int, List[float]] = {}
        self.pending_requests: Dict[int, ResourceRequest] = {}
        

        self.allocation_thread = threading.Thread(target=self._allocate_resources)
        self.allocation_thread.daemon = True
        self.allocation_thread.start()
        

        self.aging_thread = threading.Thread(target=self._age_requests)
        self.aging_thread.daemon = True
        self.aging_thread.start()
    
    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time"""
        request_time = time.time()
        
        with self.lock:

            request = ResourceRequest(
                effective_priority=float(priority),
                request_time=request_time,
                original_priority=priority,
                thread_id=thread_id
            )
            

            self.pending_requests[thread_id] = request
            

            self.request_queue.put(request)
            
            logging.info(f"Thread {thread_id} (original priority {priority}) requested resource")
            

            while self.active and thread_id in self.pending_requests:

                self.cv.wait(timeout=1.0)
        

        wait_time = time.time() - request_time
        

        if thread_id not in self.wait_times:
            self.wait_times[thread_id] = []
        self.wait_times[thread_id].append(wait_time)
        
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")
        

        self.resource.use_resource(thread_id, use_duration)
        
        return wait_time
    
    def _age_requests(self):
        """Thread that ages waiting requests to prevent starvation"""
        while self.active:
            with self.lock:
                current_time = time.time()
                aged_requests = []
                

                for thread_id, request in self.pending_requests.items():
                    wait_time = current_time - request.request_time

                    old_priority = request.effective_priority
                    request.effective_priority = request.original_priority - (wait_time * self.aging_factor)
                    
                    if abs(old_priority - request.effective_priority) > 0.1:
                        logging.info(f"Thread {thread_id} priority aged: {old_priority:.2f} -> {request.effective_priority:.2f} (waited {wait_time:.1f}s)")
                    
                    aged_requests.append(request)
                

                if aged_requests:

                    temp_queue = queue.PriorityQueue()
                    while not self.request_queue.empty():
                        try:
                            self.request_queue.get_nowait()
                        except queue.Empty:
                            break
                    
                    for request in aged_requests:
                        temp_queue.put(request)
                    
                    self.request_queue = temp_queue
                    

                    self.cv.notify_all()
            
            time.sleep(0.5)
    
    def _allocate_resources(self):
        """Thread that handles resource allocation"""
        while self.active:
            with self.lock:
                if not self.request_queue.empty() and not self.resource.in_use:
                    try:

                        request = self.request_queue.get_nowait()
                        thread_id = request.thread_id
                        

                        if thread_id in self.pending_requests:
                            del self.pending_requests[thread_id]
                        
                        logging.info(f"Allocating resource to thread {thread_id} with effective priority {request.effective_priority:.2f}")
                        

                        self.cv.notify_all()
                    except queue.Empty:
                        pass
            
            time.sleep(0.1)
    
    def stop(self):
        """Stop the resource manager"""
        self.active = False
        self.allocation_thread.join(timeout=1.0)
        self.aging_thread.join(timeout=1.0)
    
    def get_statistics(self) -> Dict:
        """Get statistics about wait times"""
        stats = {}
        for thread_id, times in self.wait_times.items():
            stats[thread_id] = {
                "min_wait": min(times),
                "max_wait": max(times),
                "total_waits": len(times)
            }
            if times:
                stats[thread_id]["avg_wait"] = sum(times) / len(times)
        return stats

def worker_thread(thread_id: int, priority: int, manager: FairResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource"""
    for i in range(iterations):

        use_duration = random.uniform(0.1, 0.5)
        

        wait_time = manager.request_resource(thread_id, priority, use_duration)
        

        think_time = random.uniform(0.1, 1.0)
        time.sleep(think_time)

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    """Run the fair resource allocation simulation"""

    resource = SharedResource("Database Connection")
    

    manager = FairResourceManager(resource, aging_factor=0.2)
    

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
            args=(i, priority, manager, 100)
        )
        thread.daemon = True
        threads.append(thread)
    

    logging.info(f"Starting fair simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
    logging.info("Using aging mechanism to prevent starvation")
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
        thread.join(timeout=0.5)
    

    stats = manager.get_statistics()
    logging.info("\n----- Fair Simulation Results -----")
    for thread_id, thread_stats in stats.items():
        priority_level = "High" if priorities[thread_id] == 1 else "Medium" if priorities[thread_id] == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {priority_level}):")
        logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
        if 'avg_wait' in thread_stats:
            logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {thread_stats['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)