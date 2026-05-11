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
    original_priority: int = field(compare=False)
    request_time: float = field(compare=False)
    thread_id: int = field(compare=False)
    wait_time: float = field(default=0.0, compare=False)

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.in_use = False
        self.lock = threading.Lock()
        self.current_user = None
    
    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        with self.lock:
            if self.in_use:
                raise RuntimeError(f"Resource {self.name} is already in use by {self.current_user}")
            self.in_use = True
            self.current_user = thread_id
            
        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        time.sleep(duration)
        
        with self.lock:
            self.in_use = False
            self.current_user = None
            
        logging.info(f"Thread {thread_id} released {self.name}")

class AntiStarvationResourceManager:
    def __init__(self, resource: SharedResource, starvation_threshold: float = 5.0, aging_factor: float = 0.1):
        self.resource = resource
        self.request_queue = queue.PriorityQueue()
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True
        self.starvation_threshold = starvation_threshold
        self.aging_factor = aging_factor
        self.wait_times: Dict[int, List[float]] = {}
        self.pending_requests: Dict[int, ResourceRequest] = {}
        

        self.allocation_thread = threading.Thread(target=self._allocate_resources)
        self.allocation_thread.daemon = True
        self.allocation_thread.start()
        

        self.aging_thread = threading.Thread(target=self._age_requests)
        self.aging_thread.daemon = True
        self.aging_thread.start()
    
    def _calculate_effective_priority(self, original_priority: int, wait_time: float) -> float:
        """Calculate effective priority with aging to prevent starvation"""


        aging_bonus = wait_time * self.aging_factor
        effective_priority = original_priority - aging_bonus
        

        return max(0.0, effective_priority)
    
    def _age_requests(self):
        """Periodically update the effective priorities of waiting requests"""
        while self.active:
            current_time = time.time()
            updated_requests = []
            
            with self.lock:

                temp_queue = queue.PriorityQueue()
                
                while not self.request_queue.empty():
                    try:
                        request = self.request_queue.get_nowait()
                        wait_time = current_time - request.request_time
                        

                        updated_request = ResourceRequest(
                            effective_priority=self._calculate_effective_priority(request.original_priority, wait_time),
                            original_priority=request.original_priority,
                            request_time=request.request_time,
                            thread_id=request.thread_id,
                            wait_time=wait_time
                        )
                        
                        temp_queue.put(updated_request)
                        

                        if wait_time > self.starvation_threshold:
                            priority_name = self._get_priority_name(request.original_priority)
                            logging.warning(f"Thread {request.thread_id} ({priority_name}) waiting {wait_time:.2f}s - priority boosted to {updated_request.effective_priority:.2f}")
                        
                    except queue.Empty:
                        break
                

                self.request_queue = temp_queue
                

                self.cv.notify_all()
            
            time.sleep(1.0)
    
    def _get_priority_name(self, priority: int) -> str:
        """Convert priority number to human-readable name"""
        if priority == 1:
            return "High"
        elif priority == 3:
            return "Medium"
        elif priority == 5:
            return "Low"
        else:
            return f"Priority-{priority}"
    
    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time"""
        request_time = time.time()
        
        with self.lock:

            request = ResourceRequest(
                effective_priority=float(priority),
                original_priority=priority,
                request_time=request_time,
                thread_id=thread_id
            )
            

            self.request_queue.put(request)
            self.pending_requests[thread_id] = request
            
            priority_name = self._get_priority_name(priority)
            logging.info(f"Thread {thread_id} ({priority_name}) requested resource")
            

            while self.active and thread_id in self.pending_requests:

                if not self.resource.in_use and not self.request_queue.empty():
                    try:

                        top_request = self.request_queue.queue[0]
                        if top_request.thread_id == thread_id:

                            self.request_queue.get_nowait()
                            del self.pending_requests[thread_id]
                            break
                    except (queue.Empty, IndexError):
                        pass
                

                self.cv.wait(timeout=1.0)
        

        wait_time = time.time() - request_time
        

        if thread_id not in self.wait_times:
            self.wait_times[thread_id] = []
        self.wait_times[thread_id].append(wait_time)
        
        priority_name = self._get_priority_name(priority)
        logging.info(f"Thread {thread_id} ({priority_name}) granted access after waiting {wait_time:.2f} seconds")
        

        self.resource.use_resource(thread_id, use_duration)
        

        with self.lock:
            self.cv.notify_all()
        
        return wait_time
    
    def _allocate_resources(self):
        """Thread that handles resource allocation notifications"""
        while self.active:
            with self.lock:
                if not self.request_queue.empty() and not self.resource.in_use:

                    self.cv.notify_all()
            
            time.sleep(0.1)
    
    def stop(self):
        """Stop the resource manager"""
        self.active = False
        

        with self.lock:
            self.pending_requests.clear()
            self.cv.notify_all()
        

        if self.allocation_thread.is_alive():
            self.allocation_thread.join(timeout=1.0)
        if self.aging_thread.is_alive():
            self.aging_thread.join(timeout=1.0)
    
    def get_statistics(self) -> Dict:
        """Get statistics about wait times"""
        stats = {}
        for thread_id, times in self.wait_times.items():
            if times:
                stats[thread_id] = {
                    "min_wait": min(times),
                    "max_wait": max(times),
                    "avg_wait": sum(times) / len(times),
                    "total_waits": len(times)
                }
        return stats

def worker_thread(thread_id: int, priority: int, manager: AntiStarvationResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource"""
    for i in range(iterations):
        if not manager.active:
            break
            

        use_duration = random.uniform(0.1, 0.5)
        
        try:

            wait_time = manager.request_resource(thread_id, priority, use_duration)
            

            think_time = random.uniform(0.1, 1.0)
            time.sleep(think_time)
        except Exception as e:
            logging.error(f"Thread {thread_id} encountered error: {e}")
            break

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    """Run the anti-starvation simulation"""

    resource = SharedResource("Database Connection")
    

    manager = AntiStarvationResourceManager(
        resource, 
        starvation_threshold=5.0,
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
            args=(i, priority, manager, 100)
        )
        thread.daemon = True
        threads.append(thread)
    

    logging.info(f"Starting anti-starvation simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
    logging.info(f"Aging factor: {manager.aging_factor} (priority improves by this amount per second)")
    start_time = time.time()
    
    for thread in threads:
        thread.start()
    

    try:
        while time.time() - start_time < runtime_seconds:
            time.sleep(1)
            

            if int(time.time() - start_time) % 10 == 0:
                with manager.lock:
                    queue_size = manager.request_queue.qsize()
                    logging.info(f"Queue size: {queue_size}, Resource in use: {resource.in_use}")
                    
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")
    

    logging.info("Stopping simulation...")
    manager.stop()
    

    for thread in threads:
        thread.join(timeout=1.0)
    

    stats = manager.get_statistics()
    logging.info("\n" + "="*50)
    logging.info("ANTI-STARVATION SIMULATION RESULTS")
    logging.info("="*50)
    
    if not stats:
        logging.warning("No statistics available - threads may not have completed any requests")
        return
    
    total_accesses = 0
    for thread_id, thread_stats in stats.items():
        priority_level = "High" if priorities[thread_id] == 1 else "Medium" if priorities[thread_id] == 3 else "Low"
        logging.info(f"\nThread {thread_id} ({priority_level} Priority):")
        logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {thread_stats['total_waits']}")
        total_accesses += thread_stats['total_waits']
    
    logging.info(f"\nTotal resource accesses: {total_accesses}")
    logging.info("="*50)

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)