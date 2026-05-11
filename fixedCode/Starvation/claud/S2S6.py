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
    effective_priority: int
    original_priority: int = field(compare=False)
    request_time: float = field(compare=False)
    thread_id: int = field(compare=False)
    wait_time: float = field(default=0.0, compare=False)

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
    def __init__(self, resource: SharedResource, starvation_threshold: float = 5.0, aging_factor: float = 0.5):
        self.resource = resource
        self.request_queue = queue.PriorityQueue()
        self.lock = threading.Lock()
        self.resource_available = threading.Condition(self.lock)
        self.active = True
        self.starvation_threshold = starvation_threshold
        self.aging_factor = aging_factor
        self.wait_times: Dict[int, List[float]] = {}
        self.pending_requests: Dict[int, ResourceRequest] = {}
        

        self.aging_thread = threading.Thread(target=self._apply_aging)
        self.aging_thread.daemon = True
        self.aging_thread.start()
    
    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time"""
        request_time = time.time()
        
        with self.lock:

            request = ResourceRequest(
                effective_priority=priority,
                original_priority=priority,
                request_time=request_time,
                thread_id=thread_id
            )
            

            self.pending_requests[thread_id] = request
            

            self.request_queue.put(request)
            
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource at {request_time:.2f}")
            

            while self.active:

                if not self.resource.in_use and not self.request_queue.empty():
                    try:

                        top_request = self._peek_queue()
                        if top_request and top_request.thread_id == thread_id:

                            self.request_queue.get()

                            if thread_id in self.pending_requests:
                                del self.pending_requests[thread_id]
                            break
                    except:
                        pass
                

                self.resource_available.wait(timeout=1.0)
        

        wait_time = time.time() - request_time
        

        if thread_id not in self.wait_times:
            self.wait_times[thread_id] = []
        self.wait_times[thread_id].append(wait_time)
        
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")
        

        self.resource.use_resource(thread_id, use_duration)
        

        with self.lock:
            self.resource_available.notify_all()
        
        return wait_time
    
    def _peek_queue(self):
        """Peek at the top item in the priority queue without removing it"""
        if self.request_queue.empty():
            return None
        

        items = []
        try:
            while not self.request_queue.empty():
                items.append(self.request_queue.get())
            
            if not items:
                return None
            

            top_item = min(items, key=lambda x: x.effective_priority)
            

            for item in items:
                self.request_queue.put(item)
            
            return top_item
        except:

            for item in items:
                self.request_queue.put(item)
            return None
    
    def _apply_aging(self):
        """Apply aging to prevent starvation - boost priority of long-waiting requests"""
        while self.active:
            current_time = time.time()
            
            with self.lock:
                starving_threads = []
                

                for thread_id, request in list(self.pending_requests.items()):
                    wait_time = current_time - request.request_time
                    
                    if wait_time > self.starvation_threshold:

                        priority_boost = int((wait_time - self.starvation_threshold) * self.aging_factor)
                        new_priority = max(0, request.original_priority - priority_boost)
                        
                        if new_priority < request.effective_priority:
                            logging.warning(f"Thread {thread_id} experiencing starvation! "
                                          f"Wait time: {wait_time:.2f}s, boosting priority from "
                                          f"{request.effective_priority} to {new_priority}")
                            

                            request.effective_priority = new_priority
                            starving_threads.append(thread_id)
                

                if starving_threads:
                    self._rebuild_queue()
                    self.resource_available.notify_all()
            
            time.sleep(1.0)
    
    def _rebuild_queue(self):
        """Rebuild the priority queue with updated priorities"""
        items = []
        

        while not self.request_queue.empty():
            try:
                items.append(self.request_queue.get_nowait())
            except queue.Empty:
                break
        

        for item in items:

            if item.thread_id in self.pending_requests:
                item.effective_priority = self.pending_requests[item.thread_id].effective_priority
            self.request_queue.put(item)
    
    def stop(self):
        """Stop the resource manager"""
        self.active = False
        with self.lock:
            self.resource_available.notify_all()
        self.aging_thread.join(timeout=2.0)
    
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
    

    manager = StarvationFreeResourceManager(resource, starvation_threshold=3.0, aging_factor=1.0)
    

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
            args=(i, priority, manager, 50)
        )
        thread.daemon = True
        threads.append(thread)
    

    logging.info(f"Starting starvation-free simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
    start_time = time.time()
    
    for thread in threads:
        thread.start()
    

    try:
        time.sleep(runtime_seconds)
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")
    

    manager.stop()
    

    for thread in threads:
        thread.join(timeout=1.0)
    

    stats = manager.get_statistics()
    logging.info("\n----- Starvation-Free Simulation Results -----")
    
    if stats:
        for thread_id, thread_stats in stats.items():
            priority_level = "High" if priorities[thread_id] == 1 else "Medium" if priorities[thread_id] == 3 else "Low"
            logging.info(f"Thread {thread_id} (Priority: {priority_level}):")
            logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
            logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
            logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
            logging.info(f"  Resource accesses: {thread_stats['total_waits']}")
    else:
        logging.info("No statistics available - simulation may have ended too quickly")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)