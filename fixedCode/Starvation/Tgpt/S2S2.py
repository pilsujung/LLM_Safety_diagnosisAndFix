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

@dataclass
class ResourceRequest:
    thread_id: int
    priority: int
    request_time: float
    use_duration: float

    grant_event: threading.Event = field(default_factory=threading.Event)

    grant_time: float = 0.0

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
    """
    Fair (FIFO) resource manager to prevent starvation.
    New requests are queued in arrival order and granted one-by-one.
    """
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0):
        self.resource = resource
        self.request_queue: "queue.Queue[ResourceRequest]" = queue.Queue()
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True
        self.starvation_threshold = starvation_threshold
        self.wait_times: Dict[int, List[float]] = {}


        self.allocation_thread = threading.Thread(target=self._allocate_resources, name="allocator", daemon=True)
        self.allocation_thread.start()
    
    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """
        Submit a request and block until the allocator grants access.
        Returns the wait time until grant.
        """
        req = ResourceRequest(
            thread_id=thread_id,
            priority=priority,
            request_time=time.time(),
            use_duration=use_duration,
        )


        self.request_queue.put(req)
        logging.info(f"Thread {thread_id} (priority {priority}) requested resource")


        while not req.grant_event.wait(timeout=1.0):
            current_wait = time.time() - req.request_time
            if current_wait > self.starvation_threshold:
                logging.warning(
                    f"Thread {thread_id} is experiencing long wait ({current_wait:.2f}s). "
                    f"(FIFO fairness ensures eventual progress.)"
                )
            if not self.active:
                return 0.0


        wait_time = req.grant_time - req.request_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted after waiting {wait_time:.2f} seconds")


        self.resource.use_resource(thread_id, use_duration)


        with self.cv:
            self.cv.notify_all()
        return wait_time
    
    def _allocate_resources(self):
        """
        Allocation thread: give the resource to the next requester in FIFO order.
        It *grants* by setting that request's event, then waits until the resource
        is released before granting the next.
        """
        while self.active:
            try:
                req: ResourceRequest = self.request_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            with self.cv:

                while self.resource.in_use and self.active:
                    self.cv.wait(timeout=0.1)
                if not self.active:
                    break


                logging.info(
                    f"Allocating resource to thread {req.thread_id} (priority {req.priority}) [FIFO grant]"
                )
                req.grant_time = time.time()
                req.grant_event.set()



                while self.resource.in_use and self.active:
                    self.cv.wait(timeout=0.1)


            self.request_queue.task_done()

    def stop(self):
        """Stop the resource manager"""
        self.active = False
        with self.cv:
            self.cv.notify_all()
        self.allocation_thread.join(timeout=1.0)
    
    def get_statistics(self) -> Dict:
        """Get statistics about wait times"""
        stats = {}
        for thread_id, times in self.wait_times.items():
            stats[thread_id] = {
                "min_wait": min(times),
                "max_wait": max(times),
                "avg_wait": sum(times) / len(times),
                "total_waits": len(times)
            }
        return stats

def worker_thread(thread_id: int, priority: int, manager: FairResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource"""
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        manager.request_resource(thread_id, priority, use_duration)
        time.sleep(random.uniform(0.1, 1.0))

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    """Run the starvation-free simulation"""
    resource = SharedResource("Database Connection")
    manager = FairResourceManager(resource, starvation_threshold=5.0)

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
        t = threading.Thread(target=worker_thread, args=(i, priority, manager, 100), daemon=True)
        threads.append(t)

    logging.info(f"Starting simulation with {num_threads} threads")
    logging.info(f"Thread priorities (ignored for fairness): {priorities}")

    start = time.time()
    for t in threads:
        t.start()

    try:
        while time.time() - start < runtime_seconds:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")

    manager.stop()
    for t in threads:
        t.join(timeout=0.5)

    stats = manager.get_statistics()
    logging.info("\n----- Simulation Results -----")
    for thread_id, s in stats.items():
        p = priorities[thread_id]
        level = "High" if p == 1 else "Medium" if p == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {level}):")
        logging.info(f"  Min wait: {s['min_wait']:.2f}s")
        logging.info(f"  Max wait: {s['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {s['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {s['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)
