import threading
import time
import random
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
    priority: int
    request_time: float
    thread_id: int
    use_duration: float

    warned_starvation: bool = False

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.lock = threading.Lock()
        self.in_use = False

    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        with self.lock:
            self.in_use = True
        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        time.sleep(duration)
        with self.lock:
            self.in_use = False
        logging.info(f"Thread {thread_id} released {self.name}")

class PriorityBasedResourceManager:
    def __init__(
        self,
        resource: SharedResource,
        starvation_threshold: float = 10.0,
        aging_interval: float = 2.0,
    ):
        self.resource = resource
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True


        self.requests: List[ResourceRequest] = []


        self.resource_in_use = False


        self.starvation_threshold = starvation_threshold
        self.aging_interval = aging_interval


        self.wait_times: Dict[int, List[float]] = {}

    def _effective_priority(self, req: ResourceRequest, now: float) -> int:
        waited = now - req.request_time
        aging_steps = int(waited // self.aging_interval)
        return max(0, req.priority - aging_steps)

    def _pick_head_index(self) -> int:
        """Choose the index of the next request to serve (min by aged priority, then FIFO by time)."""
        if not self.requests:
            return -1
        now = time.time()
        best_idx = 0
        best_key = (self._effective_priority(self.requests[0], now), self.requests[0].request_time)
        for i in range(1, len(self.requests)):
            key = (self._effective_priority(self.requests[i], now), self.requests[i].request_time)
            if key < best_key:
                best_key = key
                best_idx = i
        return best_idx

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        request_time = time.time()
        req = ResourceRequest(priority, request_time, thread_id, use_duration)

        with self.cv:

            self.requests.append(req)
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")


            while self.active:
                head_idx = self._pick_head_index()
                i_am_head = (head_idx != -1 and self.requests[head_idx].thread_id == thread_id)
                if i_am_head and not self.resource_in_use:

                    self.resource_in_use = True

                    self.requests.pop(head_idx)
                    break


                waited = time.time() - request_time
                if (waited > self.starvation_threshold) and not req.warned_starvation:
                    req.warned_starvation = True
                    logging.warning(
                        f"Thread {thread_id} is experiencing long wait ({waited:.2f}s). Aging will promote it."
                    )


                self.cv.wait(timeout=0.5)


        wait_time = time.time() - request_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")


        try:
            self.resource.use_resource(thread_id, use_duration)
        finally:
            with self.cv:
                self.resource_in_use = False

                self.cv.notify_all()

        return wait_time

    def stop(self):
        with self.cv:
            self.active = False
            self.cv.notify_all()

    def get_statistics(self) -> Dict:
        stats = {}
        for thread_id, times in self.wait_times.items():
            stats[thread_id] = {
                "min_wait": min(times),
                "max_wait": max(times),
                "avg_wait": sum(times) / len(times),
                "total_waits": len(times),
            }
        return stats

def worker_thread(thread_id: int, priority: int, manager: PriorityBasedResourceManager, iterations: int):
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        manager.request_resource(thread_id, priority, use_duration)
        time.sleep(random.uniform(0.1, 1.0))

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    resource = SharedResource("Database Connection")
    manager = PriorityBasedResourceManager(resource, starvation_threshold=5.0, aging_interval=1.5)

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
        t = threading.Thread(target=worker_thread, args=(i, priority, manager, 100))
        t.daemon = True
        threads.append(t)

    logging.info(f"Starting simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
    start_time = time.time()

    for t in threads:
        t.start()

    try:
        while time.time() - start_time < runtime_seconds:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")

    manager.stop()
    for t in threads:
        t.join(timeout=0.5)

    stats = manager.get_statistics()
    logging.info("\n----- Simulation Results -----")
    for thread_id, thread_stats in stats.items():
        priority_level = "High" if priorities[thread_id] == 1 else "Medium" if priorities[thread_id] == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {priority_level}):")
        logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {thread_stats['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)



"""
The following code produces a starvation. Can you fix it to resolve the problem?
you must fix step by step.

code : 
"""