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

@dataclass(order=True)
class ResourceRequest:
    effective_priority: float = field(init=False, compare=True)
    base_priority: int
    request_time: float
    thread_id: int = field(compare=False)
    use_duration: float = field(compare=False)
    
    def __post_init__(self):

        self.effective_priority = self.base_priority

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

class PriorityBasedResourceManager:
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0, aging_factor: float = 0.5):
        self.resource = resource
        self.requests: List[ResourceRequest] = []
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True
        self.starvation_threshold = starvation_threshold
        self.aging_factor = aging_factor
        self.wait_times: Dict[int, List[float]] = {}


        self.allocation_thread = threading.Thread(target=self._allocate_resources, daemon=True)
        self.allocation_thread.start()

    def _update_effective_priorities(self):
        current_time = time.time()
        for req in self.requests:
            wait_time = current_time - req.request_time


            req.effective_priority = req.base_priority - self.aging_factor * wait_time

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        request_time = time.time()
        request = ResourceRequest(priority, request_time, thread_id, use_duration)

        with self.lock:
            self.requests.append(request)
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")

            while self.active:
                self._update_effective_priorities()

                self.requests.sort()
                

                if self.requests[0].thread_id == thread_id and not self.resource.in_use:
                    break


                current_wait = time.time() - request_time
                if current_wait > self.starvation_threshold:
                    logging.warning(f"Thread {thread_id} is experiencing starvation! Waiting for {current_wait:.2f} seconds")

                self.cv.wait(timeout=1.0)

            wait_time = time.time() - request_time


            if thread_id not in self.wait_times:
                self.wait_times[thread_id] = []
            self.wait_times[thread_id].append(wait_time)


            self.requests = [r for r in self.requests if r.thread_id != thread_id]


        self.resource.use_resource(thread_id, use_duration)

        with self.lock:
            self.cv.notify_all()

        return wait_time

    def _allocate_resources(self):
        while self.active:
            with self.lock:
                self._update_effective_priorities()
                if self.requests and not self.resource.in_use:

                    self.requests.sort()
                    top_request = self.requests[0]

                    logging.info(f"Allocating resource to thread {top_request.thread_id} (priority {top_request.base_priority}, effective {top_request.effective_priority:.2f})")


                    self.cv.notify_all()

            time.sleep(0.1)

    def stop(self):
        with self.lock:
            self.active = False
            self.cv.notify_all()
        self.allocation_thread.join(timeout=1.0)

    def get_statistics(self) -> Dict:
        stats = {}
        for thread_id, times in self.wait_times.items():
            stats[thread_id] = {
                "min_wait": min(times),
                "max_wait": max(times),
                "avg_wait": sum(times) / len(times),
                "total_waits": len(times)
            }
        return stats

def worker_thread(thread_id: int, priority: int, manager: PriorityBasedResourceManager, iterations: int):
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        wait_time = manager.request_resource(thread_id, priority, use_duration)
        think_time = random.uniform(0.1, 1.0)
        time.sleep(think_time)

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    resource = SharedResource("Database Connection")
    manager = PriorityBasedResourceManager(resource, starvation_threshold=5.0, aging_factor=0.5)

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
        priority_name = "High" if priorities[thread_id] == 1 else "Medium" if priorities[thread_id] == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {priority_name}):")
        logging.info(f" Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f" Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f" Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f" Resource accesses: {thread_stats['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)
