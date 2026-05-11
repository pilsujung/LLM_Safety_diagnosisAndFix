import threading
import time
import random
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)




@dataclass(order=False)
class ResourceRequest:
    priority: int
    request_time: float
    thread_id: int
    use_duration: float
    event: threading.Event = field(default_factory=threading.Event, compare=False)




class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self._lock = threading.Lock()

    def use_resource(self, thread_id: int, duration: float):
        with self._lock:
            logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
            time.sleep(duration)
            logging.info(f"Thread {thread_id} released {self.name}")




class PriorityBasedResourceManager:
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0, aging_step: float = 2.0):
        self.resource = resource
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True

        self.starvation_threshold = starvation_threshold
        self.aging_step = aging_step

        self._pending: List[ResourceRequest] = []
        self._current: Optional[ResourceRequest] = None
        self.wait_times: Dict[int, List[float]] = {}

        self.allocation_thread = threading.Thread(target=self._allocate_resources, daemon=True)
        self.allocation_thread.start()

    def _effective_priority(self, req: ResourceRequest, now: float) -> float:
        waited = now - req.request_time
        boost = int(waited // self.aging_step)
        return req.priority - boost

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        req = ResourceRequest(priority=priority,
                              request_time=time.time(),
                              thread_id=thread_id,
                              use_duration=use_duration)

        with self.lock:
            self._pending.append(req)
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")

            def _maybe_warn():
                waited = time.time() - req.request_time
                if waited > self.starvation_threshold:
                    logging.warning(f"Thread {thread_id} is experiencing starvation! Waiting for {waited:.2f} seconds")

            while self.active and not req.event.is_set():
                _maybe_warn()
                self.cv.wait(timeout=1.0)

        if not self.active:
            return 0.0

        wait_time = time.time() - req.request_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")


        self.resource.use_resource(thread_id, use_duration)


        with self.lock:
            self._current = None
            self.cv.notify_all()

        return wait_time

    def _allocate_resources(self):
        while self.active:
            with self.lock:
                if self._current is None and self._pending:
                    now = time.time()
                    best_idx = min(
                        range(len(self._pending)),
                        key=lambda i: (self._effective_priority(self._pending[i], now), self._pending[i].request_time)
                    )
                    req = self._pending.pop(best_idx)
                    self._current = req
                    logging.info(
                        f"Allocating resource to thread {req.thread_id} "
                        f"(base prio={req.priority}, eff prio={self._effective_priority(req, now)})"
                    )
                    req.event.set()
                else:
                    self.cv.wait(timeout=0.1)

    def stop(self):
        with self.lock:
            self.active = False
            for req in self._pending:
                req.event.set()
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
    for i in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        wait_time = manager.request_resource(thread_id, priority, use_duration)
        think_time = random.uniform(0.1, 1.0)
        time.sleep(think_time)

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    resource = SharedResource("Database Connection")
    manager = PriorityBasedResourceManager(resource, starvation_threshold=5.0)

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
            args=(i, priority, manager, 100),
            daemon=True
        )
        threads.append(thread)

    logging.info(f"Starting simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
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
The following code produces a starvation. You must fix it to resolve the problem.

code : 
"""