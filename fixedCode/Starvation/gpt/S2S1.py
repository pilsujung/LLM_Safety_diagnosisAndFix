import threading
import time
import random
import queue
import logging
import heapq
from dataclasses import dataclass, field
from typing import List, Dict


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass(order=True)
class ResourceRequest:
    priority: int
    request_time: float
    thread_id: int = field(compare=False)
    base_priority: int = field(default=0, compare=False)
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

class PriorityBasedResourceManager:
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0):
        self.resource = resource
        self.request_queue: "queue.PriorityQueue[ResourceRequest]" = queue.PriorityQueue()
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True
        self.starvation_threshold = starvation_threshold
        self.wait_times: Dict[int, List[float]] = {}

    def _apply_aging_and_heapify(self):
        """
        Aging rule: every starvation_threshold seconds waited,
        promote priority by 1 (i.e., decrement numeric priority).
        """
        now = time.time()
        pq_list: List[ResourceRequest] = list(self.request_queue.queue)
        updated = False

        for req in pq_list:
            waited = now - req.request_time

            effective_boost = int(waited // self.starvation_threshold)
            new_priority = max(0, req.base_priority - effective_boost)
            if new_priority != req.priority:
                req.priority = new_priority
                updated = True

        if updated:
            heapq.heapify(pq_list)

            self.request_queue.queue.clear()
            for item in pq_list:
                self.request_queue.queue.append(item)

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time"""
        request_time = time.time()
        request = ResourceRequest(priority, request_time, thread_id, base_priority=priority)

        with self.cv:

            self.request_queue.put(request)
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")

            while self.active:

                self._apply_aging_and_heapify()


                if not self.resource.in_use and not self.request_queue.empty():
                    top: ResourceRequest = self.request_queue.queue[0]
                    if top.thread_id == thread_id:

                        self.request_queue.get_nowait()
                        self.resource.in_use = True
                        break


                current_wait = time.time() - request_time
                if current_wait > self.starvation_threshold:
                    logging.warning(
                        f"Thread {thread_id} is experiencing long wait ({current_wait:.2f}s); aging will boost priority."
                    )


                self.cv.wait(timeout=0.3)


        wait_time = time.time() - request_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")


        try:
            self.resource.use_resource(thread_id, use_duration)
        finally:
            with self.cv:
                self.resource.in_use = False

                self.cv.notify_all()

        return wait_time

    def stop(self):
        """Stop the resource manager"""
        with self.cv:
            self.active = False
            self.cv.notify_all()

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

def worker_thread(thread_id: int, priority: int, manager: PriorityBasedResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource"""
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        manager.request_resource(thread_id, priority, use_duration)
        time.sleep(random.uniform(0.1, 1.0))

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    resource = SharedResource("Database Connection")
    manager = PriorityBasedResourceManager(resource, starvation_threshold=5.0)

    threads = []
    priorities = {}
    for i in range(num_threads):
        if i < 2:
            prio = 1
        elif i < 4:
            prio = 3
        else:
            prio = 5
        priorities[i] = prio
        t = threading.Thread(target=worker_thread, args=(i, prio, manager, 100), daemon=True)
        threads.append(t)

    logging.info(f"Starting simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
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
    for tid, s in stats.items():
        level = "High" if priorities[tid] == 1 else "Medium" if priorities[tid] == 3 else "Low"
        logging.info(f"Thread {tid} (Priority: {level}):")
        logging.info(f"  Min wait: {s['min_wait']:.2f}s")
        logging.info(f"  Max wait: {s['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {s['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {s['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)


"""
The following code produces a starvation. Can you fix it to resolve the problem?
code :
"""