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

@dataclass
class ResourceRequest:
    thread_id: int
    priority: int
    request_time: float
    use_duration: float
    event: threading.Event
    seq: int

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self._in_use = False
        self._lock = threading.Lock()

    @property
    def in_use(self) -> bool:
        with self._lock:
            return self._in_use

    def _set_in_use(self, val: bool):
        with self._lock:
            self._in_use = val

    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        self._set_in_use(True)
        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        try:
            time.sleep(duration)
        finally:
            self._set_in_use(False)
            logging.info(f"Thread {thread_id} released {self.name}")

class PriorityBasedResourceManager:
    """
    Fair allocator with priority aging to prevent starvation.

    - Effective priority = base_priority - floor(wait_time / starvation_threshold)
      (lower is better). This means every 'starvation_threshold' seconds of wait
      boosts a request one priority level.
    - Among equal effective priorities, earlier requests (smaller seq) win (FIFO).
    """
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0):
        self.resource = resource
        self.starvation_threshold = max(0.1, starvation_threshold)

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: List[ResourceRequest] = []
        self._seq_counter = 0
        self._active = True

        self.wait_times: Dict[int, List[float]] = {}

        self._allocator = threading.Thread(target=self._allocate_resources, daemon=True)
        self._allocator.start()

    def _next_seq(self) -> int:
        self._seq_counter += 1
        return self._seq_counter

    def _effective_key(self, req: ResourceRequest) -> tuple:
        now = time.time()
        waited = max(0.0, now - req.request_time)

        boost = int(waited // self.starvation_threshold)
        effective_priority = req.priority - boost

        return (effective_priority, req.seq)

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        request_time = time.time()
        event = threading.Event()
        req = ResourceRequest(
            thread_id=thread_id,
            priority=priority,
            request_time=request_time,
            use_duration=use_duration,
            event=event,
            seq=self._next_seq()
        )

        with self._lock:
            self._pending.append(req)
            self._cv.notify()


        granted = event.wait(timeout=None)
        if not granted:

            logging.warning(f"Thread {thread_id} wait aborted")
            return 0.0


        wait_time = time.time() - request_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted after {wait_time:.2f} seconds")


        self.resource.use_resource(thread_id, use_duration)


        with self._lock:
            self._cv.notify()

        return wait_time

    def _allocate_resources(self):
        while True:
            with self._lock:
                if not self._active:

                    for req in self._pending:
                        req.event.set()
                    self._pending.clear()
                    return


                while self._active and (not self._pending or self.resource.in_use):
                    self._cv.wait(timeout=0.5)

                if not self._active:
                    continue
                if not self._pending or self.resource.in_use:
                    continue


                best_idx = min(range(len(self._pending)),
                               key=lambda i: self._effective_key(self._pending[i]))
                best_req = self._pending.pop(best_idx)

                waited = time.time() - best_req.request_time
                if waited > self.starvation_threshold:
                    logging.info(
                        f"Starvation protection: boosting thread {best_req.thread_id} "
                        f"(waited {waited:.2f}s)"
                    )


                best_req.event.set()


            time.sleep(0.001)

    def stop(self):
        with self._lock:
            self._active = False
            self._cv.notify_all()
        self._allocator.join(timeout=1.0)

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
        manager.request_resource(thread_id, priority, use_duration)
        time.sleep(random.uniform(0.1, 1.0))

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
        t = threading.Thread(target=worker_thread, args=(i, priority, manager, 100), daemon=True)
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
