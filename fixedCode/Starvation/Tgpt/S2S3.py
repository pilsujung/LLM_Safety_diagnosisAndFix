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
    promoted: bool = field(default=False, compare=False)

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.in_use = False
        self.lock = threading.Lock()

    def use_resource(self, thread_id: int, duration: float):
        with self.lock:
            self.in_use = True
        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        time.sleep(duration)
        with self.lock:
            self.in_use = False
        logging.info(f"Thread {thread_id} released {self.name}")

class PriorityBasedResourceManager:
    """
    Fair allocator with:
      - strict handoff via per-request Event (no thundering-herd wakeups)
      - starvation protection: any waiter past starvation_threshold is served FIFO among starving
      - otherwise: lowest priority first, tie-broken by oldest request_time
    """
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0):
        self.resource = resource
        self.starvation_threshold = starvation_threshold

        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)


        self._pending: List[ResourceRequest] = []
        self._active = True


        self.wait_times: Dict[int, List[float]] = {}


        self._allocator = threading.Thread(target=self._allocate_resources, daemon=True)
        self._allocator.start()

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time."""
        request_time = time.time()
        req = ResourceRequest(priority=priority,
                              request_time=request_time,
                              thread_id=thread_id,
                              use_duration=use_duration)

        with self.lock:
            self._pending.append(req)
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")

            self.cv.notify()



        while self._active:

            waited = time.time() - request_time
            if waited > self.starvation_threshold and not req.promoted:
                logging.warning(f"Thread {thread_id} is experiencing starvation risk: {waited:.2f}s")

                req.promoted = True


            if req.event.wait(timeout=1.0):
                break

        if not self._active:
            return 0.0


        wait_time = time.time() - request_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted access after {wait_time:.2f} seconds")


        self.resource.use_resource(thread_id, use_duration)


        with self.lock:

            self.cv.notify()

        return wait_time

    def _pick_next_request_locked(self) -> Optional[ResourceRequest]:
        """
        Choose the next request to serve.
        Priority rule:
          1) Any request whose wait > starvation_threshold -> 'starving pool';
             pick the OLDEST (smallest request_time) among starving.
          2) Otherwise, pick by (lowest priority, oldest request_time).
        Requires self.lock to be held.
        """
        if not self._pending:
            return None

        now = time.time()
        starving: List[ResourceRequest] = []
        for r in self._pending:
            if (now - r.request_time) > self.starvation_threshold:
                starving.append(r)

        if starving:

            chosen = min(starving, key=lambda r: r.request_time)
        else:

            chosen = min(self._pending, key=lambda r: (r.priority, r.request_time))


        self._pending.remove(chosen)
        return chosen

    def _allocate_resources(self):
        """
        Dedicated allocator:
          - waits until resource is free and there is a pending request
          - picks next request fairly (with anti-starvation)
          - hands off by setting the chosen request's Event
        """
        while True:
            with self.lock:
                if not self._active:
                    return


                while self._active and (self.resource.in_use or not self._pending):
                    self.cv.wait(timeout=0.5)

                if not self._active:
                    return


                req = self._pick_next_request_locked()
                if req is None:
                    continue





                logging.info(
                    f"Allocating resource to thread {req.thread_id} "
                    f"(priority {req.priority}, waited {time.time() - req.request_time:.2f}s)"
                )


                req.event.set()


            time.sleep(0.01)

    def stop(self):
        """Stop the resource manager cleanly."""
        with self.lock:
            self._active = False

            self.cv.notify_all()

        for r in list(self._pending):
            r.event.set()
        self._allocator.join(timeout=1.0)

    def get_statistics(self) -> Dict:
        """Get statistics about wait times."""
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
