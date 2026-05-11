import threading 
import time 
import random 
from collections import deque 
 
class SharedResource: 
    def __init__(self, name, use_duration_range=(0.02, 0.08), contention_threshold=0.01): 
        self.name = name 
        self.lock = threading.Lock() 
        self.use_duration_range = use_duration_range 
        self.contention_threshold = contention_threshold 
        self._wait_times = deque() 
        self._contention_events = 0 
        self._stats_lock = threading.Lock() 
 
    def use_resource(self, thread_id, verbose=False): 
        if verbose: 
            print(f"[{time.perf_counter():.6f}] T{thread_id}: try acquire {self.name}") 
 
        start_wait = time.perf_counter() 
        self.lock.acquire()
        try:
            wait_time = time.perf_counter() - start_wait 
    

            with self._stats_lock: 
                self._wait_times.append(wait_time) 
                if wait_time > self.contention_threshold: 
                    self._contention_events += 1 
    
            if verbose: 
                print(f"[{time.perf_counter():.6f}] T{thread_id}: acquired (wait {wait_time:.4f}s)") 
    
            use_duration = random.uniform(*self.use_duration_range) 
            time.sleep(use_duration) 
    
            if verbose: 
                print(f"[{time.perf_counter():.6f}] T{thread_id}: released after {use_duration:.4f}s") 
    
            return wait_time, use_duration
        finally:

            self.lock.release()
 
    @property 
    def wait_times(self): 
        with self._stats_lock: 
            return list(self._wait_times) 
 
    @property 
    def contention_events(self): 
        with self._stats_lock: 
            return self._contention_events 
 
 
def worker(resource: SharedResource, thread_id: int, access_count=3, delay_range=(0.005, 0.03), verbose=False): 
    total_wait = 0.0 
    total_use = 0.0 
    for _ in range(access_count): 
        time.sleep(random.uniform(*delay_range)) 
        w, u = resource.use_resource(thread_id, verbose=verbose) 
        total_wait += w 
        total_use += u 
 
    if verbose: 
        print(f"[{time.perf_counter():.6f}] T{thread_id}: done (wait {total_wait:.4f}s, use {total_use:.4f}s)") 
 
 
def simulate_resource_contention( 
    num_threads=8, 
    resource_use_range=(0.02, 0.08), 
    access_count=3, 
    contention_threshold=0.01, 
    verbose=False 
): 
    print(f"=== SHARED RESOURCE CONTENTION (fast) ===") 
    print(f"threads={num_threads}, accesses/thread={access_count}, use_range={resource_use_range}s") 
    start = time.perf_counter() 
 
    printer = SharedResource("Printer", resource_use_range, contention_threshold) 
 
    threads = [] 
    for i in range(num_threads): 
        t = threading.Thread(target=worker, args=(printer, i, access_count, (0.005, 0.03), verbose)) 
        t.start() 
        threads.append(t) 
 
    for t in threads: 
        t.join() 
 
    elapsed = time.perf_counter() - start 
 
    waits = printer.wait_times 
    count = num_threads * access_count 
    avg_wait = (sum(waits) / len(waits)) if waits else 0.0 
    max_wait = max(waits) if waits else 0.0 
    contention = printer.contention_events 
    contention_pct = (contention / count) * 100 if count else 0.0 
 
    print("\n--- RESULTS ---") 
    print(f"Total simulation time : {elapsed:.3f}s") 
    print(f"Total accesses        : {count}") 
    print(f"Avg wait time         : {avg_wait:.4f}s") 
    print(f"Max wait time         : {max_wait:.4f}s") 
    print(f"Contention events     : {contention} (>{contention_threshold:.3f}s)") 
    print(f"Contention percentage : {contention_pct:.1f}%") 
 
if __name__ == "__main__": 
    simulate_resource_contention( 
        num_threads=8, 
        resource_use_range=(0.02, 0.08),  
        access_count=3, 
        contention_threshold=0.01,         
        verbose=False                      
    )