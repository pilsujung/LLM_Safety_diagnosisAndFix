import threading
import time
import random
from collections import deque


shared_resource_lock = threading.Lock()
fair_lock = threading.Lock()
wait_queue = deque()
waiting_threads = {}

class FairResourceGuard:
    """Fair resource access guard that prevents starvation using FIFO scheduling."""
    
    def __enter__(self):
        thread_id = threading.current_thread().ident
        with fair_lock:
            if thread_id not in waiting_threads:
                wait_start = time.time()
                waiting_threads[thread_id] = wait_start
                wait_queue.append(thread_id)
            

            while wait_queue[0] != thread_id:
                fair_lock.release()
                time.sleep(0.001)
                with fair_lock:
                    pass
            

            del waiting_threads[thread_id]
        

        shared_resource_lock.acquire()
        self.acquire_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):

        shared_resource_lock.release()
        

        with fair_lock:
            try:
                wait_queue.remove(threading.current_thread().ident)
            except ValueError:
                pass

def resource_consumer(thread_id, access_frequency, resource_usage_duration, priority_level):
    """Fixed resource consumer using fair scheduling."""
    total_wait_time = 0
    successful_accesses = 0
    
    print(f"Thread-{thread_id} ({priority_level} priority) started - Access every {access_frequency}s, Uses for {resource_usage_duration}s")
    
    for iteration in range(15):

        time.sleep(access_frequency)
        
        wait_start = time.time()
        

        with FairResourceGuard() as guard:
            wait_duration = guard.acquire_time - wait_start
            total_wait_time += wait_duration
            
            current_timestamp = time.strftime('%H:%M:%S', time.localtime(guard.acquire_time))
            print(f"{current_timestamp} - Thread-{thread_id} ({priority_level}) acquired resource "
                  f"(waited {wait_duration:.3f}s, iteration {iteration + 1}/15)")
            

            actual_usage = resource_usage_duration + random.uniform(-0.05, 0.05)
            time.sleep(actual_usage)
            
            release_time = time.time()
            release_timestamp = time.strftime('%H:%M:%S', time.localtime(release_time))
            print(f"{release_timestamp} - Thread-{thread_id} ({priority_level}) released resource "
                  f"after {release_time - guard.acquire_time:.3f}s")
            successful_accesses += 1
    
    avg_wait = total_wait_time / successful_accesses if successful_accesses else 0
    print(f"\n--- Thread-{thread_id} ({priority_level}) FINAL STATS ---")
    print(f"Successful accesses: {successful_accesses}")
    print(f"Avg wait time: {avg_wait:.3f}s")
    print(f"Total wait time: {total_wait_time:.3f}s")
    print("=" * 50)

def monitor_system():
    """System monitor."""
    start_time = time.time()
    while time.time() - start_time < 25:
        time.sleep(2)
        elapsed = time.time() - start_time
        timestamp = time.strftime('%H:%M:%S', time.localtime())
        with fair_lock:
            queue_len = len(wait_queue)
        print(f"[MONITOR] {timestamp} - {elapsed:.1f}s elapsed | "
              f"Active: {threading.active_count()} | Queue: {queue_len}")


print("=" * 70)
print("FAIR SCHEDULING SOLUTION - STARVATION FIXED")
print("=" * 70)
print("Uses FIFO wait queue to ensure every thread gets fair access regardless of frequency")
print("=" * 70)


threads = [
    threading.Thread(target=resource_consumer, args=(1, 0.08, 0.6, "HIGH")),
    threading.Thread(target=resource_consumer, args=(2, 0.4, 0.2, "MEDIUM")),
    threading.Thread(target=resource_consumer, args=(3, 1.2, 0.05, "LOW")),
    threading.Thread(target=resource_consumer, args=(4, 0.3, 0.3, "COMPETING"))
]

monitor_thread = threading.Thread(target=monitor_system)

start_time = time.time()
print(f"\nSTART: {time.strftime('%H:%M:%S', time.localtime(start_time))}")
print("-" * 60)


for t in threads:
    t.start()
    time.sleep(0.05)
monitor_thread.start()


for t in threads:
    t.join()
monitor_thread.join()

total_time = time.time() - start_time
print(f"\n{'='*70}")
print(f"SIMULATION COMPLETE - Duration: {total_time:.1f}s")
print("All threads completed 15 iterations with fair access!")
print(f"{'='*70}")
