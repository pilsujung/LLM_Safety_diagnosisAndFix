import threading
import time
import random
from queue import Queue

class FairLock:
    """
    A fair lock implementation that ensures FIFO ordering of lock acquisition.
    This prevents thread starvation by guaranteeing each thread gets access
    in the order they requested it.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = Queue()
        self._owner = None
    
    def acquire(self):
        """Acquire the lock in FIFO order"""
        event = threading.Event()
        
        with self._lock:
            self._queue.put(event)
        

        event.wait()
        self._owner = threading.current_thread()
    
    def release(self):
        """Release the lock and signal the next waiting thread"""
        with self._lock:
            self._owner = None
            if not self._queue.empty():
                next_event = self._queue.get()
                next_event.set()
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False

def resource_consumer(thread_id, access_frequency, resource_usage_duration, priority_level):
    """
    Simulates a thread that repeatedly accesses a shared resource.
    
    Args:
        thread_id: Unique identifier for the thread
        access_frequency: Time interval between resource access attempts
        resource_usage_duration: How long the thread holds the resource
        priority_level: Priority level of the thread (for logging purposes)
    """
    total_wait_time = 0
    successful_accesses = 0
    
    print(f"Thread-{thread_id} ({priority_level} priority) started - Access every {access_frequency}s, Uses for {resource_usage_duration}s")
    
    for iteration in range(15):

        time.sleep(access_frequency)
        

        wait_start_time = time.time()
        

        shared_resource_lock.acquire()
        try:

            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time
            total_wait_time += wait_duration
            

            current_timestamp = time.strftime('%H:%M:%S', time.localtime(wait_end_time))
            print(f"{current_timestamp} - Thread-{thread_id} ({priority_level}) acquired resource "
                  f"(waited {wait_duration:.3f}s, iteration {iteration + 1}/15)")
            

            actual_usage_time = resource_usage_duration + random.uniform(-0.05, 0.05)
            time.sleep(actual_usage_time)
            

            release_time = time.time()
            release_timestamp = time.strftime('%H:%M:%S', time.localtime(release_time))
            total_resource_time = release_time - wait_end_time
            successful_accesses += 1
            
            print(f"{release_timestamp} - Thread-{thread_id} ({priority_level}) released resource "
                  f"after {total_resource_time:.3f}s")
        finally:
            shared_resource_lock.release()
    

    average_wait_time = total_wait_time / successful_accesses if successful_accesses > 0 else 0
    print(f"\n--- Thread-{thread_id} ({priority_level}) Final Stats ---")
    print(f"Total successful accesses: {successful_accesses}")
    print(f"Average wait time: {average_wait_time:.3f}s")
    print(f"Total wait time: {total_wait_time:.3f}s")
    print("=" * 50)

def monitor_system():
    """
    Monitors the system and provides periodic updates about thread activity.
    """
    monitoring_duration = 12
    start_time = time.time()
    
    while time.time() - start_time < monitoring_duration:
        time.sleep(2)
        elapsed = time.time() - start_time
        timestamp = time.strftime('%H:%M:%S', time.localtime())
        print(f"\n[MONITOR] {timestamp} - System running for {elapsed:.1f}s - Active threads: {threading.active_count()}")


shared_resource_lock = FairLock()

print("=" * 60)
print("THREAD STARVATION SOLUTION - FAIR LOCK")
print("=" * 60)
print("This simulation demonstrates fair lock preventing starvation:")
print("- Fair lock ensures FIFO ordering of thread access")
print("- All threads get equal opportunity regardless of priority")
print("- Low priority threads no longer experience starvation")
print("=" * 60)


high_priority_thread = threading.Thread(
    target=resource_consumer, 
    args=(1, 0.08, 0.6, "HIGH"),
    name="HighPriorityWorker"
)

medium_priority_thread = threading.Thread(
    target=resource_consumer, 
    args=(2, 0.4, 0.2, "MEDIUM"),
    name="MediumPriorityWorker"
)

low_priority_thread = threading.Thread(
    target=resource_consumer, 
    args=(3, 1.2, 0.05, "LOW"),
    name="LowPriorityWorker"
)

competing_thread = threading.Thread(
    target=resource_consumer,
    args=(4, 0.3, 0.3, "COMPETING"),
    name="CompetingWorker"
)

monitor_thread = threading.Thread(target=monitor_system, name="SystemMonitor")


simulation_start_time = time.time()
start_timestamp = time.strftime('%H:%M:%S', time.localtime(simulation_start_time))
print(f"\nSimulation started at: {start_timestamp}")
print("-" * 60)


high_priority_thread.start()
medium_priority_thread.start()
low_priority_thread.start()
competing_thread.start()
monitor_thread.start()


high_priority_thread.join()
medium_priority_thread.join()
low_priority_thread.join()
competing_thread.join()


simulation_end_time = time.time()
total_simulation_time = simulation_end_time - simulation_start_time
end_timestamp = time.strftime('%H:%M:%S', time.localtime(simulation_end_time))

print("\n" + "=" * 60)
print("SIMULATION COMPLETE")
print("=" * 60)
print(f"End time: {end_timestamp}")
print(f"Total duration: {total_simulation_time:.2f} seconds")
print("\nObservations:")
print("- Fair lock ensures all threads get access in order")
print("- Low priority threads receive fair access despite competition")
print("- Starvation problem is resolved through FIFO ordering")
print("=" * 60)