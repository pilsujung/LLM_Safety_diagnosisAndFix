import threading
import time
import random

class FairLock:
    """
    Simple fair (FIFO) lock using a ticket algorithm.
    This is conceptually similar to Java's ReentrantLock(true).
    """
    def __init__(self):
        self._internal_lock = threading.Lock()
        self._queue_lock = threading.Lock()
        self._condition = threading.Condition(self._queue_lock)
        self._next_ticket = 0
        self._serving = 0

    def acquire(self):
        with self._queue_lock:
            my_ticket = self._next_ticket
            self._next_ticket += 1

            while my_ticket != self._serving:
                self._condition.wait()

        self._internal_lock.acquire()

    def release(self):

        self._internal_lock.release()
        with self._queue_lock:
            self._serving += 1
            self._condition.notify_all()


    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def resource_consumer(thread_id, access_frequency, resource_usage_duration, priority_level):
    """
    Simulates a thread that repeatedly accesses a shared resource.
    """
    total_wait_time = 0
    successful_accesses = 0
    
    print(f"Thread-{thread_id} ({priority_level} priority) started - "
          f"Access every {access_frequency}s, Uses for {resource_usage_duration}s")
    
    for iteration in range(15):

        time.sleep(access_frequency)
        

        wait_start_time = time.time()
        

        with shared_resource_lock:

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
        print(f"\n[MONITOR] {timestamp} - System running for {elapsed:.1f}s - "
              f"Active threads: {threading.active_count()}")



shared_resource_lock = FairLock()

print("=" * 60)
print("THREAD STARVATION DEMONSTRATION (FIXED VERSION)")
print("=" * 60)
print("This simulation previously showed starvation where:")
print("- High priority thread accessed frequently with long usage")
print("- Low priority thread could be starved")
print("Now we use a FAIR lock so every waiting thread eventually gets the resource.")
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
print("- With the fair (FIFO) lock, all threads get chances to access the resource")
print("- Low priority / infrequent threads no longer suffer extreme starvation")
print("- This mirrors Java's ReentrantLock(true) fairness behavior")
print("=" * 60)
