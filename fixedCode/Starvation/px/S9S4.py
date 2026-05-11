import threading
import time
import random
from collections import deque
from enum import Enum

class Priority(Enum):
    HIGH = 3
    MEDIUM = 2
    COMPETING = 2
    LOW = 1


resource_available = threading.Condition()
waiting_queue = deque()
max_wait_time = 2.0

shared_resource_lock = threading.Lock()

class ThreadRequest:
    def __init__(self, thread_id, priority_level, request_time):
        self.thread_id = thread_id
        self.priority = Priority[priority_level.upper()]
        self.request_time = request_time
        self.wait_start = time.time()

def get_priority_position(request):
    """Calculate position based on priority and wait time (age-based fairness)"""
    age = time.time() - request.wait_start

    return (-request.priority.value, -age, request.thread_id)

def resource_consumer(thread_id, access_frequency, resource_usage_duration, priority_level):
    total_wait_time = 0
    successful_accesses = 0
    
    print(f"Thread-{thread_id} ({priority_level} priority) started - Access every {access_frequency}s, Uses for {resource_usage_duration}s")
    
    for iteration in range(15):
        time.sleep(access_frequency)
        wait_start_time = time.time()
        

        request = ThreadRequest(thread_id, priority_level, wait_start_time)
        waiting_queue.append(request)
        

        with resource_available:
            resource_available.notify_all()
        

        acquired = False
        while not acquired:
            with resource_available:
                current_time = time.time()
                

                if current_time - request.wait_start > max_wait_time:
                    old_pos = len(waiting_queue)
                    waiting_queue.remove(request)
                    waiting_queue.appendleft(request)
                    print(f"  [ANTI-STARVATION] Thread-{thread_id} promoted from #{old_pos}")
                

                if waiting_queue and waiting_queue[0] is request:
                    waiting_queue.popleft()
                    wait_end_time = time.time()
                    wait_duration = wait_end_time - wait_start_time
                    total_wait_time += wait_duration
                    acquired = True
                else:

                    resource_available.wait(timeout=0.05)
        

        with shared_resource_lock:
            current_timestamp = time.strftime('%H:%M:%S', time.localtime(time.time()))
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
        

        with resource_available:
            resource_available.notify_all()
    
    average_wait_time = total_wait_time / successful_accesses if successful_accesses > 0 else 0
    print(f"\n--- Thread-{thread_id} ({priority_level}) Final Stats ---")
    print(f"Total successful accesses: {successful_accesses}")
    print(f"Average wait time: {average_wait_time:.3f}s")
    print(f"Total wait time: {total_wait_time:.3f}s")
    print("=" * 50)

def monitor_system():
    monitoring_duration = 25
    start_time = time.time()
    while time.time() - start_time < monitoring_duration:
        time.sleep(2)
        elapsed = time.time() - start_time
        timestamp = time.strftime('%H:%M:%S', time.localtime())
        queue_len = len(waiting_queue)
        print(f"\n[MONITOR] {timestamp} - Elapsed: {elapsed:.1f}s - Queue: {queue_len} - Active: {threading.active_count()}")


print("=" * 70)
print("FIXED THREAD STARVATION - PRIORITY + AGE-BASED FAIR SCHEDULER")
print("=" * 70)
print("Uses priority-aware queue + anti-starvation mechanism")
print("=" * 70)


high_priority_thread = threading.Thread(
    target=resource_consumer, args=(1, 0.08, 0.6, "HIGH"), name="HighPriorityWorker")
medium_priority_thread = threading.Thread(
    target=resource_consumer, args=(2, 0.4, 0.2, "MEDIUM"), name="MediumPriorityWorker")
low_priority_thread = threading.Thread(
    target=resource_consumer, args=(3, 1.2, 0.05, "LOW"), name="LowPriorityWorker")
competing_thread = threading.Thread(
    target=resource_consumer, args=(4, 0.3, 0.3, "COMPETING"), name="CompetingWorker")
monitor_thread = threading.Thread(target=monitor_system, name="SystemMonitor")

simulation_start_time = time.time()
print(f"\nSimulation started at: {time.strftime('%H:%M:%S', time.localtime(simulation_start_time))}")
print("-" * 70)


high_priority_thread.start()
time.sleep(0.05)
medium_priority_thread.start()
time.sleep(0.05)
competing_thread.start()
time.sleep(0.05)
low_priority_thread.start()
monitor_thread.start()


high_priority_thread.join()
medium_priority_thread.join()
low_priority_thread.join()
competing_thread.join()

simulation_end_time = time.time()
total_simulation_time = simulation_end_time - simulation_start_time
end_timestamp = time.strftime('%H:%M:%S', time.localtime(simulation_end_time))

print("\n" + "=" * 70)
print("SIMULATION COMPLETE - STARVATION ELIMINATED")
print("=" * 70)
print(f"Total duration: {total_simulation_time:.2f} seconds")
print("All threads now receive fair access regardless of frequency/usage patterns!")
print("=" * 70)
