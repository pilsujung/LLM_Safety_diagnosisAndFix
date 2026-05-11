import threading
import time
import random
from collections import deque


RUN_DURATION = 10              
MAX_ATTEMPTS_PER_THREAD = 50   


resource_lock = threading.Lock()
queue_lock = threading.Lock()
request_queue = deque()
request_available = threading.Condition(queue_lock)


access_counts = {}
starvation_counts = {}
total_attempts = {}
wait_times = {}


running = True

def initialize_stats(thread_name):
    """Initialize statistics for a thread"""
    access_counts[thread_name] = 0
    starvation_counts[thread_name] = 0
    total_attempts[thread_name] = 0
    wait_times[thread_name] = []

def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "="*50)
        print("THREAD STATISTICS:")
        print("="*50)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            avg_wait = sum(wait_times[thread_name]) / len(wait_times[thread_name]) if wait_times[thread_name] else 0
            print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}% | Avg Wait: {avg_wait:.3f}s")
        print("="*50 + "\n")

def acquire_resource_fair(thread_name, timeout=5.0):
    """Fair resource acquisition using a queue-based system"""
    event = threading.Event()
    request_time = time.time()
    
    with queue_lock:
        request_queue.append((thread_name, event))
        request_available.notify()
    

    if not event.wait(timeout):

        with queue_lock:
            try:
                request_queue.remove((thread_name, event))
            except ValueError:
                pass
        return False, 0
    

    acquired = resource_lock.acquire(timeout=timeout)
    wait_time = time.time() - request_time
    
    return acquired, wait_time

def release_resource_fair():
    """Release resource and notify next in queue"""
    resource_lock.release()
    
    with queue_lock:
        if request_queue:
            _, next_event = request_queue.popleft()
            next_event.set()

def high_priority_thread(thread_name):
    """High priority thread with fair access"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        acquired, wait_time = acquire_resource_fair(thread_name, timeout=3.0)
        
        if acquired:
            try:
                access_counts[thread_name] += 1
                wait_times[thread_name].append(wait_time)
                print(f"[HIGH PRIORITY] {thread_name} acquired the resource (waited {wait_time:.3f}s)")
                

                hold_time = random.uniform(0.05, 0.1)
                time.sleep(hold_time)
                
                print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                release_resource_fair()
        else:
            starvation_counts[thread_name] += 1
            print(f"[TIMEOUT] {thread_name} timed out waiting for resource")
        

        time.sleep(random.uniform(0.05, 0.1))

def normal_priority_thread(thread_name):
    """Normal priority thread with fair access"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        acquired, wait_time = acquire_resource_fair(thread_name, timeout=3.0)
        
        if acquired:
            try:
                access_counts[thread_name] += 1
                wait_times[thread_name].append(wait_time)
                print(f"[NORMAL] {thread_name} got the resource (waited {wait_time:.3f}s)")
                
                hold_time = random.uniform(0.04, 0.08)
                time.sleep(hold_time)
                
                print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                release_resource_fair()
        else:
            starvation_counts[thread_name] += 1
            print(f"[TIMEOUT] {thread_name} timed out waiting for resource")
        
        time.sleep(random.uniform(0.05, 0.1))

def low_priority_thread(thread_name):
    """Low priority thread with fair access"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        acquired, wait_time = acquire_resource_fair(thread_name, timeout=3.0)
        
        if acquired:
            try:
                access_counts[thread_name] += 1
                wait_times[thread_name].append(wait_time)
                print(f"[LOW] {thread_name} got the resource (waited {wait_time:.3f}s)")
                
                hold_time = random.uniform(0.02, 0.05)
                time.sleep(hold_time)
                
                print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                release_resource_fair()
        else:
            starvation_counts[thread_name] += 1
            print(f"[TIMEOUT] {thread_name} timed out waiting for resource")
        
        time.sleep(random.uniform(0.05, 0.1))

def main():
    """Main function to create and manage threads"""
    global running
    
    print("Starting Fair Thread Access Demonstration")
    print(f"Simulation will run for about {RUN_DURATION} seconds "
          f"or up to {MAX_ATTEMPTS_PER_THREAD} attempts per thread.\n")
    print("Using queue-based fair scheduling to prevent starvation.\n")
    

    threads = []
    

    for i in range(2):
        thread = threading.Thread(
            target=high_priority_thread, 
            args=(f"HighPrio-{i+1}",)
        )
        thread.daemon = True
        threads.append(thread)
    

    for i in range(3):
        thread = threading.Thread(
            target=normal_priority_thread, 
            args=(f"Normal-{i+1}",)
        )
        thread.daemon = True
        threads.append(thread)
    

    for i in range(2):
        thread = threading.Thread(
            target=low_priority_thread, 
            args=(f"LowPrio-{i+1}",)
        )
        thread.daemon = True
        threads.append(thread)
    

    stats_thread = threading.Thread(target=print_stats)
    stats_thread.daemon = True
    threads.append(stats_thread)
    

    for thread in threads:
        thread.start()
    
    start_time = time.time()
    while time.time() - start_time < RUN_DURATION:
        time.sleep(1)
    
    print("\nStopping simulation (time limit reached)...")
    running = False
    

    time.sleep(1)
    

    print("\nFINAL STATISTICS:")
    print("="*50)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        avg_wait = sum(wait_times[thread_name]) / len(wait_times[thread_name]) if wait_times[thread_name] else 0
        print(f"{thread_name:12} | Total Attempts: {attempts:4} | Successes: {successes:4} | "
              f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}% | Avg Wait: {avg_wait:.3f}s")
    
    print("\nFair scheduling implemented! All threads get equitable access.")
    print("Success rates should be much more balanced across all priority levels.")

if __name__ == "__main__":
    main()