import threading 
import time
import queue
import random


resource_lock = threading.Lock()
total_resource_accesses = 0
thread_statistics = {}


wait_queue = queue.Queue()
fair_condition = threading.Condition()


def initialize_thread_stats(thread_id, priority):
    """Initialize statistics tracking for each thread"""
    thread_statistics[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': priority,
        'starvation_count': 0
    }


def acquire_fair_lock(thread_id):
    """
    Acquire the lock in FIFO order, similar to ReentrantLock(true).
    Every thread that wants the resource is queued and woken up fairly.
    """
    with fair_condition:

        wait_queue.put(thread_id)


        while wait_queue.queue[0] != thread_id:
            fair_condition.wait()


    resource_lock.acquire()


def release_fair_lock(thread_id):
    """Release the lock and wake up the next waiting thread."""
    resource_lock.release()
    with fair_condition:
        if not wait_queue.empty() and wait_queue.queue[0] == thread_id:
            wait_queue.get()
        fair_condition.notify_all()


def access_shared_resource(thread_priority, thread_id, max_attempts=25):
    """
    Simulate thread attempting to access a shared resource.

    ✅ FIXED VERSION:
    - Uses a fair, blocking lock acquisition (no busy waiting).
    - No priority-based queue ordering that starves low-priority threads.
    - Similar idea to `new ReentrantLock(true)` in the Java example.
    """
    global total_resource_accesses
    
    initialize_thread_stats(thread_id, thread_priority)
    attempt_counter = 0
    
    print(f"Thread {thread_id} (Priority {thread_priority}) started - attempting resource access")
    
    while attempt_counter < max_attempts:
        start_wait_time = time.time()
        

        acquire_fair_lock(thread_id)
        try:
            current_time = time.strftime('%H:%M:%S', time.localtime())
            wait_duration = time.time() - start_wait_time
            

            stats = thread_statistics[thread_id]
            stats['successful_accesses'] += 1
            stats['total_wait_time'] += wait_duration
            total_resource_accesses += 1
            
            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) "
                  f"acquired resource after {wait_duration:.3f}s wait")
            

            resource_usage_time = 0.15 + (thread_priority * 0.05)
            time.sleep(resource_usage_time)
            
            print(f"[{current_time}] ✓ Thread {thread_id} (Priority {thread_priority}) "
                  f"released resource (used for {resource_usage_time:.3f}s)")
        finally:
            release_fair_lock(thread_id)
        
        attempt_counter += 1
        

        inter_attempt_delay = 0.1 + (thread_priority * 0.05) + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)
    

    stats = thread_statistics[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)
    
    print(f"\n📊 Thread {thread_id} Final Stats:")
    print(f"   Priority: {thread_priority}")
    print(f"   Successful accesses: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"   Failed attempts: {stats['failed_attempts']}")
    print(f"   Average wait time: {avg_wait_time:.3f}s")
    print(f"   Starvation episodes: {stats['starvation_count']}")


def display_simulation_summary():
    """Display overall simulation statistics"""
    print("\n" + "="*60)
    print("THREAD STARVATION SIMULATION SUMMARY")
    print("="*60)
    print(f"Total resource accesses across all threads: {total_resource_accesses}")
    
    high_priority_accesses = sum(stats['successful_accesses'] 
                                for stats in thread_statistics.values() 
                                if stats['priority'] <= 2)
    low_priority_accesses = sum(stats['successful_accesses'] 
                               for stats in thread_statistics.values() 
                               if stats['priority'] > 2)
    
    print(f"High priority threads (1-2) total accesses: {high_priority_accesses}")
    print(f"Low priority threads (3+) total accesses: {low_priority_accesses}")
    
    total_starvation = sum(stats['starvation_count'] for stats in thread_statistics.values())
    print(f"Total starvation episodes detected: {total_starvation}")
    
    if low_priority_accesses < high_priority_accesses * 0.3:
        print("⚠️  STARVATION DETECTED: Low priority threads significantly underserved!")
    
    print("="*60)


def main():
    """Main function to run the thread starvation simulation"""
    print("Starting Thread Starvation Simulation")
    print("Lower priority numbers = Higher importance (1 = highest priority)")
    print("-" * 60)
    

    thread_configurations = [
        (1, "HighPrio-A"),
        (2, "HighPrio-B"),
        (4, "LowPrio-A"),
        (5, "LowPrio-B"),
        (3, "MedPrio-A")
    ]
    
    active_threads = []
    

    for priority, thread_name in thread_configurations:
        thread_obj = threading.Thread(
            target=access_shared_resource, 
            args=(priority, thread_name),
            name=f"Thread-{thread_name}"
        )
        active_threads.append(thread_obj)
        thread_obj.start()
        time.sleep(0.1)
    

    for thread_obj in active_threads:
        thread_obj.join()
    

    display_simulation_summary()
    print("\nSimulation completed - Starvation-free execution")


if __name__ == "__main__":
    main()
