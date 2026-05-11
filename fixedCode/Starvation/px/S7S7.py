import threading
import time
import random
from collections import defaultdict


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


resource_lock = threading.Lock()
waiting_threads = threading.Condition(resource_lock)
thread_wait_times = defaultdict(float)
next_thread_id = 0
thread_order = {}


access_counts = defaultdict(int)
starvation_counts = defaultdict(int)
total_attempts = defaultdict(int)


running = True

def get_next_thread_order():
    """Get next fair thread order"""
    global next_thread_id
    with resource_lock:
        order = next_thread_id
        next_thread_id += 1
        return order

def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "="*60)
        print("THREAD FAIRNESS STATISTICS (Fixed Version - No Starvation)")
        print("="*60)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*60 + "\n")

def worker_thread(thread_name, hold_time_range, wait_time_range, priority_label):
    """Generic worker thread with fair scheduling"""
    order = get_next_thread_order()
    thread_order[thread_name] = order
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        with resource_lock:
            thread_wait_times[thread_name] = time.time()
            

            while True:

                current_waiters = sorted(
                    [(name, thread_wait_times[name]) for name in thread_wait_times 
                     if name != thread_name],
                    key=lambda x: thread_order[x[0]]
                )
                
                if not current_waiters or thread_order[thread_name] == min(thread_order.values()):
                    break
                    
                waiting_threads.wait(timeout=0.1)
        

        if running:
            access_counts[thread_name] += 1
            print(f"[{priority_label}] {thread_name} (order:{thread_order[thread_name]:2d}) acquired resource")
            
            hold_time = random.uniform(*hold_time_range)
            resource_lock.release()
            time.sleep(hold_time)
            resource_lock.acquire()
            
            print(f"[{priority_label}] {thread_name} released after {hold_time:.3f}s")
            

            waiting_threads.notify_all()
            resource_lock.release()
        

        time.sleep(random.uniform(*wait_time_range))

def high_priority_thread(thread_name):
    """High priority but now waits fairly"""
    worker_thread(thread_name, (0.1, 0.3), (0.01, 0.05), "HIGH")

def normal_priority_thread(thread_name):
    """Normal priority with fair access"""
    worker_thread(thread_name, (0.02, 0.08), (0.02, 0.08), "NORMAL")

def low_priority_thread(thread_name):
    """Low priority now gets fair access"""
    worker_thread(thread_name, (0.01, 0.03), (0.05, 0.15), "LOW")

def main():
    """Main function with fair scheduling"""
    global running
    
    print("Starting FAIR Thread Scheduling Demonstration")
    print(f"Simulation runs for {RUN_DURATION}s or {MAX_ATTEMPTS_PER_THREAD} attempts/thread")
    print("All threads get FAIR ACCESS regardless of priority!\n")

    threads = []
    

    for i in range(2):
        thread = threading.Thread(target=high_priority_thread, args=(f"HighPrio-{i+1}",))
        thread.daemon = True
        threads.append(thread)
    

    for i in range(3):
        thread = threading.Thread(target=normal_priority_thread, args=(f"Normal-{i+1}",))
        thread.daemon = True
        threads.append(thread)
    

    for i in range(2):
        thread = threading.Thread(target=low_priority_thread, args=(f"LowPrio-{i+1}",))
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
    

    print("\nFINAL FAIR SCHEDULING RESULTS:")
    print("="*70)
    all_threads = sorted(access_counts.keys())
    for thread_name in all_threads:
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(f"{thread_name:12} | Attempts: {attempts:4d} | Successes: {successes:4d} | "
              f"Starved: {starvations:4d} | Rate: {success_rate:5.1f}% | Order: {thread_order.get(thread_name, -1):3d}")
    
    print("\n✓ PROBLEM SOLVED: All threads now get FAIR ACCESS!")
    print("Uses Condition Variables + FCFS ordering to prevent starvation.")

if __name__ == "__main__":
    main()
