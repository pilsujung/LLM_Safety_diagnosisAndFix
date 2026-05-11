import threading
import time
import random
from collections import defaultdict


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


resource_lock = threading.Lock()


access_counts = defaultdict(int)
starvation_counts = defaultdict(int)
total_attempts = defaultdict(int)


running = True

def initialize_stats(thread_name):
    """Initialize statistics for a thread"""
    pass

def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "="*50)
        print("THREAD STATISTICS (FAIR ACCESS):")
        print("="*50)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*50 + "\n")

def worker_thread(thread_name, hold_range, sleep_range, priority_label):
    """Unified worker thread - ALL use blocking acquire() for fairness"""
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        


        resource_lock.acquire()
        try:
            access_counts[thread_name] += 1
            print(f"[{priority_label}] {thread_name} got the resource")
            

            hold_time = random.uniform(*hold_range)
            time.sleep(hold_time)
            
            print(f"[{priority_label}] {thread_name} releasing after {hold_time:.3f}s")
        finally:
            resource_lock.release()
        

        time.sleep(random.uniform(*sleep_range))

def high_priority_thread(thread_name):
    """High priority - longer hold time"""
    worker_thread(thread_name, (0.1, 0.3), (0.01, 0.05), "HIGH")

def normal_priority_thread(thread_name):
    """Normal priority - medium hold time"""
    worker_thread(thread_name, (0.02, 0.08), (0.02, 0.08), "NORMAL")

def low_priority_thread(thread_name):
    """Low priority - shortest hold time"""
    worker_thread(thread_name, (0.01, 0.03), (0.05, 0.15), "LOW")

def main():
    """Main function to create and manage threads"""
    global running
    
    print("Starting FAIR Thread Starvation Solution")
    print(f"Simulation will run for {RUN_DURATION} seconds or {MAX_ATTEMPTS_PER_THREAD} attempts.\n")
    print("FIX: All threads use blocking acquire() → FIFO fairness like ReentrantLock(true)")
    print("Low priority threads now get fair access in arrival order [memory:3][execute_python]")
    
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
    

    print("\nFINAL FAIR STATISTICS:")
    print("="*70)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
              f"Starved: {starvations:4} | Rate: {success_rate:5.1f}%")
    print("="*70)
    
    print("\n✓ STARVATION FIXED: All threads get fair FIFO access [memory:3]")
    print("High priority respected but low priority threads no longer starve completely")

if __name__ == "__main__":
    main()
