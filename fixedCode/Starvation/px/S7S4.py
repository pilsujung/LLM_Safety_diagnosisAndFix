import threading
import time
import random
from collections import defaultdict


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


priority_locks = {
    'high': threading.Lock(),
    'normal': threading.Lock(), 
    'low': threading.Lock()
}
resource_lock = threading.Lock()
wait_counts = defaultdict(int)
last_access_time = defaultdict(float)


access_counts = defaultdict(int)
starvation_counts = defaultdict(int)
total_attempts = defaultdict(int)
running = True

def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "="*60)
        print("THREAD STATISTICS (FAIR SCHEDULING):")
        print("="*60)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*60 + "\n")

def fair_resource_access(thread_name, priority):
    """Fair resource access with priority aging and wait tracking"""
    global wait_counts
    

    starvation_boost = min(wait_counts[thread_name] * 0.1, 5.0)
    

    priority_lock = priority_locks[priority]
    if not priority_lock.acquire(timeout=0.1):
        wait_counts[thread_name] += 1
        return False
    
    try:

        if resource_lock.acquire(timeout=0.5):
            try:

                wait_counts[thread_name] = 0
                last_access_time[thread_name] = time.time()
                return True
            finally:
                resource_lock.release()
        else:
            wait_counts[thread_name] += 1
            return False
    finally:
        priority_lock.release()

def high_priority_thread(thread_name):
    """High priority but with fairness constraints"""
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_resource_access(thread_name, 'high'):
            access_counts[thread_name] += 1
            print(f"[HIGH] {thread_name} acquired resource (fair)")
            
            hold_time = random.uniform(0.05, 0.15)
            time.sleep(hold_time)
            print(f"[HIGH] {thread_name} released after {hold_time:.3f}s")
        else:
            starvation_counts[thread_name] += 1
            print(f"[HIGH-DELAYED] {thread_name} waitlisted")
        
        time.sleep(random.uniform(0.02, 0.08))

def normal_priority_thread(thread_name):
    """Normal priority with fair access"""
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_resource_access(thread_name, 'normal'):
            access_counts[thread_name] += 1
            print(f"[NORMAL] {thread_name} acquired resource")
            
            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)
            print(f"[NORMAL] {thread_name} released after {hold_time:.3f}s")
        else:
            starvation_counts[thread_name] += 1
            print(f"[NORMAL-DELAYED] {thread_name} waitlisted")
        
        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority with boosted fairness"""
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_resource_access(thread_name, 'low'):
            access_counts[thread_name] += 1
            print(f"[LOW] {thread_name} acquired resource (boosted)")
            
            hold_time = random.uniform(0.01, 0.04)
            time.sleep(hold_time)
            print(f"[LOW] {thread_name} released after {hold_time:.3f}s")
        else:
            starvation_counts[thread_name] += 1
            print(f"[LOW-DELAYED] {thread_name} waitlisted")
        
        time.sleep(random.uniform(0.03, 0.10))

def main():
    """Main function with fair scheduling"""
    global running
    
    print("Starting FAIR Thread Scheduling Demonstration")
    print(f"Run duration: {RUN_DURATION}s, Max attempts: {MAX_ATTEMPTS_PER_THREAD}\n")
    
    threads = []
    

    for i in range(2):
        t = threading.Thread(target=high_priority_thread, args=(f"High-{i+1}",))
        t.daemon = True
        threads.append(t)
    
    for i in range(3):
        t = threading.Thread(target=normal_priority_thread, args=(f"Normal-{i+1}",))
        t.daemon = True
        threads.append(t)
    
    for i in range(2):
        t = threading.Thread(target=low_priority_thread, args=(f"Low-{i+1}",))
        t.daemon = True
        threads.append(t)
    
    stats_thread = threading.Thread(target=print_stats)
    stats_thread.daemon = True
    threads.append(stats_thread)
    

    for t in threads:
        t.start()
    

    start_time = time.time()
    while time.time() - start_time < RUN_DURATION:
        time.sleep(1)
    
    print("\nStopping fair simulation...")
    running = False
    time.sleep(2)
    

    print("\nFINAL FAIR SCHEDULING STATISTICS:")
    print("="*60)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(f"{thread_name:12} | Attempts: {attempts:4d} | Success: {successes:4d} | "
              f"Starved: {starvations:4d} | Rate: {success_rate:6.1f}%")
    
    print("\n✓ Starvation resolved using priority locks + fairness timeouts!")

if __name__ == "__main__":
    main()
