import threading
import time
import random


RUN_DURATION = 10              
MAX_ATTEMPTS_PER_THREAD = 50   


resource_lock = threading.Lock()

access_counts = {}
starvation_counts = {}
total_attempts = {}


running = True

def initialize_stats(thread_name):
    """Initialize statistics for a thread"""
    access_counts[thread_name] = 0
    starvation_counts[thread_name] = 0
    total_attempts[thread_name] = 0

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
            print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*50 + "\n")

def high_priority_thread(thread_name):
    """High priority thread that holds the resource for longer periods"""
    initialize_stats(thread_name)
    

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        resource_lock.acquire()
        try:
            access_counts[thread_name] += 1
            print(f"[HIGH PRIORITY] {thread_name} acquired the resource")

            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)
            
            print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            resource_lock.release()

        time.sleep(random.uniform(0.01, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread that tries to acquire resource without blocking"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if resource_lock.acquire(blocking=False):
            try:
                access_counts[thread_name] += 1
                print(f"[NORMAL] {thread_name} got the resource")

                hold_time = random.uniform(0.02, 0.08)
                time.sleep(hold_time)
                
                print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                resource_lock.release()
        else:

            starvation_counts[thread_name] += 1
            print(f"[STARVING] {thread_name} cannot access resource")

        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread that gets starved most often"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        if resource_lock.acquire(blocking=False):
            try:
                access_counts[thread_name] += 1
                print(f"[LOW] {thread_name} got the resource")

                hold_time = random.uniform(0.01, 0.03)
                time.sleep(hold_time)
                
                print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                resource_lock.release()
        else:

            starvation_counts[thread_name] += 1
            print(f"[HEAVILY STARVING] {thread_name} cannot access resource")

        time.sleep(random.uniform(0.05, 0.15))

def main():
    """Main function to create and manage threads"""
    global running
    
    print("Starting Thread Starvation Demonstration")
    print(f"Simulation will run for about {RUN_DURATION} seconds "
          f"or up to {MAX_ATTEMPTS_PER_THREAD} attempts per thread.\n")

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
        print(f"{thread_name:12} | Total Attempts: {attempts:4} | Successes: {successes:4} | "
              f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
    
    print("\nStarvation occurs when high-priority threads monopolize the resource,")
    print("preventing lower-priority threads from getting fair access.")

if __name__ == "__main__":
    main()
