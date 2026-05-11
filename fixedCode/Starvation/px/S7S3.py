import threading
import time
import random
from collections import defaultdict


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


class FairLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._waiters = defaultdict(list)
        self._current_priority = float('inf')
        self._owner = None
        
    def acquire(self, priority=0, thread_name=""):
        """Acquire lock with priority (lower number = higher priority)"""
        with self._lock:
            self._waiters[priority].append(thread_name)
            

        while True:
            with self._lock:

                current_highest_priority = min(self._waiters.keys(), default=float('inf'))
                
                if current_highest_priority == float('inf'):

                    self._owner = thread_name
                    self._waiters.clear()
                    self._current_priority = priority
                    return True
                    
                if priority <= current_highest_priority and thread_name in self._waiters[priority]:

                    self._owner = thread_name
                    self._current_priority = priority
                    self._waiters[priority].remove(thread_name)
                    if not self._waiters[priority]:
                        del self._waiters[priority]
                    return True
            

            time.sleep(0.001)
    
    def release(self, thread_name=""):
        with self._lock:
            if self._owner == thread_name:
                self._owner = None
                self._current_priority = float('inf')


fair_lock = FairLock()


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
        print("THREAD STATISTICS (FAIR SCHEDULING):")
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
    """High priority thread - priority 1 (highest)"""
    initialize_stats(thread_name)
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_lock.acquire(priority=1, thread_name=thread_name):
            try:
                access_counts[thread_name] += 1
                print(f"[HIGH PRIORITY] {thread_name} acquired the resource")
                hold_time = random.uniform(0.1, 0.3)
                time.sleep(hold_time)
                print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                fair_lock.release(thread_name)
        else:
            starvation_counts[thread_name] += 1
            print(f"[HIGH PRIORITY STARVING] {thread_name} cannot access resource")
        
        time.sleep(random.uniform(0.01, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread - priority 2"""
    initialize_stats(thread_name)
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_lock.acquire(priority=2, thread_name=thread_name):
            try:
                access_counts[thread_name] += 1
                print(f"[NORMAL] {thread_name} got the resource")
                hold_time = random.uniform(0.02, 0.08)
                time.sleep(hold_time)
                print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                fair_lock.release(thread_name)
        else:
            starvation_counts[thread_name] += 1
            print(f"[NORMAL STARVING] {thread_name} cannot access resource")
        
        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread - priority 3 (lowest)"""
    initialize_stats(thread_name)
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_lock.acquire(priority=3, thread_name=thread_name):
            try:
                access_counts[thread_name] += 1
                print(f"[LOW] {thread_name} got the resource")
                hold_time = random.uniform(0.01, 0.03)
                time.sleep(hold_time)
                print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                fair_lock.release(thread_name)
        else:
            starvation_counts[thread_name] += 1
            print(f"[LOW STARVING] {thread_name} cannot access resource")
        
        time.sleep(random.uniform(0.05, 0.15))

def main():
    """Main function to create and manage threads"""
    global running
    
    print("Starting FAIR Thread Scheduling Demonstration")
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
    

    print("\nFINAL FAIR SCHEDULING STATISTICS:")
    print("="*50)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(f"{thread_name:12} | Total Attempts: {attempts:4} | Successes: {successes:4} | "
              f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
    
    print("\nFAIR LOCK ensures all priorities get access proportionally!")

if __name__ == "__main__":
    main()
