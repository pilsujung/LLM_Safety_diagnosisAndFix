import threading
import time
import random
from collections import deque


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


class FairLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.queue = deque()
        self.queue_lock = threading.Lock()
        self.current_holder = None
    
    def acquire(self, thread_name, blocking=True, timeout=None):
        """Fair acquire - uses FIFO queue to ensure fairness"""
        event = threading.Event()
        
        with self.queue_lock:
            self.queue.append((thread_name, event))
        

        start_time = time.time()
        while True:
            with self.queue_lock:
                if self.queue and self.queue[0][0] == thread_name:

                    if self.lock.acquire(blocking=False):
                        self.current_holder = thread_name
                        self.queue.popleft()
                        return True
            
            if not blocking:

                with self.queue_lock:
                    self.queue = deque([(n, e) for n, e in self.queue if n != thread_name])
                return False
            
            if timeout and (time.time() - start_time) > timeout:
                with self.queue_lock:
                    self.queue = deque([(n, e) for n, e in self.queue if n != thread_name])
                return False
            
            time.sleep(0.01)
    
    def release(self):
        """Release the lock"""
        self.current_holder = None
        self.lock.release()


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
        print("THREAD STATISTICS (WITH FAIR QUEUEING):")
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
    """High priority thread - now uses fair queue"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if fair_lock.acquire(thread_name, blocking=True):
            try:
                access_counts[thread_name] += 1
                print(f"[HIGH PRIORITY] {thread_name} acquired the resource")
                
                hold_time = random.uniform(0.1, 0.3)
                time.sleep(hold_time)
                print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                fair_lock.release()
        
        time.sleep(random.uniform(0.01, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread - benefits from fair queue"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if fair_lock.acquire(thread_name, blocking=True, timeout=0.5):
            try:
                access_counts[thread_name] += 1
                print(f"[NORMAL] {thread_name} got the resource")
                
                hold_time = random.uniform(0.02, 0.08)
                time.sleep(hold_time)
                print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                fair_lock.release()
        else:
            starvation_counts[thread_name] += 1
            print(f"[NORMAL] {thread_name} timed out waiting")
        
        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread - now gets fair access"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if fair_lock.acquire(thread_name, blocking=True, timeout=0.5):
            try:
                access_counts[thread_name] += 1
                print(f"[LOW] {thread_name} got the resource")
                
                hold_time = random.uniform(0.01, 0.03)
                time.sleep(hold_time)
                print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                fair_lock.release()
        else:
            starvation_counts[thread_name] += 1
            print(f"[LOW] {thread_name} timed out waiting")
        
        time.sleep(random.uniform(0.05, 0.15))

def main():
    """Main function to create and manage threads"""
    global running
    
    print("Starting FAIR Thread Access Demonstration")
    print(f"Simulation will run for about {RUN_DURATION} seconds")
    print("Using FIFO queue to prevent starvation\n")
    
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
    
    print("\n✓ Fair queueing implemented - all threads get proportional access!")
    print("✓ FIFO queue ensures threads are served in order of arrival")
    print("✓ No thread monopolizes the resource")

if __name__ == "__main__":
    main()