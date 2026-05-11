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
    
    def acquire(self, blocking=True, timeout=-1):
        thread_id = threading.current_thread().ident
        event = threading.Event()
        

        with self.queue_lock:
            self.queue.append((thread_id, event))
        

        start_time = time.time()
        while True:
            with self.queue_lock:
                if self.queue and self.queue[0][0] == thread_id:

                    if self.lock.acquire(blocking=False):
                        self.current_holder = thread_id
                        self.queue.popleft()
                        return True
            
            if not blocking:

                with self.queue_lock:
                    self.queue = deque([(tid, evt) for tid, evt in self.queue if tid != thread_id])
                return False
            
            if timeout > 0 and (time.time() - start_time) >= timeout:

                with self.queue_lock:
                    self.queue = deque([(tid, evt) for tid, evt in self.queue if tid != thread_id])
                return False
            
            time.sleep(0.001)
    
    def release(self):
        self.current_holder = None
        self.lock.release()


resource_lock = FairLock()


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
    """High priority thread - now uses fair lock"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if resource_lock.acquire(blocking=True):
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
    """Normal priority thread - now gets fair access"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if resource_lock.acquire(blocking=True):
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
    """Low priority thread - now gets fair access"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if resource_lock.acquire(blocking=True):
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
    
    print("Starting FIXED Thread Starvation Demonstration")
    print("Using Fair Lock - All threads get equal access in FIFO order")
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
    
    print("\nWith Fair Lock: All threads get equitable access regardless of priority!")
    print("Starvation is prevented through FIFO queue-based scheduling.")

if __name__ == "__main__":
    main()