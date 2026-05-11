import threading
import time
import random
from queue import Queue


RUN_DURATION = 10              
MAX_ATTEMPTS_PER_THREAD = 50   


class FairLock:
    """A fair lock that ensures FIFO ordering of thread access"""
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = Queue()
        self._holder = None
    
    def acquire(self, blocking=True, timeout=None):
        """Acquire lock in FIFO order"""
        event = threading.Event()
        
        with self._lock:
            if self._holder is None and self._queue.empty():
                self._holder = threading.current_thread()
                return True
            
            if not blocking:
                return False
            
            self._queue.put(event)
        

        if timeout:
            acquired = event.wait(timeout)
        else:
            event.wait()
            acquired = True
        
        if acquired:
            with self._lock:
                self._holder = threading.current_thread()
        
        return acquired
    
    def release(self):
        """Release lock and notify next waiting thread"""
        with self._lock:
            self._holder = None
            if not self._queue.empty():
                next_event = self._queue.get()
                next_event.set()


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
    """High priority thread - now with fair access and shorter hold times"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if resource_lock.acquire(blocking=True, timeout=0.5):
            try:
                access_counts[thread_name] += 1
                print(f"[HIGH PRIORITY] {thread_name} acquired the resource")
                

                hold_time = random.uniform(0.02, 0.08)
                time.sleep(hold_time)
                
                print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                resource_lock.release()

                time.sleep(0.001)
        else:
            starvation_counts[thread_name] += 1
            print(f"[TIMEOUT] {thread_name} timed out waiting for resource")
        

        time.sleep(random.uniform(0.02, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread - now uses fair blocking acquisition"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if resource_lock.acquire(blocking=True, timeout=0.5):
            try:
                access_counts[thread_name] += 1
                print(f"[NORMAL] {thread_name} got the resource")
                
                hold_time = random.uniform(0.02, 0.08)
                time.sleep(hold_time)
                
                print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                resource_lock.release()

                time.sleep(0.001)
        else:
            starvation_counts[thread_name] += 1
            print(f"[TIMEOUT] {thread_name} timed out waiting for resource")
        
        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread - now has fair access to resources"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        

        if resource_lock.acquire(blocking=True, timeout=0.5):
            try:
                access_counts[thread_name] += 1
                print(f"[LOW] {thread_name} got the resource")
                
                hold_time = random.uniform(0.02, 0.08)
                time.sleep(hold_time)
                
                print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
            finally:
                resource_lock.release()

                time.sleep(0.001)
        else:
            starvation_counts[thread_name] += 1
            print(f"[TIMEOUT] {thread_name} timed out waiting for resource")
        

        time.sleep(random.uniform(0.02, 0.08))

def main():
    """Main function to create and manage threads"""
    global running
    
    print("Starting Fair Thread Access Demonstration (STARVATION FIXED)")
    print(f"Simulation will run for about {RUN_DURATION} seconds "
          f"or up to {MAX_ATTEMPTS_PER_THREAD} attempts per thread.")
    print("Using Fair Lock (FIFO queue) to ensure equal access.\n")
    

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
    
    print("\n✓ Starvation FIXED using:")
    print("  1. Fair Lock (FIFO queue) - similar to ReentrantLock(true) in Java")
    print("  2. Equal hold times for all threads")
    print("  3. Blocking acquisition for all threads")
    print("  4. Yield-like behavior (short sleep after release)")

if __name__ == "__main__":
    main()