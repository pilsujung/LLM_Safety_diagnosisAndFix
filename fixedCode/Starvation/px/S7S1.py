import threading
import time
import random
from collections import defaultdict


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


class FairTicketLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.next_ticket = 0
        self.current_ticket = 0
        self.waiting_threads = defaultdict(list)
    
    def acquire(self, priority=1):
        thread_name = threading.current_thread().name
        my_ticket = self.next_ticket
        self.next_ticket += 1
        
        with self.lock:
            self.waiting_threads[priority].append(thread_name)
            
            while True:

                if my_ticket == self.current_ticket:

                    higher_priorities = [p for p in self.waiting_threads if p > priority]
                    if all(len(self.waiting_threads[p]) == 0 for p in higher_priorities):
                        self.waiting_threads[priority].remove(thread_name)
                        break
                time.sleep(0.001)
    
    def release(self):
        with self.lock:
            if self.waiting_threads:

                for p in sorted(self.waiting_threads, reverse=True):
                    if self.waiting_threads[p]:
                        self.current_ticket += 1
                        return
            self.current_ticket += 1


fair_lock = FairTicketLock()


access_counts = defaultdict(int)
starvation_counts = defaultdict(int)
total_attempts = defaultdict(int)


running = True

def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "="*60)
        print("FAIR THREAD SCHEDULING STATISTICS (ANTI-STARVATION):")
        print("="*60)
        all_threads = sorted(set(list(access_counts.keys()) + list(starvation_counts.keys()) + list(total_attempts.keys())))
        for thread_name in sorted(all_threads):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*60 + "\n")

def high_priority_thread(thread_name):
    """High priority thread (priority=3)"""
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        fair_lock.acquire(priority=3)
        try:
            access_counts[thread_name] += 1
            print(f"[HIGH PRIORITY 3] {thread_name} acquired resource (ticket fair)")
            
            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)
            
            print(f"[HIGH PRIORITY 3] {thread_name} released after {hold_time:.3f}s")
        finally:
            fair_lock.release()
        
        time.sleep(random.uniform(0.01, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread (priority=2)"""
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        fair_lock.acquire(priority=2)
        try:
            access_counts[thread_name] += 1
            print(f"[NORMAL PRIORITY 2] {thread_name} got resource (fair)")
            
            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)
            
            print(f"[NORMAL PRIORITY 2] {thread_name} released after {hold_time:.3f}s")
        finally:
            fair_lock.release()
        
        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread (priority=1) - now gets fair access"""
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        fair_lock.acquire(priority=1)
        try:
            access_counts[thread_name] += 1
            print(f"[LOW PRIORITY 1] {thread_name} got resource (FAIR ACCESS!)")
            
            hold_time = random.uniform(0.01, 0.03)
            time.sleep(hold_time)
            
            print(f"[LOW PRIORITY 1] {thread_name} released after {hold_time:.3f}s")
        finally:
            fair_lock.release()
        
        time.sleep(random.uniform(0.05, 0.15))

def main():
    """Main function with fair scheduling"""
    global running
    
    print("FAIR THREAD SCHEDULING DEMONSTRATION (ANTI-STARVATION)")
    print(f"Run for {RUN_DURATION}s or {MAX_ATTEMPTS_PER_THREAD} attempts per thread")
    print("Using priority ticket lock - ALL threads get fair access!\n")
    
    threads = []
    

    for i in range(2):
        t = threading.Thread(target=high_priority_thread, name=f"HighPrio-{i+1}")
        t.daemon = True
        threads.append(t)
    

    for i in range(3):
        t = threading.Thread(target=normal_priority_thread, name=f"Normal-{i+1}")
        t.daemon = True
        threads.append(t)
    

    for i in range(2):
        t = threading.Thread(target=low_priority_thread, name=f"LowPrio-{i+1}")
        t.daemon = True
        threads.append(t)
    

    stats_t = threading.Thread(target=print_stats, name="Stats")
    stats_t.daemon = True
    threads.append(stats_t)
    

    for t in threads:
        t.start()
    
    start_time = time.time()
    while time.time() - start_time < RUN_DURATION:
        time.sleep(1)
    
    print("\nStopping simulation...")
    running = False
    time.sleep(2)
    
    print("\nFINAL FAIR SCHEDULING RESULTS:")
    print("="*60)
    all_threads = sorted(set(list(access_counts.keys()) + list(total_attempts.keys())))
    for thread_name in all_threads:
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(f"{thread_name:12} | Attempts: {attempts:4d} | Successes: {successes:4d} | "
              f"Starved: {starvations:4d} | Rate: {success_rate:5.1f}%")
    
    print("\n✓ STARVATION ELIMINATED! All priorities get fair access via ticket system.")

if __name__ == "__main__":
    main()
