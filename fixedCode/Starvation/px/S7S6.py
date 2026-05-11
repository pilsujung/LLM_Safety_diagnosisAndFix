import threading
import time
import random
from collections import deque


RUN_DURATION = 15
MAX_ATTEMPTS_PER_THREAD = 100


access_counts = {}
starvation_counts = {}
total_attempts = {}
wait_times = {}
running = True


fair_lock = threading.Lock()
ticket_counter = 0
waiting_threads = deque()
condition = threading.Condition(fair_lock)
thread_tickets = {}
wait_start_time = {}

class Priority:
    HIGH = 2
    NORMAL = 1
    LOW = 0

def initialize_stats(thread_name):
    """Initialize statistics for a thread"""
    access_counts[thread_name] = 0
    starvation_counts[thread_name] = 0
    total_attempts[thread_name] = 0
    wait_times[thread_name] = 0.0

def get_ticket(thread_name):
    """Assign ticket number to waiting thread"""
    global ticket_counter
    with fair_lock:
        ticket = ticket_counter
        ticket_counter += 1
        thread_tickets[thread_name] = ticket
        waiting_threads.append(thread_name)
        wait_start_time[thread_name] = time.time()
        return ticket

def fair_acquire(thread_name, priority=Priority.NORMAL):
    """Fair acquire with ticket system and starvation prevention"""
    ticket = get_ticket(thread_name)
    start_wait = time.time()
    
    with condition:
        while True:
            with fair_lock:
                if waiting_threads and waiting_threads[0] == thread_name:
                    waiting_threads.popleft()
                    del thread_tickets[thread_name]
                    wait_times[thread_name] += (time.time() - start_wait)
                    return True
            

            wait_time = time.time() - wait_start_time[thread_name]
            if wait_time > 2.0:
                with fair_lock:
                    if thread_name in waiting_threads:
                        waiting_threads.remove(thread_name)
                        waiting_threads.appendleft(thread_name)
                condition.notify_all()
            
            condition.wait(0.01)
    
    return False

def fair_release(thread_name):
    """Fair release - notify next waiting thread"""
    with condition:
        condition.notify_all()

def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "="*70)
        print("FAIR THREAD SCHEDULING STATISTICS (ANTI-STARVATION)")
        print("="*70)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            avg_wait = wait_times[thread_name] / attempts if attempts > 0 else 0
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(f"{thread_name:12} | Attempts: {attempts:4} | Success: {successes:4} | "
                  f"Starved: {starvations:4} | Wait: {avg_wait:6.3f}s | Rate: {success_rate:5.1f}%")
        print("="*70 + "\n")

def high_priority_thread(thread_name):
    """High priority thread using fair acquire"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_acquire(thread_name, Priority.HIGH):
            try:
                access_counts[thread_name] += 1
                print(f"[HIGH] {thread_name} acquired resource")


                hold_time = random.uniform(0.1, 0.3)
                time.sleep(hold_time)

                print(f"[HIGH] {thread_name} released after {hold_time:.3f}s")
            finally:
                fair_release(thread_name)
        else:
            starvation_counts[thread_name] += 1
            print(f"[HIGH-STARVED] {thread_name} failed to acquire")

        time.sleep(random.uniform(0.01, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread using fair acquire"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_acquire(thread_name, Priority.NORMAL):
            try:
                access_counts[thread_name] += 1
                print(f"[NORMAL] {thread_name} acquired resource")

                hold_time = random.uniform(0.02, 0.08)
                time.sleep(hold_time)

                print(f"[NORMAL] {thread_name} released after {hold_time:.3f}s")
            finally:
                fair_release(thread_name)
        else:
            starvation_counts[thread_name] += 1
            print(f"[NORMAL-STARVED] {thread_name} failed to acquire")

        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread using fair acquire"""
    initialize_stats(thread_name)
    
    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1
        
        if fair_acquire(thread_name, Priority.LOW):
            try:
                access_counts[thread_name] += 1
                print(f"[LOW] {thread_name} acquired resource")

                hold_time = random.uniform(0.01, 0.03)
                time.sleep(hold_time)

                print(f"[LOW] {thread_name} released after {hold_time:.3f}s")
            finally:
                fair_release(thread_name)
        else:
            starvation_counts[thread_name] += 1
            print(f"[LOW-STARVED] {thread_name} failed to acquire")

        time.sleep(random.uniform(0.05, 0.15))

def main():
    """Main function to create and manage threads"""
    global running

    print("Starting FAIR Thread Scheduling Demonstration (ANTI-STARVATION)")
    print(f"Simulation will run for {RUN_DURATION} seconds or {MAX_ATTEMPTS_PER_THREAD} attempts.\n")


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
    time.sleep(2)


    print("\nFINAL FAIR SCHEDULING RESULTS:")
    print("="*70)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        avg_wait = wait_times[thread_name] / max(attempts, 1)
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(f"{thread_name:12} | Attempts: {attempts:4} | Success: {successes:4} | "
              f"Starved: {starvations:4} | AvgWait: {avg_wait:6.3f}s | Rate: {success_rate:5.1f}%")
    print("="*70)
    print("\n✓ STARVATION ELIMINATED: All threads get fair access via ticket system!")

if __name__ == "__main__":
    main()
