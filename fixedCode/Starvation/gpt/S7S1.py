import threading
import time
import random


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


resource_lock = threading.Lock()


thread_order = []
current_turn_index = 0
turn_lock = threading.Lock()
turn_condition = threading.Condition(turn_lock)


access_counts = {}
starvation_counts = {}
total_attempts = {}


running = True

def initialize_stats(thread_name):
    """Initialize statistics for a thread"""
    access_counts[thread_name] = 0
    starvation_counts[thread_name] = 0
    total_attempts[thread_name] = 0

def fair_acquire(thread_name, starvation_threshold=0.5):
    """
    Acquire the shared resource in a fair, round-robin way.

    All worker threads share a global order (thread_order).
    Only the thread whose "turn" it currently is may take the lock.
    After each release the turn moves to the next thread in that list.

    This prevents high-priority threads from continuously re-acquiring
    the resource and starving others.
    """
    global current_turn_index


    my_index = thread_order.index(thread_name)

    wait_start = time.time()
    with turn_condition:

        while running and my_index != current_turn_index:
            turn_condition.wait()

        if not running:
            return False


    resource_lock.acquire()


    wait_time = time.time() - wait_start
    if wait_time > starvation_threshold:
        starvation_counts[thread_name] += 1

    return True

def fair_release():
    """
    Release the shared resource and advance the turn to the next thread.
    """
    global current_turn_index

    resource_lock.release()

    with turn_condition:
        current_turn_index = (current_turn_index + 1) % len(thread_order)

        turn_condition.notify_all()

def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(2)
        print("\n" + "="*60)
        print("THREAD STATISTICS (intermediate):")
        print("="*60)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                  f"Starved(wait>0.5s): {starvations:4} | Success Rate: {success_rate:5.1f}%")
        print("="*60 + "\n")

def high_priority_thread(thread_name):
    """High priority thread that would normally hog the resource"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        if not fair_acquire(thread_name):
            break

        try:
            access_counts[thread_name] += 1
            print(f"[HIGH PRIORITY] {thread_name} acquired the resource")



            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)

            print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            fair_release()


        time.sleep(random.uniform(0.01, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread that now participates fairly"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        if not fair_acquire(thread_name):
            break

        try:
            access_counts[thread_name] += 1
            print(f"[NORMAL] {thread_name} got the resource")

            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)

            print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            fair_release()

        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread that used to starve, but now gets turns"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        if not fair_acquire(thread_name):
            break

        try:
            access_counts[thread_name] += 1
            print(f"[LOW] {thread_name} got the resource")

            hold_time = random.uniform(0.01, 0.03)
            time.sleep(hold_time)

            print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            fair_release()

        time.sleep(random.uniform(0.05, 0.15))

def main():
    """Main function to create and manage threads"""
    global running

    print("Starting Thread Starvation-Free Demonstration")
    print(f"Simulation will run for about {RUN_DURATION} seconds "
          f"or up to {MAX_ATTEMPTS_PER_THREAD} attempts per thread.\n")

    threads = []




    for i in range(2):
        thread_order.append(f"HighPrio-{i+1}")
    for i in range(3):
        thread_order.append(f"Normal-{i+1}")
    for i in range(2):
        thread_order.append(f"LowPrio-{i+1}")


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


    with turn_condition:
        turn_condition.notify_all()


    time.sleep(1)


    print("\nFINAL STATISTICS:")
    print("="*60)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(f"{thread_name:12} | Total Attempts: {attempts:4} | Successes: {successes:4} | "
              f"Starved(wait>0.5s): {starvations:4} | Success Rate: {success_rate:5.1f}%")

    print("\nIn this fixed version, every thread gets a turn to acquire the resource.")
    print("High-priority threads may still hold it a bit longer,")
    print("but they can no longer monopolize it and starve lower-priority threads.")

if __name__ == "__main__":
    main()
