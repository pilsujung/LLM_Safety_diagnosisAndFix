import threading
import time
import random


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


STARVATION_THRESHOLD = 0.7


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
        print("\n" + "=" * 50)
        print("THREAD STATISTICS:")
        print("=" * 50)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(
                f"{thread_name:12} | Attempts: {attempts:4} | "
                f"Successes: {successes:4} | "
                f"Long waits(>{STARVATION_THRESHOLD:.1f}s): {starvations:4} | "
                f"Success Rate: {success_rate:5.1f}%"
            )
        print("=" * 50 + "\n")


def acquire_fairly(thread_name, label):
    """
    Helper: block on the lock (like lock() in the Java example)
    and measure how long we waited before acquiring it.
    """
    start_wait = time.time()
    resource_lock.acquire()
    wait_time = time.time() - start_wait

    access_counts[thread_name] += 1
    if wait_time > STARVATION_THRESHOLD:
        starvation_counts[thread_name] += 1

    print(f"[{label}] {thread_name} acquired the resource after {wait_time:.3f}s wait")
    return wait_time


def high_priority_thread(thread_name):
    """High priority thread that holds the resource for longer periods"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        acquire_fairly(thread_name, "HIGH PRIORITY")
        try:

            hold_time = random.uniform(0.15, 0.30)
            time.sleep(hold_time)
            print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.02, 0.05))


def normal_priority_thread(thread_name):
    """
    Normal priority thread.
    FIX: use blocking acquire instead of acquire(blocking=False).
    This matches the Java ReentrantLock(true) + lock() pattern:
    everyone queues fairly on the same lock instead of “try & fail”.
    """
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        acquire_fairly(thread_name, "NORMAL")
        try:

            hold_time = random.uniform(0.05, 0.10)
            time.sleep(hold_time)
            print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.03, 0.07))


def low_priority_thread(thread_name):
    """
    Low priority thread.
    FIX: also uses blocking acquire (no more immediate “cannot access” failures).
    Priority difference is only in how often and how long it uses the resource,
    not in whether it can ever get it.
    """
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        acquire_fairly(thread_name, "LOW")
        try:

            hold_time = random.uniform(0.02, 0.05)
            time.sleep(hold_time)
            print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            resource_lock.release()



        time.sleep(random.uniform(0.06, 0.15))


def main():
    """Main function to create and manage threads"""
    global running

    print("Starting Thread Starvation Demonstration (starvation-free version)")
    print(
        f"Simulation will run for about {RUN_DURATION} seconds "
        f"or up to {MAX_ATTEMPTS_PER_THREAD} attempts per thread.\n"
    )

    threads = []


    for i in range(2):
        thread = threading.Thread(
            target=high_priority_thread,
            args=(f"HighPrio-{i+1}",),
        )
        thread.daemon = True
        threads.append(thread)


    for i in range(3):
        thread = threading.Thread(
            target=normal_priority_thread,
            args=(f"Normal-{i+1}",),
        )
        thread.daemon = True
        threads.append(thread)


    for i in range(2):
        thread = threading.Thread(
            target=low_priority_thread,
            args=(f"LowPrio-{i+1}",),
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
    print("=" * 50)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(
            f"{thread_name:12} | Total Attempts: {attempts:4} | "
            f"Successes: {successes:4} | "
            f"Long waits(>{STARVATION_THRESHOLD:.1f}s): {starvations:4} | "
            f"Success Rate: {success_rate:5.1f}%"
        )

    print("\nIn this fixed version, all threads block on the same lock.")
    print("Normal / low-priority threads no longer spin with non-blocking "
          "acquire, so they eventually acquire the resource and are not starved.")


if __name__ == "__main__":
    main()
