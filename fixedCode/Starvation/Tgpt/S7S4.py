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

STARVATION_THRESHOLD = 1.0


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
                f"{thread_name:12} | Attempts: {attempts:4} "
                f"| Successes: {successes:4} "
                f"| Starved(> {STARVATION_THRESHOLD:.1f}s wait): {starvations:4} "
                f"| Success Rate: {success_rate:5.1f}%"
            )
        print("=" * 50 + "\n")


def acquire_with_wait_stats(thread_name: str, label: str) -> float:
    """
    Acquire the shared lock in blocking mode and measure waiting time.
    If the wait is 'too long', count it as a starvation event for statistics.
    """
    start_wait = time.time()
    resource_lock.acquire()
    waited = time.time() - start_wait

    if waited > STARVATION_THRESHOLD:
        starvation_counts[thread_name] += 1
        print(
            f"[STARVATION WARNING] [{label}] {thread_name} waited "
            f"{waited:.3f}s before acquiring the resource"
        )
    else:
        print(
            f"[{label}] {thread_name} acquired the resource "
            f"after {waited:.3f}s wait"
        )

    return waited


def high_priority_thread(thread_name):
    """High priority thread that holds the resource for longer periods"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        acquire_with_wait_stats(thread_name, "HIGH PRIORITY")
        try:
            access_counts[thread_name] += 1


            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)

            print(
                f"[HIGH PRIORITY] {thread_name} releasing resource "
                f"after {hold_time:.3f}s"
            )
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.01, 0.05))


def normal_priority_thread(thread_name):
    """Normal priority thread that now blocks fairly for the resource"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1


        acquire_with_wait_stats(thread_name, "NORMAL")
        try:
            access_counts[thread_name] += 1
            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)
            print(
                f"[NORMAL] {thread_name} releasing resource "
                f"after {hold_time:.3f}s"
            )
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.02, 0.08))


def low_priority_thread(thread_name):
    """Low priority thread: blocks for the resource but tries less often"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1


        acquire_with_wait_stats(thread_name, "LOW")
        try:
            access_counts[thread_name] += 1
            hold_time = random.uniform(0.01, 0.03)
            time.sleep(hold_time)
            print(
                f"[LOW] {thread_name} releasing resource "
                f"after {hold_time:.3f}s"
            )
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.05, 0.15))


def main():
    global running

    print("Starting Thread Starvation Demonstration (starvation-free version)")
    print(
        f"Simulation will run for about {RUN_DURATION} seconds "
        f"or up to {MAX_ATTEMPTS_PER_THREAD} attempts per thread.\n"
    )

    threads = []


    for i in range(2):
        t = threading.Thread(
            target=high_priority_thread,
            args=(f"HighPrio-{i + 1}",),
            daemon=True,
        )
        threads.append(t)


    for i in range(3):
        t = threading.Thread(
            target=normal_priority_thread,
            args=(f"Normal-{i + 1}",),
            daemon=True,
        )
        threads.append(t)


    for i in range(2):
        t = threading.Thread(
            target=low_priority_thread,
            args=(f"LowPrio-{i + 1}",),
            daemon=True,
        )
        threads.append(t)


    stats_thread = threading.Thread(target=print_stats, daemon=True)
    threads.append(stats_thread)


    for t in threads:
        t.start()


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
            f"{thread_name:12} | Total Attempts: {attempts:4} "
            f"| Successes: {successes:4} "
            f"| Starved(> {STARVATION_THRESHOLD:.1f}s wait): {starvations:4} "
            f"| Success Rate: {success_rate:5.1f}%"
        )

    print("\nWith blocking acquisition, every thread eventually acquires the lock,")
    print("so normal and low priority threads are no longer starved.")


if __name__ == "__main__":
    main()
