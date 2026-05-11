import threading
import time
import random


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50





_resource_lock = threading.Lock()


queue_lock = threading.Lock()
queue_cond = threading.Condition(queue_lock)
next_ticket = 0
serving = 0


def fair_acquire(thread_name):
    """Acquire the shared resource in FIFO order and return waiting time."""
    global next_ticket, serving
    with queue_cond:
        my_ticket = next_ticket
        next_ticket += 1
        start_wait = time.time()

        while my_ticket != serving:
            queue_cond.wait()
        wait_time = time.time() - start_wait

    _resource_lock.acquire()
    return wait_time


def fair_release():
    """Release the shared resource and wake the next waiter."""
    global serving
    _resource_lock.release()
    with queue_cond:
        serving += 1
        queue_cond.notify_all()





access_counts = {}
starvation_counts = {}
total_attempts = {}
total_wait_time = {}


running = True


def initialize_stats(thread_name):
    """Initialize statistics for a thread"""
    access_counts[thread_name] = 0
    starvation_counts[thread_name] = 0
    total_attempts[thread_name] = 0
    total_wait_time[thread_name] = 0.0


def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "=" * 70)
        print("THREAD STATISTICS:")
        print("=" * 70)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            avg_wait = (total_wait_time[thread_name] / successes) if successes > 0 else 0.0
            print(
                f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}% | "
                f"Avg wait: {avg_wait:6.3f}s"
            )
        print("=" * 70 + "\n")





def high_priority_thread(thread_name):
    """High priority thread that holds the resource for longer periods"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        wait_time = fair_acquire(thread_name)
        total_wait_time[thread_name] += wait_time
        try:
            access_counts[thread_name] += 1
            print(f"[HIGH PRIORITY] {thread_name} acquired the resource after {wait_time:.3f}s wait")


            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)

            print(f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            fair_release()


        time.sleep(random.uniform(0.01, 0.05))


def normal_priority_thread(thread_name):
    """Normal priority thread that now waits fairly for the resource"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1


        wait_time = fair_acquire(thread_name)
        total_wait_time[thread_name] += wait_time
        try:
            access_counts[thread_name] += 1
            print(f"[NORMAL] {thread_name} got the resource after waiting {wait_time:.3f}s")


            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)

            print(f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            fair_release()

        time.sleep(random.uniform(0.02, 0.08))


def low_priority_thread(thread_name):
    """Low priority thread that used to starve, now also uses the fair lock"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1


        wait_time = fair_acquire(thread_name)
        total_wait_time[thread_name] += wait_time
        try:
            access_counts[thread_name] += 1
            print(f"[LOW] {thread_name} got the resource after waiting {wait_time:.3f}s")


            hold_time = random.uniform(0.01, 0.03)
            time.sleep(hold_time)

            print(f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s")
        finally:
            fair_release()


        time.sleep(random.uniform(0.05, 0.15))





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
    print("=" * 70)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        avg_wait = (total_wait_time[thread_name] / successes) if successes > 0 else 0.0
        print(
            f"{thread_name:12} | Total Attempts: {attempts:4} | Successes: {successes:4} | "
            f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}% | "
            f"Avg wait: {avg_wait:6.3f}s"
        )

    print(
        "\nStarvation is avoided because all threads acquire the resource through a fair "
        "FIFO ticket lock instead of non-blocking tryLock calls."
    )


if __name__ == "__main__":
    main()
