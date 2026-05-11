import threading
import time
import random


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


resource_lock = threading.Lock()


ticket_lock = threading.Lock()
ticket_cond = threading.Condition(ticket_lock)
next_ticket = 0
serving_ticket = 0

def acquire_fair(thread_name):
    """
    Acquire the shared resource in FIFO order using a ticket lock.
    Returns how long this thread waited before entering the critical section.
    """
    global next_ticket, serving_ticket

    request_time = time.time()
    with ticket_lock:
        my_ticket = next_ticket
        next_ticket += 1


        while my_ticket != serving_ticket:
            ticket_cond.wait()


    resource_lock.acquire()
    wait_time = time.time() - request_time
    return wait_time


def release_fair():
    """
    Release the shared resource and wake up the next ticket holder.
    """
    global serving_ticket


    resource_lock.release()


    with ticket_lock:
        serving_ticket += 1
        ticket_cond.notify_all()



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
                f"{thread_name:12} | Attempts: {attempts:4} | Successes: {successes:4} | "
                f"Starved: {starvations:4} | Success Rate: {success_rate:5.1f}%"
            )
        print("=" * 50 + "\n")


def high_priority_thread(thread_name):
    """High priority thread that holds the resource for longer periods"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1


        wait_time = acquire_fair(thread_name)
        try:
            access_counts[thread_name] += 1
            print(
                f"[HIGH PRIORITY] {thread_name} acquired the resource "
                f"after {wait_time:.3f}s wait"
            )


            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)

            print(
                f"[HIGH PRIORITY] {thread_name} releasing resource after {hold_time:.3f}s"
            )
        finally:
            release_fair()


        time.sleep(random.uniform(0.01, 0.05))


def normal_priority_thread(thread_name):
    """Normal priority thread that now acquires the resource fairly (blocking)"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1


        wait_time = acquire_fair(thread_name)
        try:
            access_counts[thread_name] += 1
            print(
                f"[NORMAL] {thread_name} acquired the resource "
                f"after {wait_time:.3f}s wait"
            )


            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)

            print(
                f"[NORMAL] {thread_name} releasing resource after {hold_time:.3f}s"
            )
        finally:
            release_fair()


        time.sleep(random.uniform(0.02, 0.08))


def low_priority_thread(thread_name):
    """Low priority thread that used to starve; now it also uses the fair lock"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1


        wait_time = acquire_fair(thread_name)
        try:
            access_counts[thread_name] += 1
            print(
                f"[LOW] {thread_name} acquired the resource "
                f"after {wait_time:.3f}s wait"
            )


            hold_time = random.uniform(0.01, 0.03)
            time.sleep(hold_time)

            print(
                f"[LOW] {thread_name} releasing resource after {hold_time:.3f}s"
            )
        finally:
            release_fair()


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
            args=(f"HighPrio-{i + 1}",),
        )
        thread.daemon = True
        threads.append(thread)


    for i in range(3):
        thread = threading.Thread(
            target=normal_priority_thread,
            args=(f"Normal-{i + 1}",),
        )
        thread.daemon = True
        threads.append(thread)


    for i in range(2):
        thread = threading.Thread(
            target=low_priority_thread,
            args=(f"LowPrio-{i + 1}",),
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
            f"Successes: {successes:4} | Starved: {starvations:4} | "
            f"Success Rate: {success_rate:5.1f}%"
        )

    print("\nStarvation is resolved by enforcing fair (FIFO) access to the resource.")
    print("All threads eventually acquire the lock, even if some are 'low priority'.")


if __name__ == "__main__":
    main()
