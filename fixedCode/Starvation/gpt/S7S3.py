import threading
import time
import random


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50


ticket_cond = threading.Condition()
next_ticket = 0
serving_ticket = 0


access_counts = {}
starvation_counts = {}
total_attempts = {}
total_wait_times = {}


running = True


def initialize_stats(thread_name):
    """Initialize statistics for a thread"""
    access_counts[thread_name] = 0
    starvation_counts[thread_name] = 0
    total_attempts[thread_name] = 0
    total_wait_times[thread_name] = 0.0


def fair_acquire(thread_name):
    """
    Ticket-based fair acquisition of the 'resource'.
    Every caller gets a ticket and is served strictly in FIFO order.

    Returns:
        (acquired: bool, wait_time: float)
    """
    global next_ticket, serving_ticket

    with ticket_cond:
        if not running:
            return False, 0.0

        my_ticket = next_ticket
        next_ticket += 1
        start_wait = time.time()


        while running and my_ticket != serving_ticket:
            ticket_cond.wait(timeout=0.1)

        if not running:

            return False, time.time() - start_wait


    wait_time = time.time() - start_wait
    return True, wait_time


def fair_release():
    """
    Release the fair lock and wake the next waiter.
    """
    global serving_ticket
    with ticket_cond:
        serving_ticket += 1
        ticket_cond.notify_all()


def print_stats():
    """Print current statistics every few seconds"""
    while running:
        time.sleep(3)
        print("\n" + "=" * 60)
        print("THREAD STATISTICS:")
        print("=" * 60)
        for thread_name in sorted(access_counts.keys()):
            attempts = total_attempts[thread_name]
            successes = access_counts[thread_name]
            starvations = starvation_counts[thread_name]
            avg_wait = (total_wait_times[thread_name] / successes
                        if successes > 0 else 0.0)
            success_rate = (successes / attempts * 100) if attempts > 0 else 0
            print(
                f"{thread_name:12} | Attempts: {attempts:4} | "
                f"Successes: {successes:4} | Starved: {starvations:4} | "
                f"Success Rate: {success_rate:5.1f}% | "
                f"Avg wait: {avg_wait:6.3f}s"
            )
        print("=" * 60 + "\n")


def high_priority_thread(thread_name):
    """High priority thread that holds the resource for longer periods"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        acquired, wait_time = fair_acquire(thread_name)
        if not acquired:
            break

        try:
            access_counts[thread_name] += 1
            total_wait_times[thread_name] += wait_time
            print(
                f"[HIGH PRIORITY] {thread_name} acquired the resource "
                f"after {wait_time:.3f}s wait"
            )


            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)

            print(
                f"[HIGH PRIORITY] {thread_name} releasing resource "
                f"after {hold_time:.3f}s"
            )
        finally:
            fair_release()


        time.sleep(random.uniform(0.01, 0.05))


def normal_priority_thread(thread_name):
    """Normal priority thread (medium hold time)"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        acquired, wait_time = fair_acquire(thread_name)
        if not acquired:
            break

        try:
            access_counts[thread_name] += 1
            total_wait_times[thread_name] += wait_time
            print(
                f"[NORMAL] {thread_name} got the resource "
                f"after waiting {wait_time:.3f}s"
            )


            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)

            print(
                f"[NORMAL] {thread_name} releasing resource "
                f"after {hold_time:.3f}s"
            )
        finally:
            fair_release()

        time.sleep(random.uniform(0.02, 0.08))


def low_priority_thread(thread_name):
    """Low priority thread; now also guaranteed fair access"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        acquired, wait_time = fair_acquire(thread_name)
        if not acquired:
            break

        try:
            access_counts[thread_name] += 1
            total_wait_times[thread_name] += wait_time
            print(
                f"[LOW] {thread_name} got the resource "
                f"after waiting {wait_time:.3f}s"
            )


            hold_time = random.uniform(0.01, 0.03)
            time.sleep(hold_time)

            print(
                f"[LOW] {thread_name} releasing resource "
                f"after {hold_time:.3f}s"
            )
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
            args=(f"HighPrio-{i + 1}",),
            daemon=True,
        )
        threads.append(thread)


    for i in range(3):
        thread = threading.Thread(
            target=normal_priority_thread,
            args=(f"Normal-{i + 1}",),
            daemon=True,
        )
        threads.append(thread)


    for i in range(2):
        thread = threading.Thread(
            target=low_priority_thread,
            args=(f"LowPrio-{i + 1}",),
            daemon=True,
        )
        threads.append(thread)


    stats_thread = threading.Thread(target=print_stats, daemon=True)
    threads.append(stats_thread)


    for thread in threads:
        thread.start()

    start_time = time.time()
    while time.time() - start_time < RUN_DURATION:
        time.sleep(1)


    print("\nStopping simulation (time limit reached)...")
    running = False


    with ticket_cond:
        ticket_cond.notify_all()


    time.sleep(1)


    print("\nFINAL STATISTICS:")
    print("=" * 60)
    for thread_name in sorted(access_counts.keys()):
        attempts = total_attempts[thread_name]
        successes = access_counts[thread_name]
        starvations = starvation_counts[thread_name]
        avg_wait = (total_wait_times[thread_name] / successes
                    if successes > 0 else 0.0)
        success_rate = (successes / attempts * 100) if attempts > 0 else 0
        print(
            f"{thread_name:12} | Total Attempts: {attempts:4} | "
            f"Successes: {successes:4} | Starved: {starvations:4} | "
            f"Success Rate: {success_rate:5.1f}% | "
            f"Avg wait: {avg_wait:6.3f}s"
        )

    print("\nStarvation has been removed: every thread eventually acquires")
    print("the resource thanks to the fair ticket-based locking scheme.")


if __name__ == "__main__":
    main()
