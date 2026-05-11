import threading
import time
import random


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50
STARVATION_THRESHOLD = 1.0


class FairLock:
    """
    Simple FIFO (fair) lock using a ticket algorithm.

    - Each thread gets a monotonically increasing ticket number.
    - Only the thread whose ticket is currently being served may enter.
    - This guarantees that threads acquire the lock in the order
      they requested it (no starvation).
    """
    def __init__(self):
        self._mutex = threading.Lock()
        self._cond = threading.Condition(self._mutex)
        self._next_ticket = 0
        self._serving = 0

    def acquire(self):
        with self._mutex:
            my_ticket = self._next_ticket
            self._next_ticket += 1

            while my_ticket != self._serving:
                self._cond.wait()

    def release(self):
        with self._mutex:
            self._serving += 1
            self._cond.notify_all()



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
                f"Successes: {successes:4} | Starved: {starvations:4} | "
                f"Success Rate: {success_rate:5.1f}%"
            )
        print("=" * 50 + "\n")


def high_priority_thread(thread_name):
    """
    High priority thread that holds the resource for longer periods.
    It still uses the same fair lock, so it can no longer starve others.
    """
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        wait_start = time.time()
        resource_lock.acquire()
        wait_time = time.time() - wait_start

        try:
            access_counts[thread_name] += 1
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
            resource_lock.release()


        time.sleep(random.uniform(0.01, 0.05))


def normal_priority_thread(thread_name):
    """
    Normal priority thread.

    *** FIXED ***
    Previously: used acquire(blocking=False) and counted each failure
    as starvation, so high-priority threads could monopolize the lock.

    Now: uses the same fair, blocking acquire as everyone else, so
    it always eventually acquires the resource.
    """
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        wait_start = time.time()
        resource_lock.acquire()
        wait_time = time.time() - wait_start


        if wait_time > STARVATION_THRESHOLD:
            starvation_counts[thread_name] += 1

        try:
            access_counts[thread_name] += 1
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
            resource_lock.release()


        time.sleep(random.uniform(0.02, 0.08))


def low_priority_thread(thread_name):
    """
    Low priority thread.

    *** FIXED ***
    Same idea as the normal thread: it now blocks fairly on the lock,
    so it cannot be permanently starved.
    """
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        total_attempts[thread_name] += 1

        wait_start = time.time()
        resource_lock.acquire()
        wait_time = time.time() - wait_start

        if wait_time > STARVATION_THRESHOLD:
            starvation_counts[thread_name] += 1

        try:
            access_counts[thread_name] += 1
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
            resource_lock.release()


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
        t = threading.Thread(
            target=high_priority_thread,
            args=(f"HighPrio-{i+1}",),
            daemon=True,
        )
        threads.append(t)


    for i in range(3):
        t = threading.Thread(
            target=normal_priority_thread,
            args=(f"Normal-{i+1}",),
            daemon=True,
        )
        threads.append(t)


    for i in range(2):
        t = threading.Thread(
            target=low_priority_thread,
            args=(f"LowPrio-{i+1}",),
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
            f"{thread_name:12} | Total Attempts: {attempts:4} | "
            f"Successes: {successes:4} | Starved: {starvations:4} | "
            f"Success Rate: {success_rate:5.1f}%"
        )

    print("\nWith the fair lock, each thread eventually acquires the resource.")
    print("High-priority threads still hold it longer, but they can no longer")
    print("starve the normal and low-priority threads.")


if __name__ == "__main__":
    main()
