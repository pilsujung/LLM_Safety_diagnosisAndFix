import threading
import time
import random


RUN_DURATION = 10
MAX_ATTEMPTS_PER_THREAD = 50

class FairLock:
    """
    A simple fair (FIFO) lock implementation.
    Threads acquire the lock in the order they requested it,
    similar to Java's ReentrantLock(true).
    """
    def __init__(self):
        self._cond = threading.Condition()
        self._locked = False
        self._next_ticket = 0
        self._now_serving = 0

    def acquire(self):
        with self._cond:
            my_ticket = self._next_ticket
            self._next_ticket += 1




            while self._locked or my_ticket != self._now_serving:
                self._cond.wait()

            self._locked = True

    def release(self):
        with self._cond:
            self._locked = False
            self._now_serving += 1
            self._cond.notify_all()


    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


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
    """High priority thread that holds the resource for longer periods"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        request_time = time.time()
        total_attempts[thread_name] += 1

        resource_lock.acquire()
        wait_time = time.time() - request_time
        try:
            access_counts[thread_name] += 1
            print(f"[HIGH PRIORITY] {thread_name} acquired the resource "
                  f"after waiting {wait_time:.3f}s")


            hold_time = random.uniform(0.1, 0.3)
            time.sleep(hold_time)

            print(f"[HIGH PRIORITY] {thread_name} releasing resource "
                  f"after {hold_time:.3f}s")
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.01, 0.05))

def normal_priority_thread(thread_name):
    """Normal priority thread that acquires the resource fairly (blocking)"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        request_time = time.time()
        total_attempts[thread_name] += 1


        resource_lock.acquire()
        wait_time = time.time() - request_time
        try:
            access_counts[thread_name] += 1
            print(f"[NORMAL] {thread_name} acquired the resource "
                  f"after waiting {wait_time:.3f}s")


            hold_time = random.uniform(0.02, 0.08)
            time.sleep(hold_time)

            print(f"[NORMAL] {thread_name} releasing resource "
                  f"after {hold_time:.3f}s")
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.02, 0.08))

def low_priority_thread(thread_name):
    """Low priority thread that also acquires the resource fairly (blocking)"""
    initialize_stats(thread_name)

    while running and total_attempts[thread_name] < MAX_ATTEMPTS_PER_THREAD:
        request_time = time.time()
        total_attempts[thread_name] += 1


        resource_lock.acquire()
        wait_time = time.time() - request_time
        try:
            access_counts[thread_name] += 1
            print(f"[LOW] {thread_name} acquired the resource "
                  f"after waiting {wait_time:.3f}s")


            hold_time = random.uniform(0.01, 0.03)
            time.sleep(hold_time)

            print(f"[LOW] {thread_name} releasing resource "
                  f"after {hold_time:.3f}s")
        finally:
            resource_lock.release()


        time.sleep(random.uniform(0.05, 0.15))

def main():
    """Main function to create and manage threads"""
    global running

    print("Starting Thread Starvation Demonstration (starvation-free version)")
    print(f"Simulation will run for about {RUN_DURATION} seconds "
          f"or up to {MAX_ATTEMPTS_PER_THREAD} attempts per thread.\n")


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
        print(f"{thread_name:12} | Total Attempts: {attempts:4} | "
              f"Successes: {successes:4} | Starved: {starvations:4} | "
              f"Success Rate: {success_rate:5.1f}%")

    print("\nIn this fixed version, all threads acquire the fair lock using "
          "a blocking call, similar to Java's ReentrantLock(true).")
    print("As a result, no thread is permanently starved; even lower-priority "
          "threads eventually access the shared resource.")

if __name__ == "__main__":
    main()
