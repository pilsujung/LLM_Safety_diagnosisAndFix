import multiprocessing
import time
import random
from dataclasses import dataclass
from typing import List

@dataclass
class ResourcePool:
    total_resources: int = 10
    allocated_to_a: int = 0
    allocated_to_b: int = 0
    a_requested: bool = False
    b_requested: bool = False


def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    """
    Livelock-resistant worker:
      - Starts out 'polite' (releasing resources if the other worker wants them).
      - After a few attempts, one worker becomes 'decisive' and stops being polite.
      - Uses random backoff between attempts to break symmetry.
    """
    attempts = 0
    max_attempts = 20
    polite_threshold = 5

    while attempts < max_attempts:
        with lock:

            if resource_pool.decided_worker == "" and attempts >= polite_threshold:
                resource_pool.decided_worker = name
                print(f"{name}: Becoming decisive worker after {attempts} attempts")

            decisive_worker = resource_pool.decided_worker


            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated

            if name == "Worker-A":
                my_resources = resource_pool.allocated_to_a
                other_requested = resource_pool.b_requested
                resource_pool.a_requested = True
            else:
                my_resources = resource_pool.allocated_to_b
                other_requested = resource_pool.a_requested
                resource_pool.b_requested = True

            print(
                f"{name}: Has {my_resources}, needs {required_resources}, "
                f"available: {available}, attempts: {attempts}, "
                f"decisive_worker: {decisive_worker}"
            )


            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources! Releasing resources...")

                if name == "Worker-A":

                    resource_pool.allocated_to_a = max(
                        0, resource_pool.allocated_to_a - required_resources
                    )
                    resource_pool.a_requested = False
                    resource_pool.a_completed = True
                else:
                    resource_pool.allocated_to_b = max(
                        0, resource_pool.allocated_to_b - required_resources
                    )
                    resource_pool.b_requested = False
                    resource_pool.b_completed = True

                return True

            i_am_decisive = (decisive_worker == name)



            if other_requested and my_resources > 0 and not i_am_decisive:
                print(f"{name}: Being polite and releasing my resources...")
                if name == "Worker-A":
                    resource_pool.allocated_to_a = 0
                else:
                    resource_pool.allocated_to_b = 0


            elif available > 0:
                acquire_amount = min(available, required_resources - my_resources)
                if acquire_amount > 0:
                    print(f"{name}: Attempting to acquire {acquire_amount} resources")
                    if name == "Worker-A":
                        resource_pool.allocated_to_a += acquire_amount
                    else:
                        resource_pool.allocated_to_b += acquire_amount


        attempts += 1
        sleep_time = random.uniform(0.2, 0.5)
        print(f"{name}: Waiting {sleep_time:.2f} seconds before next attempt")
        time.sleep(sleep_time)

    print(f"{name}: Failed to acquire enough resources after {attempts} attempts")
    return False


def monitor_resources(resource_pool, lock: multiprocessing.Lock, duration: int = 10):
    start_time = time.time()
    while time.time() - start_time < duration:
        with lock:
            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            print(f"\nResource Monitor Status:")
            print(f"Total Resources: {resource_pool.total_resources}")
            print(f"Allocated to A: {resource_pool.allocated_to_a}")
            print(f"Allocated to B: {resource_pool.allocated_to_b}")
            print(f"Available: {available}")
            print(f"A requested: {resource_pool.a_requested}")
            print(f"B requested: {resource_pool.b_requested}")
            print(f"A completed: {resource_pool.a_completed}")
            print(f"B completed: {resource_pool.b_completed}")
            print(f"Decided worker: {resource_pool.decided_worker}")
        time.sleep(1)


if __name__ == "__main__":

    manager = multiprocessing.Manager()
    resource_pool = manager.Namespace()


    resource_pool.total_resources = 10
    resource_pool.allocated_to_a = 0
    resource_pool.allocated_to_b = 0
    resource_pool.a_requested = False
    resource_pool.b_requested = False
    resource_pool.a_completed = False
    resource_pool.b_completed = False
    resource_pool.decided_worker = ""

    lock = multiprocessing.Lock()


    worker_a = multiprocessing.Process(
        target=polite_worker,
        args=("Worker-A", 6, resource_pool, lock)
    )

    worker_b = multiprocessing.Process(
        target=polite_worker,
        args=("Worker-B", 7, resource_pool, lock)
    )

    monitor = multiprocessing.Process(
        target=monitor_resources,
        args=(resource_pool, lock)
    )

    print("Starting resource sharing simulation...")


    monitor.start()
    worker_a.start()
    worker_b.start()


    worker_a.join()
    worker_b.join()


    monitor.terminate()
    monitor.join()

    print("\nSimulation completed!")
    print("Final resource state (remaining allocations):")
    print(f"Worker A allocation: {resource_pool.allocated_to_a}")
    print(f"Worker B allocation: {resource_pool.allocated_to_b}")
    print(f"Worker A completed: {resource_pool.a_completed}")
    print(f"Worker B completed: {resource_pool.b_completed}")

    if resource_pool.a_completed and resource_pool.b_completed:
        print("Both workers completed their tasks without livelock.")
    elif not resource_pool.a_completed and not resource_pool.b_completed:
        print("Livelock/starvation detected: neither worker could finish.")
    else:
        print("Partial success: one worker finished, the other could not acquire enough resources.")
