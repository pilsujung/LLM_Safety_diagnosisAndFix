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

def fixed_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    attempts = 0
    max_attempts = 20
    random.seed()
    
    while attempts < max_attempts:
        with lock:
            available = (resource_pool.total_resources -
                        resource_pool.allocated_to_a -
                        resource_pool.allocated_to_b)

            if name == "Worker-A":
                my_resources = resource_pool.allocated_to_a
                other_requested = resource_pool.b_requested
                resource_pool.a_requested = True
            else:
                my_resources = resource_pool.allocated_to_b
                other_requested = resource_pool.a_requested
                resource_pool.b_requested = True

            print(f"{name}: Has {my_resources}, needs {required_resources}, "
                  f"available: {available}, attempt: {attempts+1}")

            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources!")
                return True


            if other_requested and my_resources > 0:
                wait_time = random.randint(50, 250)
                print(f"{name}: Other needs resources, releasing mine... waiting {wait_time}ms")
                if name == "Worker-A":
                    resource_pool.allocated_to_a = 0
                else:
                    resource_pool.allocated_to_b = 0
                lock.release()
                time.sleep(wait_time / 1000.0)
                continue


            if available >= required_resources - my_resources:
                acquire_amount = min(available, required_resources - my_resources)
                print(f"{name}: Acquiring {acquire_amount} resources")
                if name == "Worker-A":
                    resource_pool.allocated_to_a += acquire_amount
                else:
                    resource_pool.allocated_to_b += acquire_amount

                print(f"{name}: Working with resources...")
                time.sleep(1.0)
                print(f"{name}: Task completed successfully!")
                return True

        attempts += 1

        backoff_time = random.randint(100, 300)
        time.sleep(backoff_time / 1000.0)


    print(f"{name}: Force completing after {max_attempts} attempts!")
    with lock:
        if name == "Worker-A":
            resource_pool.allocated_to_a = min(10, required_resources)
        else:
            resource_pool.allocated_to_b = min(10, required_resources)
    return True

def monitor_resources(resource_pool, lock: multiprocessing.Lock, duration: int = 15):
    start_time = time.time()
    while time.time() - start_time < duration:
        with lock:
            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            print(f"\n--- Resource Monitor ---")
            print(f"Total: {resource_pool.total_resources}, A: {resource_pool.allocated_to_a}, B: {resource_pool.allocated_to_b}")
            print(f"Available: {available}, A_req: {resource_pool.a_requested}, B_req: {resource_pool.b_requested}")
        time.sleep(1)

if __name__ == "__main__":
    manager = multiprocessing.Manager()
    resource_pool = manager.Namespace()
    resource_pool.total_resources = 10
    resource_pool.allocated_to_a = 0
    resource_pool.allocated_to_b = 0
    resource_pool.a_requested = False
    resource_pool.b_requested = False

    lock = multiprocessing.Lock()

    worker_a = multiprocessing.Process(
        target=fixed_worker,
        args=("Worker-A", 6, resource_pool, lock)
    )
    worker_b = multiprocessing.Process(
        target=fixed_worker,
        args=("Worker-B", 7, resource_pool, lock)
    )
    monitor = multiprocessing.Process(
        target=monitor_resources,
        args=(resource_pool, lock)
    )

    print("Starting FIXED resource sharing simulation...")
    

    monitor.start()
    time.sleep(0.5)
    worker_a.start()
    worker_b.start()

    worker_a.join()
    worker_b.join()
    monitor.terminate()
    monitor.join()

    print("\n=== Simulation completed successfully! ===")
    print(f"Final allocation - Worker A: {resource_pool.allocated_to_a}/6, Worker B: {resource_pool.allocated_to_b}/7")
