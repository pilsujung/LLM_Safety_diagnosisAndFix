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
    attempts = 0
    max_attempts = 20
    
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
                  f"available: {available}")

            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources!")
                return True


            if other_requested and my_resources > 0:
                print(f"{name}: Other worker needs resources, releasing mine...")
                if name == "Worker-A":
                    resource_pool.allocated_to_a = 0
                else:
                    resource_pool.allocated_to_b = 0

                wait_time = random.randint(50, 200)
                print(f"{name}: Waiting {wait_time}ms before retry...")
                lock.release()
                time.sleep(wait_time / 1000.0)
                attempts += 1
                continue


            elif available > 0:
                acquire_amount = min(available, required_resources - my_resources)
                print(f"{name}: Attempting to acquire {acquire_amount} resources")
                if name == "Worker-A":
                    resource_pool.allocated_to_a += acquire_amount
                else:
                    resource_pool.allocated_to_b += acquire_amount


        wait_time = random.randint(50, 200)
        time.sleep(wait_time / 1000.0)
        attempts += 1

    print(f"{name}: Failed to acquire enough resources after {attempts} attempts")
    return False

def monitor_resources(resource_pool, lock: multiprocessing.Lock, duration: int = 15):
    start_time = time.time()
    while time.time() - start_time < duration:
        with lock:
            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            print(f"\n--- Resource Monitor ({time.time()-start_time:.1f}s) ---")
            print(f"Total: {resource_pool.total_resources} | A: {resource_pool.allocated_to_a} | B: {resource_pool.allocated_to_b} | Free: {available}")
            print(f"A req: {resource_pool.a_requested} | B req: {resource_pool.b_requested}")
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

    print("Starting fixed livelock resource sharing simulation...")
    

    monitor.start()
    worker_a.start()
    worker_b.start()


    worker_a.join()
    worker_b.join()


    monitor.terminate()
    monitor.join()

    print("\n=== Simulation completed! ===")
    print(f"Final resource allocation:")
    print(f"Worker A: {resource_pool.allocated_to_a}/6")
    print(f"Worker B: {resource_pool.allocated_to_b}/7")
    
    a_success = resource_pool.allocated_to_a >= 6
    b_success = resource_pool.allocated_to_b >= 7
    if a_success or b_success:
        print("✓ Livelock resolved: At least one worker succeeded!")
    else:
        print("✗ Both workers still failed (rare with random timing)")
