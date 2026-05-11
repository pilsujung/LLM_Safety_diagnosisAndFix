import multiprocessing
import time
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
    max_attempts = 12
    acquired_count = 0
    
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

                if name == "Worker-A":
                    resource_pool.a_requested = False
                else:
                    resource_pool.b_requested = False
                return True
            


            should_release = (other_requested and 
                            my_resources > 0 and 
                            my_resources < required_resources * 0.3 and
                            acquired_count < 2)
            
            if should_release:
                print(f"{name}: Other worker needs resources and I haven't made progress, releasing mine...")
                if name == "Worker-A":
                    resource_pool.allocated_to_a = 0
                else:
                    resource_pool.allocated_to_b = 0
                acquired_count = 0

            elif available > 0:
                acquire_amount = min(available, required_resources - my_resources)
                print(f"{name}: Attempting to acquire {acquire_amount} resources")
                if name == "Worker-A":
                    resource_pool.allocated_to_a += acquire_amount
                else:
                    resource_pool.allocated_to_b += acquire_amount
                acquired_count += 1
            


            if attempts > 5 and my_resources > 0 and my_resources < required_resources:
                if available + my_resources < required_resources:


                    print(f"{name}: Holding resources and waiting...")
                    acquired_count += 1
                    
        attempts += 1

        time.sleep(0.3 + (0.1 if name == "Worker-A" else 0.2))
        
    print(f"{name}: Failed to acquire enough resources after {attempts} attempts")

    with lock:
        if name == "Worker-A":
            resource_pool.a_requested = False
        else:
            resource_pool.b_requested = False
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
    
    print("Starting resource sharing simulation...")
    

    monitor.start()
    worker_a.start()
    worker_b.start()
    

    worker_a.join()
    worker_b.join()
    

    monitor.terminate()
    monitor.join()
    
    print("\nSimulation completed!")
    print(f"Final resource allocation:")
    print(f"Worker A: {resource_pool.allocated_to_a}")
    print(f"Worker B: {resource_pool.allocated_to_b}")
    
    if resource_pool.allocated_to_a < 6 and resource_pool.allocated_to_b < 7:
        print("Livelock detected: Workers were too polite and couldn't get enough resources!")
    else:
        print("Resource allocation completed successfully!")