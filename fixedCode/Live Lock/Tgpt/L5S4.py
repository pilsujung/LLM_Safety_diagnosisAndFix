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
    max_attempts = 12

    gave_up_for_other = False
    
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
                  f"available: {available}, other_requested={other_requested}, "
                  f"gave_up_for_other={gave_up_for_other}")
            

            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources!")
                
                if name == "Worker-A":

                    resource_pool.allocated_to_a -= required_resources
                    resource_pool.a_requested = False
                    resource_pool.a_done = True
                else:
                    resource_pool.allocated_to_b -= required_resources
                    resource_pool.b_requested = False
                    resource_pool.b_done = True
                return True
            


            if other_requested and my_resources > 0 and not gave_up_for_other:
                print(f"{name}: Other worker needs resources, releasing mine this time...")
                if name == "Worker-A":
                    resource_pool.allocated_to_a = 0
                else:
                    resource_pool.allocated_to_b = 0
                gave_up_for_other = True

            elif available > 0:
                acquire_amount = min(available, required_resources - my_resources)
                if acquire_amount > 0:
                    print(f"{name}: Attempting to acquire {acquire_amount} resources")
                    if name == "Worker-A":
                        resource_pool.allocated_to_a += acquire_amount
                    else:
                        resource_pool.allocated_to_b += acquire_amount

                gave_up_for_other = False
            else:

                gave_up_for_other = False
                    
        attempts += 1

        time.sleep(random.uniform(0.3, 0.7))
        
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
        time.sleep(1)

if __name__ == "__main__":

    manager = multiprocessing.Manager()
    resource_pool = manager.Namespace()
    resource_pool.total_resources = 10
    resource_pool.allocated_to_a = 0
    resource_pool.allocated_to_b = 0
    resource_pool.a_requested = False
    resource_pool.b_requested = False

    resource_pool.a_done = False
    resource_pool.b_done = False
    
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
    print(f"A done: {resource_pool.a_done}")
    print(f"B done: {resource_pool.b_done}")
    
    if resource_pool.a_done and resource_pool.b_done:
        print("Resource allocation completed successfully!")
    elif not resource_pool.a_done and not resource_pool.b_done:
        print("Livelock/starvation: both workers failed to complete.")
    else:
        print("Partial success: one worker completed, the other did not.")
