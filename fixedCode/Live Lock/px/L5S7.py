import multiprocessing
import time
import random

class ResourcePool:
    def __init__(self):
        self.total_resources = 10
        self.allocated_to_a = 0
        self.allocated_to_b = 0
        self.a_requested = False
        self.b_requested = False

def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    attempts = 0
    max_attempts = 20
    priority = 1.0 if name == "Worker-A" else 0.6

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

            print(f"{name}: Has {my_resources}, needs {required_resources}, available: {available}")


            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources!")

                if name == "Worker-A":
                    resource_pool.a_requested = False
                else:
                    resource_pool.b_requested = False
                return True


            if other_requested and my_resources > 0:
                if random.random() < priority:
                    print(f"{name}: Priority hold - keeping resources despite other request")
                else:
                    print(f"{name}: Yielding resources to other worker")
                    if name == "Worker-A":
                        resource_pool.allocated_to_a = 0
                    else:
                        resource_pool.allocated_to_b = 0

            elif available > 0:
                acquire_amount = min(available, required_resources - my_resources)
                print(f"{name}: Acquiring {acquire_amount} resources")
                if name == "Worker-A":
                    resource_pool.allocated_to_a += acquire_amount
                else:
                    resource_pool.allocated_to_b += acquire_amount


        time.sleep(random.uniform(0.2, 0.6))
        attempts += 1

    print(f"{name}: Max attempts ({max_attempts}) reached, giving up")

    with lock:
        if name == "Worker-A":
            resource_pool.a_requested = False
        else:
            resource_pool.b_requested = False
    return False

def monitor_resources(resource_pool, lock: multiprocessing.Lock, duration: int = 15):
    start_time = time.time()
    while time.time() - start_time < duration:
        with lock:
            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            print(f"\n=== MONITOR: A={resource_pool.allocated_to_a}/6, B={resource_pool.allocated_to_b}/7, "
                  f"Avail={available}, ReqA={resource_pool.a_requested}, ReqB={resource_pool.b_requested} ===")
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
        args=(resource_pool, lock, 20)
    )

    print("Starting fixed resource sharing simulation...")
    print("Key fixes: Priority system + random backoff + proper flag reset")


    monitor.start()
    worker_a.start()
    worker_b.start()


    worker_a.join()
    worker_b.join()


    monitor.terminate()
    monitor.join()

    print("\n" + "="*60)
    print("SIMULATION COMPLETED!")
    print(f"Final allocation - Worker A: {resource_pool.allocated_to_a}/6, Worker B: {resource_pool.allocated_to_b}/7")
    
    a_success = resource_pool.allocated_to_a >= 6
    b_success = resource_pool.allocated_to_b >= 7
    
    if a_success or b_success:
        print("✓ Livelock resolved! At least one worker succeeded.")
        if a_success:
            print("✓ Worker A completed successfully")
        if b_success:
            print("✓ Worker B completed successfully")
    else:
        print("✗ Both workers failed - check resource constraints (6+7 > 10)")
