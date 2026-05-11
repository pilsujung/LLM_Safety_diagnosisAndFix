import multiprocessing
import time
import random
from dataclasses import dataclass

@dataclass
class ResourcePool:
    total_resources: int = 10
    allocated_to_a: int = 0
    allocated_to_b: int = 0
    a_requested: bool = False
    b_requested: bool = False
    turn: str = ""

def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    """
    Livelock-free version: uses a shared 'turn' to grant acquisition rights.
    Only the worker whose turn it is may acquire resources; the other must release and wait.
    """
    attempts = 0
    max_attempts = 100

    def get_my_alloc():
        return resource_pool.allocated_to_a if name == "Worker-A" else resource_pool.allocated_to_b

    def set_my_alloc(val):
        if name == "Worker-A":
            resource_pool.allocated_to_a = val
        else:
            resource_pool.allocated_to_b = val

    def get_other_name():
        return "Worker-B" if name == "Worker-A" else "Worker-A"

    def other_requested():
        return resource_pool.b_requested if name == "Worker-A" else resource_pool.a_requested

    def set_requested(flag: bool):
        if name == "Worker-A":
            resource_pool.a_requested = flag
        else:
            resource_pool.b_requested = flag

    while attempts < max_attempts:
        with lock:

            set_requested(True)


            my_alloc = get_my_alloc()
            other = get_other_name()
            total_alloc = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_alloc

            print(f"{name}: has {my_alloc}, needs {required_resources}, available {available}, turn={resource_pool.turn}")


            if my_alloc >= required_resources:
                print(f"{name}: Task completed ✅")

                set_requested(False)

                if other_requested():
                    resource_pool.turn = other
                else:
                    resource_pool.turn = ""
                return True


            if resource_pool.turn == "":


                resource_pool.turn = name
                print(f"{name}: claiming turn")
            elif resource_pool.turn != name:

                if my_alloc > 0:
                    print(f"{name}: not my turn; releasing {my_alloc}")
                    set_my_alloc(0)

                pass
            else:

                need = required_resources - my_alloc
                if available > 0 and need > 0:
                    acquire = min(available, need)
                    print(f"{name}: acquiring {acquire}")
                    set_my_alloc(my_alloc + acquire)


        attempts += 1
        time.sleep(0.05 + random.random() * 0.1)


    with lock:
        print(f"{name}: Failed to acquire enough resources after {attempts} attempts")
        set_requested(False)
        if resource_pool.turn == name:
            resource_pool.turn = get_other_name() if other_requested() else ""
    return False

def monitor_resources(resource_pool, lock: multiprocessing.Lock, duration: int = 10):
    start_time = time.time()
    while time.time() - start_time < duration:
        with lock:
            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            print("\nResource Monitor Status:")
            print(f"Total Resources: {resource_pool.total_resources}")
            print(f"Allocated to A: {resource_pool.allocated_to_a}")
            print(f"Allocated to B: {resource_pool.allocated_to_b}")
            print(f"Available: {available}")
            print(f"A requested: {resource_pool.a_requested}")
            print(f"B requested: {resource_pool.b_requested}")
            print(f"Turn: {resource_pool.turn}")
        time.sleep(1)

if __name__ == "__main__":
    manager = multiprocessing.Manager()
    resource_pool = manager.Namespace()
    resource_pool.total_resources = 10
    resource_pool.allocated_to_a = 0
    resource_pool.allocated_to_b = 0
    resource_pool.a_requested = False
    resource_pool.b_requested = False
    resource_pool.turn = ""

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
        args=(resource_pool, lock, 15)
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
    print("Final resource allocation:")
    print(f"Worker A: {resource_pool.allocated_to_a}")
    print(f"Worker B: {resource_pool.allocated_to_b}")

    if resource_pool.allocated_to_a < 6 and resource_pool.allocated_to_b < 7:
        print("Livelock detected: Workers could not get enough resources.")
    else:
        print("Resource allocation completed successfully!")
