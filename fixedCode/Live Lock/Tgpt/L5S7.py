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
    priority: str = "Worker-A"


def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    attempts = 0
    max_attempts = 12

    my_attr = "allocated_to_a" if name == "Worker-A" else "allocated_to_b"
    other_attr = "allocated_to_b" if name == "Worker-A" else "allocated_to_a"
    my_req_attr = "a_requested" if name == "Worker-A" else "b_requested"
    other_req_attr = "b_requested" if name == "Worker-A" else "a_requested"
    other_name = "Worker-B" if name == "Worker-A" else "Worker-A"

    while attempts < max_attempts:
        with lock:

            setattr(resource_pool, my_req_attr, True)

            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            my_resources = getattr(resource_pool, my_attr)
            other_requested = getattr(resource_pool, other_req_attr)

            print(f"{name}: Has {my_resources}, needs {required_resources}, "
                  f"available: {available}, priority: {resource_pool.priority}")


            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources! Releasing and handing over.")

                pass


        with lock:
            my_resources = getattr(resource_pool, my_attr)
            if my_resources >= required_resources:

                setattr(resource_pool, my_attr, 0)

                setattr(resource_pool, my_req_attr, False)

                resource_pool.priority = other_name
                return True




            can_acquire = (available > 0) and ((not other_requested) or (resource_pool.priority == name))
            if can_acquire:
                acquire_amount = min(available, required_resources - my_resources)
                if acquire_amount > 0:
                    print(f"{name}: Acquiring {acquire_amount} resources")
                    setattr(resource_pool, my_attr, my_resources + acquire_amount)


            else:


                print(f"{name}: Waiting (other has priority or none available).")

        attempts += 1
        time.sleep(0.5)


    with lock:
        setattr(resource_pool, my_req_attr, False)
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
            print(f"Priority: {resource_pool.priority}")
        time.sleep(1)


if __name__ == "__main__":

    manager = multiprocessing.Manager()
    resource_pool = manager.Namespace()
    resource_pool.total_resources = 10
    resource_pool.allocated_to_a = 0
    resource_pool.allocated_to_b = 0
    resource_pool.a_requested = False
    resource_pool.b_requested = False
    resource_pool.priority = "Worker-A"

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
