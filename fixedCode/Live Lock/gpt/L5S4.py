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
    a_done: bool = False
    b_done: bool = False


def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    attempts = 0
    max_attempts = 30

    while attempts < max_attempts:
        with lock:

            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated

            if name == "Worker-A":
                my_resources = resource_pool.allocated_to_a
                other_resources = resource_pool.allocated_to_b
                other_requested = resource_pool.b_requested
                resource_pool.a_requested = True
                done_attr = "a_done"
            else:
                my_resources = resource_pool.allocated_to_b
                other_resources = resource_pool.allocated_to_a
                other_requested = resource_pool.a_requested
                resource_pool.b_requested = True
                done_attr = "b_done"

            print(
                f"{name}: Has {my_resources}, needs {required_resources}, "
                f"available: {available}"
            )


            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources!")
                setattr(resource_pool, done_attr, True)


                if name == "Worker-A":
                    resource_pool.allocated_to_a = 0
                else:
                    resource_pool.allocated_to_b = 0

                return True




            if other_requested and my_resources > 0 and my_resources <= other_resources:
                print(f"{name}: Yielding to other worker, releasing my {my_resources} resources...")
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

        time.sleep(random.uniform(0.1, 0.5))

    print(f"{name}: Failed to acquire enough resources after {attempts} attempts")
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
            print(f"A done: {resource_pool.a_done}")
            print(f"B done: {resource_pool.b_done}")
        time.sleep(1)


if __name__ == "__main__":

    manager = multiprocessing.Manager()
    initial_state = ResourcePool()
    resource_pool = manager.Namespace(**initial_state.__dict__)

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
    print("Final status:")
    print(f"Worker A done: {resource_pool.a_done}")
    print(f"Worker B done: {resource_pool.b_done}")

    if resource_pool.a_done and resource_pool.b_done:
        print("Both workers completed successfully!")
    elif resource_pool.a_done or resource_pool.b_done:
        print("One worker completed; the other could not get enough resources.")
    else:
        print("Livelock / starvation detected: no worker finished.")
