import multiprocessing
import time
from dataclasses import dataclass

@dataclass
class ResourcePool:
    total_resources: int = 10
    allocated_to_a: int = 0
    allocated_to_b: int = 0
    a_requested: bool = False
    b_requested: bool = False
    turn: str = "Worker-A"

def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock, cond: multiprocessing.Condition):

    with cond:
        if name == "Worker-A":
            resource_pool.a_requested = True
        else:
            resource_pool.b_requested = True

        while True:
            available = (resource_pool.total_resources -
                         resource_pool.allocated_to_a -
                         resource_pool.allocated_to_b)

            other_requested = resource_pool.b_requested if name == "Worker-A" else resource_pool.a_requested
            my_alloc = resource_pool.allocated_to_a if name == "Worker-A" else resource_pool.allocated_to_b


            if my_alloc >= required_resources:
                break


            can_grant = (
                available >= required_resources and
                (
                    not other_requested or
                    resource_pool.turn == name
                )
            )

            if can_grant:
                if name == "Worker-A":
                    resource_pool.allocated_to_a = required_resources
                else:
                    resource_pool.allocated_to_b = required_resources
                print(f"{name}: Granted {required_resources} resources atomically (available was {available}).")
                break


            cond.wait(timeout=0.1)


    print(f"{name}: Working...")
    time.sleep(1.0)


    with cond:
        if name == "Worker-A":
            resource_pool.allocated_to_a = 0
            resource_pool.a_requested = False
            resource_pool.turn = "Worker-B"
        else:
            resource_pool.allocated_to_b = 0
            resource_pool.b_requested = False
            resource_pool.turn = "Worker-A"
        print(f"{name}: Work done. Released resources. Next turn: {resource_pool.turn}")
        cond.notify_all()

def monitor_resources(resource_pool, lock: multiprocessing.Lock, cond: multiprocessing.Condition, duration: int = 10):
    start_time = time.time()
    while time.time() - start_time < duration:
        with cond:
            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            print(f"""
Resource Monitor Status:
  Total Resources: {resource_pool.total_resources}
  Allocated to A: {resource_pool.allocated_to_a}
  Allocated to B: {resource_pool.allocated_to_b}
  Available:      {available}
  A requested:    {resource_pool.a_requested}
  B requested:    {resource_pool.b_requested}
  Turn:           {resource_pool.turn}
""")
        time.sleep(0.5)

if __name__ == "__main__":

    manager = multiprocessing.Manager()
    resource_pool = manager.Namespace()
    resource_pool.total_resources = 10
    resource_pool.allocated_to_a = 0
    resource_pool.allocated_to_b = 0
    resource_pool.a_requested = False
    resource_pool.b_requested = False
    resource_pool.turn = "Worker-A"


    lock = multiprocessing.Lock()
    cond = multiprocessing.Condition(lock)


    worker_a = multiprocessing.Process(
        target=polite_worker,
        args=("Worker-A", 6, resource_pool, lock, cond)
    )
    worker_b = multiprocessing.Process(
        target=polite_worker,
        args=("Worker-B", 7, resource_pool, lock, cond)
    )
    monitor = multiprocessing.Process(
        target=monitor_resources,
        args=(resource_pool, lock, cond, 8)
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
    print(f"Final resource allocation at rest:")
    print(f"Worker A: {resource_pool.allocated_to_a}")
    print(f"Worker B: {resource_pool.allocated_to_b}")
    print("No livelock: both workers completed in turn with atomic allocation and fair tie-breaking.")
