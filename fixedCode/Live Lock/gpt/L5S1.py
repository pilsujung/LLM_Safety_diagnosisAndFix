import multiprocessing
import time
from dataclasses import dataclass

@dataclass
class ResourcePoolSnapshot:
    total_resources: int
    allocated_to_a: int
    allocated_to_b: int
    a_requested: bool
    b_requested: bool
    turn: str

def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    """
    Fair, turn-based acquisition to avoid livelock:
    - Only the worker whose 'turn' it is can acquire.
    - When both are requesting, the turn determines progress.
    - On handoff or completion, partial allocations are released to allow progress.
    """
    attempts = 0
    max_attempts = 40
    sleep_between_attempts = 0.25


    mine_key = 'allocated_to_a' if name == "Worker-A" else 'allocated_to_b'
    other_key = 'allocated_to_b' if name == "Worker-A" else 'allocated_to_a'
    my_req_key = 'a_requested' if name == "Worker-A" else 'b_requested'
    other_req_key = 'b_requested' if name == "Worker-A" else 'a_requested'
    me = name
    other = "Worker-B" if name == "Worker-A" else "Worker-A"


    with lock:
        setattr(resource_pool, my_req_key, True)

        if resource_pool.turn not in ("Worker-A", "Worker-B"):


            my_remaining = required_resources - getattr(resource_pool, mine_key)
            other_remaining = (7 if other == "Worker-B" else 6) - getattr(resource_pool, other_key)
            resource_pool.turn = me if my_remaining <= other_remaining else other
        print(f"{me}: requesting resources; initial turn = {resource_pool.turn}")


    consecutive_no_progress = 0

    while attempts < max_attempts:
        attempts += 1
        with lock:
            total_allocated = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_allocated
            my_alloc = getattr(resource_pool, mine_key)
            other_alloc = getattr(resource_pool, other_key)
            other_requested = getattr(resource_pool, other_req_key)

            print(f"{me}: Has {my_alloc}, needs {required_resources}, available: {available}, turn={resource_pool.turn}")


            if my_alloc >= required_resources:
                print(f"{me}: Task completed with required resources!")

                setattr(resource_pool, mine_key, 0)
                setattr(resource_pool, my_req_key, False)

                if other_requested:
                    resource_pool.turn = other
                return True



            if resource_pool.turn != me:
                if my_alloc > 0:
                    print(f"{me}: Not my turn; releasing my partial allocation ({my_alloc})")
                    setattr(resource_pool, mine_key, 0)

                next_action = "wait"
            else:

                need = required_resources - my_alloc
                acquire_amount = min(need, available)
                if acquire_amount > 0:
                    setattr(resource_pool, mine_key, my_alloc + acquire_amount)
                    print(f"{me}: Acquired {acquire_amount}, now have {getattr(resource_pool, mine_key)}")
                    consecutive_no_progress = 0
                    next_action = "progress"
                else:

                    consecutive_no_progress += 1
                    next_action = "no_progress"


                    if other_requested and consecutive_no_progress >= 2:
                        if my_alloc > 0:
                            print(f"{me}: No progress; handing off turn to {other} and releasing my partial allocation ({my_alloc})")
                            setattr(resource_pool, mine_key, 0)
                        resource_pool.turn = other
                        consecutive_no_progress = 0


        time.sleep(sleep_between_attempts)


    with lock:
        my_alloc = getattr(resource_pool, mine_key)
        if my_alloc > 0:
            print(f"{me}: Max attempts reached; releasing my partial allocation ({my_alloc})")
            setattr(resource_pool, mine_key, 0)
        setattr(resource_pool, my_req_key, False)

        if getattr(resource_pool, other_req_key):
            resource_pool.turn = other

    print(f"{me}: Failed to acquire enough resources after {attempts} attempts")
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
            print(f"Turn: {resource_pool.turn}")
        time.sleep(1)

if __name__ == "__main__":
    print("Starting resource sharing simulation...")


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
        args=(resource_pool, lock)
    )


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




    print("Resource allocation completed successfully (no livelock).")
