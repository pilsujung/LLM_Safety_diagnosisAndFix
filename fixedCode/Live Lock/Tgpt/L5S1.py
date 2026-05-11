import multiprocessing
import time

def polite_worker(name: str, required_resources: int, resource_pool, lock: multiprocessing.Lock):
    """
    Fixed: avoids polite livelock by introducing a 'turn' arbitration.
    Exactly one worker is allowed to accumulate when both have requested.
    """
    attempts = 0
    max_attempts = 200


    is_a = (name == "Worker-A")
    my_key = 'allocated_to_a' if is_a else 'allocated_to_b'
    other_key = 'allocated_to_b' if is_a else 'allocated_to_a'
    my_req_key = 'a_requested' if is_a else 'b_requested'
    other_req_key = 'b_requested' if is_a else 'a_requested'
    my_label = 'A' if is_a else 'B'
    other_label = 'B' if is_a else 'A'


    with lock:
        if is_a:
            resource_pool.a_required = required_resources
        else:
            resource_pool.b_required = required_resources

    while attempts < max_attempts:
        with lock:
            total_alloc = resource_pool.allocated_to_a + resource_pool.allocated_to_b
            available = resource_pool.total_resources - total_alloc

            my_resources = getattr(resource_pool, my_key)
            other_resources = getattr(resource_pool, other_key)


            setattr(resource_pool, my_req_key, True)


            my_remaining = required_resources - my_resources
            other_required = resource_pool.b_required if is_a else resource_pool.a_required
            other_remaining = other_required - other_resources


            if my_resources >= required_resources:
                print(f"{name}: Task completed with required resources!")

                setattr(resource_pool, my_req_key, False)
                if resource_pool.turn == my_label:
                    resource_pool.turn = None
                return True


            both_requested = getattr(resource_pool, other_req_key) and getattr(resource_pool, my_req_key)
            if both_requested:
                if resource_pool.turn is None:

                    if my_remaining < other_remaining or (my_remaining == other_remaining and my_label < other_label):
                        resource_pool.turn = my_label
                    else:
                        resource_pool.turn = other_label


                if resource_pool.turn != my_label:
                    if my_resources > 0:
                        print(f"{name}: Not my turn ({resource_pool.turn}'s turn). Releasing {my_resources} resources.")
                        setattr(resource_pool, my_key, 0)

                else:

                    if available > 0 and my_remaining > 0:
                        acquire_amount = min(available, my_remaining)
                        print(f"{name}: It's my turn. Acquiring {acquire_amount} resources (need {my_remaining}).")
                        setattr(resource_pool, my_key, my_resources + acquire_amount)
            else:

                if available > 0 and my_remaining > 0:
                    acquire_amount = min(available, my_remaining)
                    print(f"{name}: Acquiring {acquire_amount} resources (solo or uncontended).")
                    setattr(resource_pool, my_key, my_resources + acquire_amount)


        attempts += 1
        time.sleep(0.05)

    print(f"{name}: Failed to acquire enough resources after {attempts} attempts")

    with lock:
        if resource_pool.turn == my_label:
            resource_pool.turn = None
        setattr(resource_pool, my_req_key, False)
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
        time.sleep(0.5)


if __name__ == "__main__":
    manager = multiprocessing.Manager()
    resource_pool = manager.Namespace()
    resource_pool.total_resources = 10
    resource_pool.allocated_to_a = 0
    resource_pool.allocated_to_b = 0
    resource_pool.a_requested = False
    resource_pool.b_requested = False
    resource_pool.a_required = 0
    resource_pool.b_required = 0
    resource_pool.turn = None

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
    print(f"Final resource allocation:")
    print(f"Worker A: {resource_pool.allocated_to_a}")
    print(f"Worker B: {resource_pool.allocated_to_b}")

    if resource_pool.allocated_to_a < 6 and resource_pool.allocated_to_b < 7:
        print("Livelock detected: Workers were too polite and couldn't get enough resources!")
    else:
        print("Resource allocation completed successfully!")
