import threading
import time
import random

class Resource:
    def __init__(self, name, resource_id):
        self.name = name
        self.resource_id = resource_id
        self.lock = threading.Lock()
        self.owner = None
    
    def acquire(self, owner):
        """Try to acquire the resource for the given owner"""
        result = self.lock.acquire(blocking=False)
        if result:
            self.owner = owner
            print(f"{time.time():.2f}: {owner} acquired {self.name}")
        return result
    
    def acquire_blocking(self, owner):
        """Acquire the resource with blocking (for ordered acquisition)"""
        self.lock.acquire()
        self.owner = owner
        print(f"{time.time():.2f}: {owner} acquired {self.name}")
    
    def release(self):
        """Release the resource"""
        previous_owner = self.owner
        self.owner = None
        self.lock.release()
        print(f"{time.time():.2f}: {previous_owner} released {self.name}")


class Worker(threading.Thread):
    def __init__(self, name, first_resource, second_resource):
        super().__init__(name=name)
        self.first_resource = first_resource
        self.second_resource = second_resource
        self.active = True
        self.last_progress = time.time()
        self.work_count = 0
    
    def run(self):
        while self.active and self.work_count < 5:

            self.acquire_resources_ordered()
            

            time.sleep(random.uniform(0.1, 0.2))
    
    def acquire_resources_ordered(self):
        """Acquire resources in a consistent order based on resource ID"""

        resources = [self.first_resource, self.second_resource]
        resources.sort(key=lambda r: r.resource_id)
        
        first_to_acquire = resources[0]
        second_to_acquire = resources[1]
        
        print(f"{time.time():.2f}: {self.name} attempting ordered acquisition...")
        

        first_to_acquire.acquire_blocking(self.name)
        
        try:

            time.sleep(0.1)
            

            print(f"{time.time():.2f}: {self.name} trying to acquire {second_to_acquire.name}")
            second_to_acquire.acquire_blocking(self.name)
            
            try:

                self.last_progress = time.time()
                self.work_count += 1
                print(f"{time.time():.2f}: {self.name} has both resources! (Work #{self.work_count})")
                

                time.sleep(0.2)
                
            finally:

                second_to_acquire.release()
        finally:

            first_to_acquire.release()



class WorkerWithBackoff(threading.Thread):
    """Alternative solution using exponential backoff"""
    def __init__(self, name, first_resource, second_resource):
        super().__init__(name=name)
        self.first_resource = first_resource
        self.second_resource = second_resource
        self.active = True
        self.last_progress = time.time()
        self.work_count = 0
    
    def run(self):
        backoff_time = 0.1
        max_backoff = 2.0
        
        while self.active and self.work_count < 5:
            success = self.try_acquire_both_resources()
            
            if success:
                backoff_time = 0.1
                self.work_count += 1
            else:

                jitter = random.uniform(0, backoff_time * 0.5)
                time.sleep(backoff_time + jitter)
                backoff_time = min(backoff_time * 2, max_backoff)
    
    def try_acquire_both_resources(self):
        """Try to acquire both resources non-blocking"""
        if self.first_resource.acquire(self.name):
            try:
                time.sleep(0.1)
                
                if self.second_resource.acquire(self.name):
                    try:
                        self.last_progress = time.time()
                        print(f"{time.time():.2f}: {self.name} has both resources!")
                        time.sleep(0.2)
                        return True
                    finally:
                        self.second_resource.release()
                else:
                    print(f"{time.time():.2f}: {self.name} couldn't get {self.second_resource.name}")
            finally:
                self.first_resource.release()
        return False


def monitor_progress(workers, timeout=10):
    """Monitor worker threads"""
    start_time = time.time()
    
    while any(worker.is_alive() for worker in workers):
        current_time = time.time()
        

        for worker in workers:
            if worker.is_alive():
                print(f"{time.time():.2f}: {worker.name} completed {worker.work_count} work units")
        
        if current_time - start_time > timeout:
            print(f"\nTimeout reached. Stopping workers...")
            for worker in workers:
                worker.active = False
            break
        
        time.sleep(2)


def main():
    print("=== SOLUTION 1: Resource Ordering ===")
    

    resource_x = Resource("Resource X", resource_id=1)
    resource_y = Resource("Resource Y", resource_id=2)
    

    thread_a = Worker("Thread A", resource_x, resource_y)
    thread_b = Worker("Thread B", resource_y, resource_x)
    
    print("Starting workers with resource ordering solution...")
    thread_a.start()
    thread_b.start()
    

    monitor_thread = threading.Thread(target=monitor_progress, args=([thread_a, thread_b],))
    monitor_thread.start()
    

    monitor_thread.join()
    thread_a.join(timeout=2)
    thread_b.join(timeout=2)
    
    print(f"Thread A completed {thread_a.work_count} work units")
    print(f"Thread B completed {thread_b.work_count} work units")
    print("Simulation completed successfully - no livelock!")


def demo_backoff_solution():
    """Demonstrate the exponential backoff solution"""
    print("\n=== SOLUTION 2: Exponential Backoff ===")
    
    resource_x = Resource("Resource X", resource_id=1)
    resource_y = Resource("Resource Y", resource_id=2)
    
    thread_a = WorkerWithBackoff("Thread A", resource_x, resource_y)
    thread_b = WorkerWithBackoff("Thread B", resource_y, resource_x)
    
    print("Starting workers with exponential backoff solution...")
    thread_a.start()
    thread_b.start()
    
    monitor_thread = threading.Thread(target=monitor_progress, args=([thread_a, thread_b],))
    monitor_thread.start()
    
    monitor_thread.join()
    thread_a.join(timeout=2)
    thread_b.join(timeout=2)
    
    print(f"Thread A completed {thread_a.work_count} work units")
    print(f"Thread B completed {thread_b.work_count} work units")


if __name__ == "__main__":
    main()

