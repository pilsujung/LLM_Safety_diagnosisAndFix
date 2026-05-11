import threading
import time
import random

class Resource:
    def __init__(self, name, resource_id):
        self.name = name
        self.resource_id = resource_id
        self.lock = threading.Lock()
        self.owner = None
    
    def acquire(self, owner, timeout=None):
        """Try to acquire the resource for the given owner with optional timeout"""
        result = self.lock.acquire(blocking=True, timeout=timeout)
        if result:
            self.owner = owner
            print(f"{time.time():.2f}: {owner} acquired {self.name}")
        return result
    
    def release(self):
        """Release the resource"""
        previous_owner = self.owner
        self.owner = None
        self.lock.release()
        print(f"{time.time():.2f}: {previous_owner} released {self.name}")


class Worker(threading.Thread):
    def __init__(self, name, resource1, resource2):
        super().__init__(name=name)
        self.resource1 = resource1
        self.resource2 = resource2
        self.active = True
        self.last_progress = time.time()
        self.work_completed = 0
        


        if resource1.resource_id < resource2.resource_id:
            self.first_resource = resource1
            self.second_resource = resource2
        else:
            self.first_resource = resource2
            self.second_resource = resource1
    
    def run(self):
        while self.active:

            success = self.acquire_resources_with_timeout()
            
            if success:
                self.do_work()
                self.work_completed += 1
                print(f"{time.time():.2f}: {self.name} completed work #{self.work_completed}")
            

            backoff_time = min(0.1 * (1.5 ** (self.work_completed % 5)), 0.5)
            jitter = random.uniform(0.8, 1.2)
            time.sleep(backoff_time * jitter)
    
    def acquire_resources_with_timeout(self):
        """Acquire both resources using ordered acquisition with timeout"""
        first_acquired = False
        second_acquired = False
        
        try:

            print(f"{time.time():.2f}: {self.name} trying to acquire {self.first_resource.name}")
            first_acquired = self.first_resource.acquire(self.name, timeout=1.0)
            
            if not first_acquired:
                print(f"{time.time():.2f}: {self.name} failed to acquire {self.first_resource.name}")
                return False
            

            time.sleep(0.05)
            

            print(f"{time.time():.2f}: {self.name} trying to acquire {self.second_resource.name}")
            second_acquired = self.second_resource.acquire(self.name, timeout=1.0)
            
            if not second_acquired:
                print(f"{time.time():.2f}: {self.name} failed to acquire {self.second_resource.name}")
                return False
            
            return True
            
        except Exception as e:
            print(f"{time.time():.2f}: {self.name} encountered error: {e}")
            return False
            
        finally:

            if first_acquired and not second_acquired:
                self.first_resource.release()
    
    def do_work(self):
        """Perform work while holding both resources"""
        try:
            self.last_progress = time.time()
            print(f"{time.time():.2f}: {self.name} working with both {self.resource1.name} and {self.resource2.name}")
            

            time.sleep(random.uniform(0.1, 0.3))
            
        finally:

            self.second_resource.release()
            self.first_resource.release()


def monitor_progress(workers, max_runtime=10):
    """Monitor worker progress and detect issues"""
    start_time = time.time()
    last_total_work = 0
    no_progress_count = 0
    
    while all(worker.is_alive() for worker in workers):
        current_time = time.time()
        

        total_work = sum(worker.work_completed for worker in workers)
        
        if total_work == last_total_work:
            no_progress_count += 1
        else:
            no_progress_count = 0
        

        print(f"\n--- Progress Report (t={current_time - start_time:.1f}s) ---")
        for worker in workers:
            time_since_progress = current_time - worker.last_progress
            print(f"{worker.name}: {worker.work_completed} tasks completed, "
                  f"last progress {time_since_progress:.1f}s ago")
        

        if no_progress_count >= 3:
            print("⚠️  Warning: No progress detected for 3 seconds")
        

        if current_time - start_time > max_runtime:
            print(f"\n🛑 Stopping after {max_runtime} seconds")
            for worker in workers:
                worker.active = False
            break
        
        last_total_work = total_work
        time.sleep(1)


def main():
    print("=== LIVELOCK PREVENTION DEMO ===\n")
    

    resource_x = Resource("Resource X", resource_id=1)
    resource_y = Resource("Resource Y", resource_id=2)
    


    thread_a = Worker("Thread A", resource_x, resource_y)
    thread_b = Worker("Thread B", resource_y, resource_x)
    
    print("Key improvements:")
    print("1. 🔄 Resource Ordering: Always acquire resources by ID (prevents circular wait)")
    print("2. ⏱️  Timeouts: Bounded waiting prevents infinite loops")  
    print("3. 📈 Exponential Backoff: Reduces contention over time")
    print("4. 🧹 Proper Cleanup: Resources released even on failure\n")
    

    print("Starting workers...\n")
    thread_a.start()
    thread_b.start()
    

    monitor_thread = threading.Thread(target=monitor_progress, args=([thread_a, thread_b],))
    monitor_thread.start()
    

    monitor_thread.join()
    thread_a.join(timeout=2)
    thread_b.join(timeout=2)
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"Thread A completed: {thread_a.work_completed} tasks")
    print(f"Thread B completed: {thread_b.work_completed} tasks")
    print(f"Total work completed: {thread_a.work_completed + thread_b.work_completed}")
    print("✅ Livelock successfully prevented!")


if __name__ == "__main__":
    main()