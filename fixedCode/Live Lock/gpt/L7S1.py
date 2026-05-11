import threading
import time
import random
from enum import Enum
from datetime import datetime

class WorkerStatus(Enum):
    IDLE = "IDLE"
    WORKING = "WORKING"
    WAITING = "WAITING"
    FINISHED = "FINISHED"

class SharedResource:
    def __init__(self):
        self.active = True
        self.lock = threading.Lock()
        self.access_count = 0
        self.last_accessed_by = None
        self.last_access_time = None
        
    def is_active(self):
        return self.active
        
    def set_active(self, active, worker_name):
        with self.lock:
            self.active = active
            self.access_count += 1
            self.last_accessed_by = worker_name
            self.last_access_time = datetime.now()
            
    def get_stats(self):
        return {
            "access_count": self.access_count,
            "last_accessed_by": self.last_accessed_by,
            "last_access_time": self.last_access_time
        }

class Task:
    def __init__(self, name, duration):
        self.name = name
        self.duration = duration
        self.completed = False
        
    def execute(self):
        time.sleep(self.duration)
        self.completed = True
        return f"Completed task: {self.name}"

class TurnCoordinator:
    """Prevents polite livelock by serializing progress: exactly one worker proceeds at a time."""
    def __init__(self, worker_names):
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.turn = worker_names[0]
        self.progress_counter = 0
        self.last_progress_time = time.time()

    def wait_my_turn(self, name):
        with self.cond:
            while self.turn != name:
                self.cond.wait()

    def pass_turn(self, next_name):
        with self.cond:
            self.turn = next_name
            self.progress_counter += 1
            self.last_progress_time = time.time()
            self.cond.notify_all()

    def get_turn(self):
        with self.lock:
            return self.turn, self.progress_counter, self.last_progress_time

class Worker:
    def __init__(self, name, active=True):
        self.name = name
        self.status = WorkerStatus.IDLE
        self.tasks_completed = 0
        self.total_work_time = 0.0
        self.lock = threading.Lock()

        self._still_running = True

    def get_name(self):
        return self.name
        
    def get_status(self):
        return self.status
        
    def perform_task(self):
        task = Task(f"Task-{random.randint(1000, 9999)}", random.uniform(0.1, 0.5))
        start_time = time.time()
        result = task.execute()
        self.total_work_time += time.time() - start_time
        self.tasks_completed += 1
        return result
        
    def work(self, shared_resource, other_worker, coordinator: TurnCoordinator, max_tasks=5):
        while self._still_running and self.tasks_completed < max_tasks:

            self.status = WorkerStatus.WAITING
            coordinator.wait_my_turn(self.name)


            self.status = WorkerStatus.WORKING
            with self.lock:
                task_result = self.perform_task()
                print(f"{self.name} [{self.status.value}]: {task_result}")
                

                if shared_resource.is_active():
                    print(f"{self.name} sets the shared resource to inactive.")
                    shared_resource.set_active(False, self.name)
                else:
                    print(f"{self.name} activates the shared resource.")
                    shared_resource.set_active(True, self.name)


            coordinator.pass_turn(other_worker.get_name())

        self.status = WorkerStatus.FINISHED
        print(f"\n{self.name} Statistics:")
        print(f"Tasks Completed: {self.tasks_completed}")
        print(f"Total Work Time: {self.total_work_time:.2f} seconds")

def monitor_resource(shared_resource, workers, coordinator: TurnCoordinator, interval=1.0):
    while any(worker.get_status() != WorkerStatus.FINISHED for worker in workers):
        stats = shared_resource.get_stats()
        turn, prog, last = coordinator.get_turn()
        print(f"\nResource Monitor:")
        print(f"Access Count: {stats['access_count']}")
        print(f"Last Accessed By: {stats['last_accessed_by']}")
        print(f"Last Access Time: {stats['last_access_time']}")
        print(f"Current Turn: {turn} | Progress#: {prog}")
        print("Worker Status:")
        for worker in workers:
            print(f"- {worker.get_name()}: {worker.get_status().value}")
        time.sleep(interval)

def main():
    shared_resource = SharedResource()
    worker1 = Worker("Worker 1")
    worker2 = Worker("Worker 2")

    coordinator = TurnCoordinator([worker1.get_name(), worker2.get_name()])
    workers = [worker1, worker2]
    

    monitor_thread = threading.Thread(
        target=monitor_resource, 
        args=(shared_resource, workers, coordinator),
        daemon=True
    )
    

    thread1 = threading.Thread(
        target=worker1.work, 
        args=(shared_resource, worker2, coordinator)
    )
    thread2 = threading.Thread(
        target=worker2.work, 
        args=(shared_resource, worker1, coordinator)
    )
    

    monitor_thread.start()
    thread1.start()
    thread2.start()
    

    thread1.join()
    thread2.join()

if __name__ == "__main__":
    main()
