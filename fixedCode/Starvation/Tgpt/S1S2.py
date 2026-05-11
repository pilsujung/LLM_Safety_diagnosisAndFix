import time
import random
from datetime import datetime
import threading
import matplotlib.pyplot as plt


class ResourceManager:
    def __init__(self, resource_units=1):
        """
        Initialize the resource manager with a limited number of resource units.

        Args:
            resource_units: Number of concurrent resources available
        """
        self.resource_units = resource_units


        self.state_lock = threading.Lock()



        self.request_queue = []


        self.active_processes = set()


        self.completion_times = {}
        self.waiting_times = {}
        self.resource_access_logs = []
        self.process_start_times = {}


        self.exit_flag = threading.Event()

    def _log(self, msg):
        print(msg)
        self.resource_access_logs.append(msg)

    def allocate_resources(self):
        """
         :
        -    (FIFO)
        -   priority    
        -   grant  thread  ' '  
          allocator        
        """
        self._log("Resource allocator started")

        while True:

            if self.exit_flag.is_set():
                with self.state_lock:
                    queue_empty = (len(self.request_queue) == 0)
                    active_empty = (len(self.active_processes) == 0)
                if queue_empty and active_empty:
                    break

            granted_task = None

            with self.state_lock:

                if len(self.active_processes) < self.resource_units and len(self.request_queue) > 0:
                    arrival_time, process_id, priority, work_time = self.request_queue.pop(0)


                    self.active_processes.add(process_id)


                    wait_time = time.time() - self.process_start_times[process_id]


                    if process_id in self.waiting_times:
                        self.waiting_times[process_id].append(wait_time)
                    else:
                        self.waiting_times[process_id] = [wait_time]


                    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    self._log(
                        f"{timestamp} - Process {process_id} "
                        f"(Priority: {priority}) granted resource access after waiting {wait_time:.2f} seconds"
                    )


                    granted_task = (process_id, priority, work_time)


            if granted_task is not None:
                process_id, priority, work_time = granted_task

                def run_task(p_id, prio, hold_time):

                    time.sleep(hold_time)


                    with self.state_lock:

                        if p_id in self.active_processes:
                            self.active_processes.remove(p_id)


                        completion_time = time.time()
                        if p_id in self.completion_times:
                            self.completion_times[p_id].append(completion_time)
                        else:
                            self.completion_times[p_id] = [completion_time]


                        timestamp_done = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        self._log(
                            f"{timestamp_done} - Process {p_id} completed task, "
                            f"held resource for {hold_time:.2f} seconds"
                        )

                t = threading.Thread(target=run_task, args=(process_id, priority, work_time), daemon=True)
                t.start()


            time.sleep(0.01)

        self._log("Resource allocator stopped")

    def request_resource(self, process_id, priority, work_time):
        """
        Request access to the resource.

        Args:
            process_id: Unique identifier for the process
            priority: Priority level (higher number = higher priority) -- now only logged
            work_time: How long the process needs the resource
        """
        arrival_time = time.time()

        with self.state_lock:
            self.process_start_times[process_id] = arrival_time

            self.request_queue.append((arrival_time, process_id, priority, work_time))

        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = (
            f"{timestamp} - Process {process_id} (Priority: {priority}) "
            f"requested resource access for {work_time:.2f} seconds"
        )
        self._log(log_entry)


def worker_process(process_id, priority, work_iterations, resource_manager, delay_range):
    """
    Worker "thread" that repeatedly requests access to the shared resource.
    (  thread     )
    """
    try:
        for _ in range(work_iterations):

            work_time = random.uniform(0.5, 2.0)


            resource_manager.request_resource(process_id, priority, work_time)


            delay = random.uniform(delay_range[0], delay_range[1])
            time.sleep(delay)

    except Exception as e:
        print(f"Error in worker process {process_id}: {e}")


def analyze_results(resource_manager):
    """Analyze and display the results of the simulation"""
    print("\n=== SIMULATION RESULTS ===")


    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)


    avg_waiting_times = {}
    for pid, wait_times in waiting_times.items():
        avg_waiting_times[pid] = sum(wait_times) / len(wait_times)


    tasks_completed = {}
    for pid, completions in completion_times.items():
        tasks_completed[pid] = len(completions)


    print("\nProcess Summary:")
    print("---------------")
    for pid in sorted(avg_waiting_times.keys()):
        avg_wait = avg_waiting_times[pid]
        done = tasks_completed.get(pid, 0)
        print(f"Process {pid}: Avg Wait: {avg_wait:.2f}s, Tasks Completed: {done}")


    starved_threshold = 2.0
    starved_processes = [
        pid for pid, wait_time in avg_waiting_times.items()
        if wait_time > starved_threshold
    ]

    if starved_processes:
        print("\nStarved Processes:")
        print("----------------")
        for pid in starved_processes:
            print(f"Process {pid}: Avg Wait: {avg_waiting_times[pid]:.2f}s")


    process_ids = sorted(avg_waiting_times.keys())
    avg_waits = [avg_waiting_times[pid] for pid in process_ids]

    plt.figure(figsize=(10, 6))
    plt.bar(
        process_ids,
        avg_waits,
        color=['red' if pid in starved_processes else 'blue' for pid in process_ids]
    )
    plt.title('Average Waiting Time by Process')
    plt.xlabel('Process ID')
    plt.ylabel('Average Waiting Time (seconds)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('waiting_times.png')


    tasks = [tasks_completed.get(pid, 0) for pid in process_ids]

    plt.figure(figsize=(10, 6))
    plt.bar(
        process_ids,
        tasks,
        color=['green' if tasks_completed.get(pid, 0) > 1 else 'orange' for pid in process_ids]
    )
    plt.title('Tasks Completed by Process')
    plt.xlabel('Process ID')
    plt.ylabel('Number of Tasks Completed')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('tasks_completed.png')

    print("\nSaved visualizations: waiting_times.png and tasks_completed.png")


    with open('resource_access_logs.txt', 'w') as f:
        for log in resource_manager.resource_access_logs:
            f.write(log + '\n')

    print("\nDetailed logs saved to resource_access_logs.txt")


def main():
    """Main function to run the simulation"""
    num_workers = 5
    resource_units = 1
    simulation_duration = 30

    resource_manager = ResourceManager(resource_units=resource_units)


    allocator_thread = threading.Thread(
        target=resource_manager.allocate_resources,
        daemon=True
    )
    allocator_thread.start()


    worker_threads = []


    for i in range(2):
        t = threading.Thread(
            target=worker_process,
            args=(i, 9, 10, resource_manager, (0.5, 1.5)),
            daemon=True
        )
        worker_threads.append(t)


    for i in range(2, 4):
        t = threading.Thread(
            target=worker_process,
            args=(i, 5, 10, resource_manager, (0.5, 1.5)),
            daemon=True
        )
        worker_threads.append(t)


    t = threading.Thread(
        target=worker_process,
        args=(4, 1, 10, resource_manager, (0.5, 1.5)),
        daemon=True
    )
    worker_threads.append(t)

    print(f"Starting simulation with {num_workers} processes...")


    for t in worker_threads:
        t.start()


    time.sleep(simulation_duration)


    resource_manager.exit_flag.set()


    allocator_thread.join()

    print("All workers have finished or been stopped")


    analyze_results(resource_manager)


if __name__ == "__main__":
    main()
