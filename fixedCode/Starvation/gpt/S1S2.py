import multiprocessing as mp
import time
import random
import queue
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import threading


class ResourceManager:
    def __init__(self, resource_units=1):
        """
        Initialize the resource manager with a limited number of resource units.
        We enforce fairness (first-come-first-served) instead of strict priority.
        """
        self.resource_units = resource_units


        self.resource_lock = mp.Lock()



        self.process_queue = mp.Queue()


        self.active_processes = set()


        manager = mp.Manager()
        self.completion_times = manager.dict()
        self.waiting_times = manager.dict()
        self.resource_access_logs = manager.list()
        self.process_start_times = manager.dict()

        self.exit_flag = mp.Event()

    def allocate_resources(self):
        """
        Allocation loop.
        Instead of always picking the highest priority,
        we pick the request that has waited the longest (FIFO fairness),
        similar to a fair ReentrantLock.
        """
        print("Resource allocator started")




        pending_requests = []

        while not self.exit_flag.is_set():
            try:

                while not self.process_queue.empty():
                    try:
                        process_id, priority, work_time, request_time = self.process_queue.get(block=False)
                        pending_requests.append((request_time, process_id, priority, work_time))
                    except queue.Empty:
                        break


                pending_requests.sort(key=lambda x: x[0])


                if len(self.active_processes) < self.resource_units and pending_requests:
                    request_time, process_id, priority, work_time = pending_requests.pop(0)
                    self.active_processes.add(process_id)


                    wait_time = time.time() - request_time


                    if process_id in self.waiting_times:
                        self.waiting_times[process_id].append(wait_time)
                    else:
                        self.waiting_times[process_id] = [wait_time]


                    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    log_entry = (
                        f"{timestamp} - Process {process_id} "
                        f"(Priority: {priority}) granted resource access "
                        f"after waiting {wait_time:.2f} seconds"
                    )
                    self.resource_access_logs.append(log_entry)
                    print(log_entry)


                    time.sleep(work_time)


                    self.active_processes.remove(process_id)


                    completion_time = time.time()
                    if process_id in self.completion_times:
                        self.completion_times[process_id].append(completion_time)
                    else:
                        self.completion_times[process_id] = [completion_time]

                    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    log_entry = (
                        f"{timestamp} - Process {process_id} completed task, "
                        f"held resource for {work_time:.2f} seconds"
                    )
                    self.resource_access_logs.append(log_entry)
                    print(log_entry)


                time.sleep(0.1)

            except Exception as e:
                print(f"Error in resource allocator: {e}")

        print("Resource allocator stopped")

    def request_resource(self, process_id, priority, work_time):
        """
        Called by workers whenever they want the resource.
        Now we enqueue with the current timestamp so fairness can be enforced.
        """
        request_time = time.time()
        self.process_start_times[process_id] = request_time
        self.process_queue.put((process_id, priority, work_time, request_time))

        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = (
            f"{timestamp} - Process {process_id} "
            f"(Priority: {priority}) requested resource access "
            f"for {work_time:.2f} seconds"
        )
        self.resource_access_logs.append(log_entry)
        print(log_entry)


def worker_process(process_id, priority, work_iterations, resource_manager, delay_range):
    """
    Worker repeatedly requests the resource.
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
    """Analyze and display simulation results (unchanged logic)."""
    print("\n=== SIMULATION RESULTS ===")

    waiting_times = dict(resource_manager.waiting_times)
    completion_times = dict(resource_manager.completion_times)


    avg_waiting_times = {
        pid: (sum(wait_times) / len(wait_times))
        for pid, wait_times in waiting_times.items()
    }


    tasks_completed = {
        pid: len(completions)
        for pid, completions in completion_times.items()
    }

    print("\nProcess Summary:")
    print("---------------")
    for pid in sorted(avg_waiting_times.keys()):
        print(
            f"Process {pid}: "
            f"Avg Wait: {avg_waiting_times[pid]:.2f}s, "
            f"Tasks Completed: {tasks_completed.get(pid, 0)}"
        )


    starved_threshold = 2.0
    starved_processes = [
        pid for pid, wt in avg_waiting_times.items()
        if wt > starved_threshold
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
        color=[
            'green' if tasks_completed.get(pid, 0) > 1 else 'orange'
            for pid in process_ids
        ]
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
    """
    Run the fairness simulation.
    We still *assign* priorities to processes,
    but the allocator now ignores them for ordering (fair scheduling).
    """
    num_processes = 5
    resource_units = 1
    simulation_duration = 30

    resource_manager = ResourceManager(resource_units=resource_units)


    allocator_thread = threading.Thread(
        target=resource_manager.allocate_resources,
        daemon=True
    )
    allocator_thread.start()

    processes = []


    for i in range(2):
        p = mp.Process(
            target=worker_process,
            args=(i, 9, 10, resource_manager, (0.5, 1.5))
        )
        processes.append(p)


    for i in range(2, 4):
        p = mp.Process(
            target=worker_process,
            args=(i, 5, 10, resource_manager, (0.5, 1.5))
        )
        processes.append(p)


    p = mp.Process(
        target=worker_process,
        args=(4, 1, 10, resource_manager, (0.5, 1.5))
    )
    processes.append(p)

    print(f"Starting simulation with {num_processes} processes...")
    for p in processes:
        p.start()


    time.sleep(simulation_duration)


    resource_manager.exit_flag.set()


    for p in processes:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    print("All processes have completed or been terminated")

    analyze_results(resource_manager)


if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()
