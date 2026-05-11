import multiprocessing
import time
import random
import sys
from typing import List, Tuple, Protocol
import statistics


class SharedIntCounter(Protocol):
    value: int




def simulate_worker_with_order_violation(
    shared_counter: SharedIntCounter,
    worker_process_id: int,
    total_iterations_per_worker: int,
    max_work_delay_seconds: float,
    max_race_condition_delay_seconds: float = 0.05
) -> None:
    """
    ( )  .
       read-modify-write     
           .
    """
    process_start_time = time.time()
    print(f"[WORKER {worker_process_id}] Starting execution with {total_iterations_per_worker} iterations")

    successful_increments = 0

    for current_iteration in range(total_iterations_per_worker):
        iteration_start_time = time.time()


        simulated_work_delay = random.uniform(0.005, max_work_delay_seconds)
        time.sleep(simulated_work_delay)


        with shared_counter.get_lock():
            current_counter_value = shared_counter.value



            race_condition_delay = random.uniform(0.01, max_race_condition_delay_seconds)
            time.sleep(race_condition_delay)

            new_counter_value = current_counter_value + 1
            shared_counter.value = new_counter_value


        successful_increments += 1

        iteration_end_time = time.time()
        iteration_duration = iteration_end_time - iteration_start_time

        print(
            f"[WORKER {worker_process_id}] Iteration {current_iteration + 1}: "
            f"Updated counter to {new_counter_value} "
            f"(took {iteration_duration:.4f}s, race_delay={race_condition_delay:.4f}s)"
        )

    process_end_time = time.time()
    total_process_duration = process_end_time - process_start_time

    print(
        f"[WORKER {worker_process_id}] COMPLETED: "
        f"{successful_increments} increments in {total_process_duration:.4f}s"
    )


def run_order_violation_experiment(
    num_worker_processes: int = 4,
    iterations_per_worker: int = 25,
    max_work_delay: float = 0.03,
    max_race_delay: float = 0.05
) -> Tuple[int, int, int]:



    shared_counter_value = multiprocessing.Value('i', 0, lock=True)

    worker_process_list: List[multiprocessing.Process] = []
    experiment_start_time = time.time()

    for worker_id in range(1, num_worker_processes + 1):
        worker_process_list.append(
            multiprocessing.Process(
                target=simulate_worker_with_order_violation,
                args=(shared_counter_value, worker_id, iterations_per_worker, max_work_delay, max_race_delay),
                name=f"Worker-{worker_id}",
            )
        )

    for p in worker_process_list:
        p.start()
    for p in worker_process_list:
        p.join()

    total_experiment_duration = time.time() - experiment_start_time

    final_counter_value = shared_counter_value.value
    expected_final_value = num_worker_processes * iterations_per_worker
    lost_update_operations = expected_final_value - final_counter_value

    print("=" * 80)
    print("EXPERIMENT RESULTS")
    print("=" * 80)
    print(f"Final counter value: {final_counter_value}")
    print(f"Expected value: {expected_final_value}")
    print(f"Lost updates: {lost_update_operations}")
    print(f"Corruption percentage: {(lost_update_operations / expected_final_value * 100):.2f}%")
    print(f"Total duration: {total_experiment_duration:.4f}s")

    return final_counter_value, expected_final_value, lost_update_operations


def run_multiple_experiments(num_experiments: int = 5) -> None:
    print("\n" + "=" * 80)
    print(f"RUNNING {num_experiments} INDEPENDENT EXPERIMENTS")
    print("=" * 80)

    results: List[Tuple[int, int, int]] = []
    for experiment_number in range(1, num_experiments + 1):
        print(f"\n>>> EXPERIMENT {experiment_number}/{num_experiments} <<<")
        results.append(
            run_order_violation_experiment(
                num_worker_processes=6,
                iterations_per_worker=20,
                max_work_delay=0.02,
                max_race_delay=0.08,
            )
        )
        time.sleep(0.5)

    final_values = [r[0] for r in results]
    lost_updates = [r[2] for r in results]
    corruption_percentages = [
        (lost / expected * 100) for _, expected, lost in results
    ]

    print("\n" + "=" * 80)
    print("STATISTICAL ANALYSIS")
    print("=" * 80)
    print(f"Final values: {final_values}")
    print(f"Lost updates: {lost_updates}")
    print(f"Average final value: {statistics.mean(final_values):.2f}")
    print(f"Std dev: {statistics.stdev(final_values):.2f}" if len(final_values) > 1 else "Std dev: N/A")
    print(f"Average corruption %: {statistics.mean(corruption_percentages):.2f}%")
    print(f"Max lost updates: {max(lost_updates)}")
    print(f"Min lost updates: {min(lost_updates)}")


if __name__ == "__main__":
    RUN_STATS = False

    try:
        run_order_violation_experiment(
            num_worker_processes=4,
            iterations_per_worker=15,
            max_work_delay=0.025,
            max_race_delay=0.06,
        )

        if RUN_STATS:
            run_multiple_experiments(num_experiments=3)

    except KeyboardInterrupt:
        print("\nExperiment interrupted by user")
        sys.exit(0)
    except Exception as simulation_error:
        print(f"\nError during simulation: {simulation_error}")
        sys.exit(1)
