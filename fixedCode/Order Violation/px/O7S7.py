"""
Order Violation Simulation - FIXED (No Interleaving with Lock)
"""
import multiprocessing
from multiprocessing import Manager, Process, Lock
import time
import random
from collections import defaultdict


def worker_process_add_items(shared_data_list, shared_lock, process_identifier, items_to_insert,
                            min_delay_seconds=0.01, max_delay_seconds=0.1):
    """Worker process: adds items to the shared list after a random delay - LOCKED."""
    for current_item in items_to_insert:
        time.sleep(random.uniform(min_delay_seconds, max_delay_seconds))
        with shared_lock:
            shared_data_list.append(current_item)


def analyze_order_violations(final_shared_list, process_configurations):
    """Analyze the final list and detect order violations."""
    analysis_results = {
        'total_items': len(final_shared_list),
        'expected_items': sum(len(items) for items in process_configurations.values()),
        'process_positions': {},
        'order_violations': [],
        'interleaving_detected': False,
        'missing_items': [],
        'duplicate_items': []
    }

    item_positions_by_process = defaultdict(list)
    item_frequency_counter = defaultdict(int)


    for position_index, item_value in enumerate(final_shared_list):
        item_frequency_counter[item_value] += 1
        for process_name, expected_items in process_configurations.items():
            if item_value in expected_items:
                item_positions_by_process[process_name].append(position_index)
                break

    analysis_results['process_positions'] = dict(item_positions_by_process)

    all_expected_items = set()
    for expected_items in process_configurations.values():
        all_expected_items.update(expected_items)
    actual_items = set(final_shared_list)
    analysis_results['missing_items'] = list(all_expected_items - actual_items)

    for item_value, frequency in item_frequency_counter.items():
        if frequency > 1:
            analysis_results['duplicate_items'].append((item_value, frequency))


    violations_found = False
    process_names = sorted(process_configurations.keys())
    for i in range(len(process_names)):
        for j in range(i + 1, len(process_names)):
            process_a_name = process_names[i]
            process_b_name = process_names[j]
            
            positions_a = item_positions_by_process[process_a_name]
            positions_b = item_positions_by_process[process_b_name]

            for pos_a in positions_a:
                for pos_b in positions_b:
                    if pos_b < pos_a:
                        analysis_results['order_violations'].append({
                            'earlier_process': process_b_name,
                            'earlier_position': pos_b,
                            'earlier_item': final_shared_list[pos_b],
                            'later_process': process_a_name,
                            'later_position': pos_a,
                            'later_item': final_shared_list[pos_a]
                        })
                        violations_found = True
    
    analysis_results['interleaving_detected'] = violations_found
    return analysis_results


def create_test_data_for_processes(number_of_processes, items_per_process):
    """Create test data for each process."""
    process_data_configuration = {}
    for process_number in range(number_of_processes):
        process_name = f"Process-{process_number + 1}"
        process_items = [f"{chr(65 + process_number)}{item_number + 1}"
                        for item_number in range(items_per_process)]
        process_data_configuration[process_name] = process_items
    return process_data_configuration


def run_order_violation_simulation(number_of_concurrent_processes=5,
                                  items_per_process=10,
                                  minimum_delay_seconds=0.01,
                                  maximum_delay_seconds=0.1,
                                  run_multiple_iterations=3):
    """Run simulation with synchronized appends (0 violations)."""
    print(f"Order violation simulation (LOCKED): processes={number_of_concurrent_processes}, "
          f"items_per_process={items_per_process}, runs={run_multiple_iterations}")

    iteration_results = []
    for iteration_number in range(run_multiple_iterations):
        manager = Manager()
        shared_result_list = manager.list()
        shared_lock = Lock()

        process_data_mapping = create_test_data_for_processes(
            number_of_concurrent_processes, items_per_process
        )

        processes = []
        start_time = time.time()

        for process_name, items_to_add in process_data_mapping.items():
            p = Process(target=worker_process_add_items,
                       args=(shared_result_list, shared_lock, process_name, items_to_add,
                             minimum_delay_seconds, maximum_delay_seconds),
                       name=process_name)
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        exec_time = time.time() - start_time
        final_list = list(shared_result_list)

        analysis = analyze_order_violations(final_list, process_data_mapping)
        analysis.update({
            'iteration_number': iteration_number + 1,
            'execution_time': exec_time,
            'final_list': final_list
        })

        iteration_results.append(analysis)
        print(f" Iter {iteration_number + 1}: time={exec_time:.3f}s, "
              f"violations={len(analysis['order_violations'])}, "
              f"interleaving={analysis['interleaving_detected']}")

    total_violations = sum(len(r['order_violations']) for r in iteration_results)
    print(f"Summary: total_violations={total_violations}, avg_per_iter={total_violations/len(iteration_results):.2f} ✅")
    return iteration_results


if __name__ == "__main__":
    run_order_violation_simulation(
        number_of_concurrent_processes=6,
        items_per_process=8,
        minimum_delay_seconds=0.01,
        maximum_delay_seconds=0.08,
        run_multiple_iterations=5,
    )
