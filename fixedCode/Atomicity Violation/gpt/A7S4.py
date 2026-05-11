import multiprocessing
import time
import random


def modify_shared_list(shared_data_list, increment_value, target_index,
                       operation_count, process_id, index_lock):
    """
    Modifies a shared list by performing arithmetic operations on a specific index
    in an atomic way using a per-index lock.

    Args:
        shared_data_list: Manager().list() object shared between processes
        increment_value: Value to add/subtract from the target index
        target_index: Index in the list to modify
        operation_count: Number of operations to perform
        process_id: Identifier for the current process
        index_lock: multiprocessing.Lock() associated with this index
    """
    print(f"Process {process_id} starting with {operation_count} operations")

    for iteration in range(operation_count):
                                                     
        with index_lock:
            shared_data_list[target_index] += increment_value

                                                                            
        if iteration % 50000 == 0:
            print(f"Process {process_id} completed {iteration} operations")
            time.sleep(random.uniform(0.001, 0.005))                      

    print(f"Process {process_id} completed all {operation_count} operations")


if __name__ == '__main__':
                                                               
    process_manager = multiprocessing.Manager()

                                                            
    initial_values = [0, 100, -50, 25, 0]
    shared_data_list = process_manager.list(initial_values)

                                                                     
                                            
    index_locks = [multiprocessing.Lock() for _ in range(len(initial_values))]

    print("Initial shared list values:", list(shared_data_list))
    print("Starting multiprocessing operations...\n")

                                           
    process_configs = [
        {"increment_value": 1, "target_index": 0, "operation_count": 500000, "process_id": "ADDER_1"},
        {"increment_value": -1, "target_index": 0, "operation_count": 500000, "process_id": "SUBTRACTOR_1"},
        {"increment_value": 2, "target_index": 1, "operation_count": 250000, "process_id": "DOUBLER"},
        {"increment_value": -3, "target_index": 1, "operation_count": 150000, "process_id": "TRIPLER"},
        {"increment_value": 5, "target_index": 2, "operation_count": 200000, "process_id": "INCREMENTER"},
        {"increment_value": 10, "target_index": 4, "operation_count": 100000, "process_id": "BIG_ADDER"},
    ]

                            
    process_list = []
    for config in process_configs:
        lock_for_index = index_locks[config["target_index"]]
        process_obj = multiprocessing.Process(
            target=modify_shared_list,
            args=(
                shared_data_list,
                config["increment_value"],
                config["target_index"],
                config["operation_count"],
                config["process_id"],
                lock_for_index,
            ),
        )
        process_list.append(process_obj)

                                                   
    start_time = time.time()

                         
    print("Launching all processes...")
    for process_obj in process_list:
        process_obj.start()

                                        
    print("Waiting for all processes to finish...\n")
    for process_obj in process_list:
        process_obj.join()

                     
    end_time = time.time()
    execution_time = end_time - start_time

                           
    print("\n" + "=" * 50)
    print("FINAL RESULTS")
    print("=" * 50)
    print(f"Initial values: {initial_values}")
    print(f"Final values:   {list(shared_data_list)}")
    print(f"Total execution time: {execution_time:.2f} seconds")

                                                                                    
    expected_results = initial_values.copy()
    expected_results[0] += (500000 * 1) + (500000 * -1)                 
    expected_results[1] += (250000 * 2) + (150000 * -3)                            
    expected_results[2] += (200000 * 5)                               
    expected_results[4] += (100000 * 10)                              

    print(f"\nExpected results (no atomic violation): {expected_results}")

                         
    print("\nAnalysis of atomic violation:")
    for i, (expected, actual) in enumerate(zip(expected_results, shared_data_list)):
        difference = actual - expected
        if difference != 0:
            print(f"Index {i}: Expected {expected}, Got {actual}, Difference: {difference}")
        else:
            print(f"Index {i}: No atomic violation detected")

    print("\nAll indices with zero difference are now protected against atomic violations "
          "by using per-index locks for the shared list updates.")
