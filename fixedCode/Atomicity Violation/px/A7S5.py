import multiprocessing
import time
import random
from multiprocessing import Manager, Lock

def modify_shared_list(shared_data_locks, increment_value, target_index, operation_count, process_id):
    """
    Modifies shared values using atomic Value objects with lock protection.
    
    Args:
    shared_data_locks: Dict containing Manager().Value and Lock for each index
    increment_value: Value to add/subtract from the target index
    target_index: Index to modify
    operation_count: Number of operations to perform
    process_id: Identifier for the current process
    """
    print(f"Process {process_id} starting with {operation_count} operations")
    
    target_value = shared_data_locks[target_index]['value']
    target_lock = shared_data_locks[target_index]['lock']
    
    for iteration in range(operation_count):
                                                   
        with target_lock:
            target_value.value += increment_value
        
                                                                   
        if iteration % 50000 == 0:
            print(f"Process {process_id} completed {iteration} operations")
            time.sleep(random.uniform(0.001, 0.005))
    
    print(f"Process {process_id} completed all {operation_count} operations")

if __name__ == '__main__':
                                                               
    process_manager = multiprocessing.Manager()
    
                                                                                  
    shared_data_locks = {
        0: {'value': process_manager.Value('i', 0), 'lock': process_manager.Lock()},
        1: {'value': process_manager.Value('i', 100), 'lock': process_manager.Lock()},
        2: {'value': process_manager.Value('i', -50), 'lock': process_manager.Lock()},
        4: {'value': process_manager.Value('i', 0), 'lock': process_manager.Lock()}
    }
    
    print("Initial shared values:", [shared_data_locks[i]['value'].value for i in [0,1,2,4]])
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
        process_obj = multiprocessing.Process(
            target=modify_shared_list,
            args=(
                shared_data_locks,
                config["increment_value"],
                config["target_index"],
                config["operation_count"],
                config["process_id"]
            )
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
    
                           
    final_values = [shared_data_locks[i]['value'].value for i in [0,1,2,4]]
    print("\n" + "="*50)
    print("FINAL RESULTS")
    print("="*50)
    print(f"Initial values: [0, 100, -50, 25, 0]")
    print(f"Final values: [{final_values[0]}, {final_values[1]}, {final_values[2]}, 25, {final_values[3]}]")
    print(f"Total execution time: {execution_time:.2f} seconds")
    
                                
    expected_results = [0, 100, -50, 25, 0]
    expected_results[0] += (500000 * 1) + (500000 * -1)     
    expected_results[1] += (250000 * 2) + (150000 * -3)         
    expected_results[2] += (200000 * 5)                                       
    expected_results[4] += (100000 * 10)                          
    
    print(f"\nExpected results: {expected_results}")
    
                      
    print("\nAtomicity verification:")
    all_correct = True
    for i, (expected, actual) in enumerate(zip(expected_results, [final_values[0], final_values[1], final_values[2], 25, final_values[3]])):
        if expected == actual:
            print(f"Index {i}: ✓ Correct ({actual})")
        else:
            print(f"Index {i}: ✗ Expected {expected}, Got {actual}")
            all_correct = False
    
    print(f"\nResult: {'ATOMICALLY CORRECT ✓' if all_correct else 'Race condition detected!'}")
