import multiprocessing
import time
import random
from multiprocessing import Manager, Value

def modify_shared_value(shared_value, increment_value, operation_count, process_id):
    """
    Modifies a shared multiprocessing.Value by performing atomic arithmetic operations.
    
    Args:
    shared_value: multiprocessing.Value object shared between processes
    increment_value: Value to add to the shared value
    operation_count: Number of operations to perform
    process_id: Identifier for the current process
    """
    print(f"Process {process_id} starting with {operation_count} operations")

    for iteration in range(operation_count):
                                                    
        with shared_value.get_lock():
            shared_value.value += increment_value
        
                                                             
        if iteration % 50000 == 0:
            print(f"Process {process_id} completed {iteration} operations")
            time.sleep(random.uniform(0.001, 0.005))

    print(f"Process {process_id} completed all {operation_count} operations")

if __name__ == '__main__':
                                      
    process_manager = multiprocessing.Manager()

                                                                                
                                            
    shared_values = {
        0: Value('i', 0),                                                   
        1: Value('i', 100),                                            
        2: Value('i', -50),                               
        4: Value('i', 0)                                 
    }

    print("Initial shared values:", {k: v.value for k, v in shared_values.items()})
    print("Starting multiprocessing operations...\n")

                                                                          
    process_configs = [
        {"shared_value": shared_values[0], "increment_value": 1, "operation_count": 500000, "process_id": "ADDER_1"},
        {"shared_value": shared_values[0], "increment_value": -1, "operation_count": 500000, "process_id": "SUBTRACTOR_1"},
        {"shared_value": shared_values[1], "increment_value": 2, "operation_count": 250000, "process_id": "DOUBLER"},
        {"shared_value": shared_values[1], "increment_value": -3, "operation_count": 150000, "process_id": "TRIPLER"},
        {"shared_value": shared_values[2], "increment_value": 5, "operation_count": 200000, "process_id": "INCREMENTER"},
        {"shared_value": shared_values[4], "increment_value": 10, "operation_count": 100000, "process_id": "BIG_ADDER"},
    ]

                            
    process_list = []
    for config in process_configs:
        process_obj = multiprocessing.Process(
            target=modify_shared_value,
            args=(
                config["shared_value"],
                config["increment_value"],
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

                           
    final_values = [shared_values[i].value if i in shared_values else None for i in range(5)]
    print("\n" + "="*50)
    print("FINAL RESULTS")
    print("="*50)
    print(f"Initial values: [0, 100, -50, 25, 0]")
    print(f"Final values: {final_values}")
    print(f"Total execution time: {execution_time:.2f} seconds")

                                
    expected_results = [0, 100, -50, 25, 0]
    expected_results[0] += (500000 * 1) + (500000 * -1)            
    expected_results[1] += (250000 * 2) + (150000 * -3)         
    expected_results[2] += (200000 * 5)                          
    expected_results[4] += (100000 * 10)                          

    print(f"\nExpected results: {expected_results}")

                      
    print("\nAtomicity verification:")
    all_correct = True
    for i in [0, 1, 2, 4]:
        expected = expected_results[i]
        actual = shared_values[i].value
        if actual == expected:
            print(f"Index {i}: ✓ Correct ({actual})")
        else:
            print(f"Index {i}: ✗ Expected {expected}, Got {actual}")
            all_correct = False
    
    print(f"\nAtomic operations: {'✓ PASSED' if all_correct else '✗ FAILED'}")
    print("All shared values updated atomically using multiprocessing.Value with locks.")
