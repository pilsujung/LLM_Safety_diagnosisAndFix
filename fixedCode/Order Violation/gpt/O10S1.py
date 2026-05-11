import threading
import time
import random
import sys


shared_database = {}
initialization_complete = False
processing_attempts = 0
successful_processes = 0
failed_processes = 0


init_done_event = threading.Event()
stats_lock = threading.Lock()


def initialize_database():
    """
    Initializes the shared database with default values.
    This function simulates a database initialization process that takes time.
    """
    global shared_database, initialization_complete
    
    print("Starting database initialization...")
    

    time.sleep(1.5)
    

    shared_database['config'] = {
        'host': 'localhost',
        'port': 5432,
        'database': 'production_db'
    }
    
    shared_database['user_data'] = {
        'admin': {'role': 'administrator', 'permissions': 'full'},
        'user1': {'role': 'standard', 'permissions': 'read_write'},
        'user2': {'role': 'guest', 'permissions': 'read_only'}
    }
    
    shared_database['system_status'] = 'operational'
    shared_database['last_update'] = time.time()
    

    initialization_complete = True
    init_done_event.set()
    
    print("Database initialization completed successfully.")


def process_user_request(user_id, request_type):
    """
    Processes a user request by accessing the shared database.
    This function now waits for database initialization,
    removing the order violation.
    """
    global shared_database, processing_attempts, successful_processes, failed_processes
    

    with stats_lock:
        processing_attempts += 1
    


    if not init_done_event.wait(timeout=5.0):
        print(f"ERROR: Timeout waiting for database initialization for user {user_id}")
        with stats_lock:
            failed_processes += 1
        return
    

    if random.random() < 0.15:
        processing_delay = 0
        print(f"Fast processing for user {user_id}")
    else:
        processing_delay = random.uniform(2.0, 3.0)
        print(f"Normal processing for user {user_id} (delay: {processing_delay:.2f}s)")
    
    time.sleep(processing_delay)
    
    print(f"Attempting to process {request_type} request for user {user_id}...")
    

    if 'config' in shared_database and 'user_data' in shared_database:
        if user_id in shared_database['user_data']:
            user_info = shared_database['user_data'][user_id]
            system_status = shared_database['system_status']
            
            print(f"SUCCESS: Processing {request_type} for {user_id}")
            print(f"  User role: {user_info['role']}")
            print(f"  User permissions: {user_info['permissions']}")
            print(f"  System status: {system_status}")
            print(f"  Database host: {shared_database['config']['host']}")
            
            with stats_lock:
                successful_processes += 1
        else:
            print(f"ERROR: User {user_id} not found in database")
            with stats_lock:
                failed_processes += 1
    else:

        print(f"ERROR: Database not ready for user {user_id}")
        print(f"  Available keys: {list(shared_database.keys())}")
        print(f"  Initialization status: {initialization_complete}")
        with stats_lock:
            failed_processes += 1


def monitor_system():
    """
    Monitors the system status and reports statistics.
    """
    global processing_attempts, successful_processes, failed_processes
    

    time.sleep(0.5)
    
    print("\n--- System Monitor Started ---")
    
    for i in range(8):
        time.sleep(0.5)

        with stats_lock:
            attempts = processing_attempts
            successes = successful_processes
            failures = failed_processes
        
        print(f"Monitor [{i+1}/8]: Attempts={attempts}, "
              f"Success={successes}, Failed={failures}")
    
    print("--- System Monitor Ended ---\n")


def generate_load():
    """
    Generates additional load on the system to increase chances of concurrency.
    """
    time.sleep(0.2)
    
    for i in range(3):
        print(f"Load generator: Creating background task {i+1}")
        time.sleep(0.3)

        temp_data = {'load_test': f'task_{i}'}
        time.sleep(0.1)


def main():
    """
    Main function that demonstrates the (fixed) order scenario.
    """
    print("=== Order Violation Demonstration (FIXED) ===")
    print("This program shows how to avoid order violations by using proper synchronization.\n")
    

    threads = []
    

    user_thread1 = threading.Thread(
        target=process_user_request, 
        args=('admin', 'data_query'),
        name='UserProcessor-1'
    )
    

    user_thread2 = threading.Thread(
        target=process_user_request, 
        args=('user1', 'file_upload'),
        name='UserProcessor-2'
    )
    

    init_thread = threading.Thread(
        target=initialize_database,
        name='DatabaseInitializer'
    )
    

    monitor_thread = threading.Thread(
        target=monitor_system,
        name='SystemMonitor'
    )
    

    load_thread = threading.Thread(
        target=generate_load,
        name='LoadGenerator'
    )
    

    threads.extend([user_thread1, user_thread2, init_thread, monitor_thread, load_thread])
    


    print("Starting all threads...")
    for thread in threads:
        thread.start()
        print(f"  Started: {thread.name}")
    
    print("\nAll threads started. Waiting for completion...\n")
    

    for thread in threads:
        thread.join()
        print(f"  Completed: {thread.name}")
    

    print(f"\n=== Final Results ===")
    print(f"Database initialization status: {initialization_complete}")
    print(f"Final database keys: {list(shared_database.keys())}")
    
    with stats_lock:
        print(f"Total processing attempts: {processing_attempts}")
        print(f"Successful processes: {successful_processes}")
        print(f"Failed processes: {failed_processes}")
    
    if failed_processes > 0:
        print(f"\n⚠️  Some requests failed (e.g., invalid users), "
              f"but not due to initialization order.")
    else:
        print(f"\n✅ No order violations or request failures detected this run.")


if __name__ == "__main__":
    main()
