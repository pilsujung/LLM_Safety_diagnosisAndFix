import threading
import time
import random


resource_condition = threading.Condition()


database_ready = False
network_connection_active = False
user_authentication_complete = False
system_initialization_finished = False


total_operations_completed = 0
failed_operations_count = 0
successful_operations_count = 0


max_retry_attempts = 3
operation_timeout_seconds = 5

def database_worker_thread():
    """
    Worker thread that handles database operations.
    Waits for database to be ready before proceeding.
    """
    global database_ready, total_operations_completed, successful_operations_count

    thread_id = threading.current_thread().ident
    print(f"[Database Worker {thread_id}] Starting database worker thread...")

    with resource_condition:
        print(f"[Database Worker {thread_id}] Waiting for database to be ready...")

        
        while not database_ready:
            print(f"[Database Worker {thread_id}] Database not ready, continuing to wait...")
            resource_condition.wait(timeout=operation_timeout_seconds)

        print(f"[Database Worker {thread_id}] Database is now ready! Proceeding with operations...")

    
    for operation_number in range(1, 6):
        print(f"[Database Worker {thread_id}] Executing database operation #{operation_number}")
        time.sleep(random.uniform(0.1, 0.3))  
        total_operations_completed += 1
        successful_operations_count += 1

    print(f"[Database Worker {thread_id}] All database operations completed successfully")

def network_worker_thread():
    """
    Worker thread that handles network operations.
    Waits for network connection to be active before proceeding.
    """
    global network_connection_active, total_operations_completed, successful_operations_count

    thread_id = threading.current_thread().ident
    print(f"[Network Worker {thread_id}] Starting network worker thread...")

    with resource_condition:
        print(f"[Network Worker {thread_id}] Waiting for network connection to be active...")

        
        while not network_connection_active:
            print(f"[Network Worker {thread_id}] Network connection not active, continuing to wait...")
            resource_condition.wait(timeout=operation_timeout_seconds)

        print(f"[Network Worker {thread_id}] Network connection is now active! Proceeding with operations...")

    
    for request_number in range(1, 4):
        print(f"[Network Worker {thread_id}] Processing network request #{request_number}")
        time.sleep(random.uniform(0.2, 0.4))  
        total_operations_completed += 1
        successful_operations_count += 1

    print(f"[Network Worker {thread_id}] All network operations completed successfully")

def authentication_worker_thread():
    """
    Worker thread that handles user authentication.
    Waits for authentication system to be ready.
    """
    global user_authentication_complete, total_operations_completed, successful_operations_count

    thread_id = threading.current_thread().ident
    print(f"[Auth Worker {thread_id}] Starting authentication worker thread...")

    with resource_condition:
        print(f"[Auth Worker {thread_id}] Waiting for authentication system to be ready...")

        
        while not user_authentication_complete:
            print(f"[Auth Worker {thread_id}] Authentication system not ready, continuing to wait...")
            resource_condition.wait(timeout=operation_timeout_seconds)

        print(f"[Auth Worker {thread_id}] Authentication system is ready! Proceeding with operations...")

    
    user_accounts = ["user1", "user2", "admin", "guest"]
    for username in user_accounts:
        print(f"[Auth Worker {thread_id}] Authenticating user: {username}")
        time.sleep(random.uniform(0.1, 0.2))  
        total_operations_completed += 1
        successful_operations_count += 1

    print(f"[Auth Worker {thread_id}] All authentication operations completed successfully")

def system_initialization_thread():
    """
    Thread responsible for initializing system resources.
    FIXED: Now properly notifies waiting threads to resolve deadlock.
    """
    global database_ready, network_connection_active, user_authentication_complete
    global system_initialization_finished

    thread_id = threading.current_thread().ident
    print(f"[System Init {thread_id}] Starting system initialization thread...")

    
    print(f"[System Init {thread_id}] Performing system startup procedures...")
    time.sleep(1.0)

    with resource_condition:
        print(f"[System Init {thread_id}] Initializing database connection...")
        time.sleep(0.5)
        database_ready = True
        print(f"[System Init {thread_id}] Database connection established!")

        print(f"[System Init {thread_id}] Establishing network connection...")
        time.sleep(0.3)
        network_connection_active = True
        print(f"[System Init {thread_id}] Network connection established!")

        print(f"[System Init {thread_id}] Setting up authentication system...")
        time.sleep(0.4)
        user_authentication_complete = True
        print(f"[System Init {thread_id}] Authentication system ready!")

        
        resource_condition.notify_all()
        print(f"[System Init {thread_id}] ✅ All waiting threads notified!")

    system_initialization_finished = True
    print(f"[System Init {thread_id}] All system resources have been initialized!")

def monitoring_thread():
    """
    Monitoring thread that periodically reports system status.
    Improved to stop when work is complete.
    """
    global total_operations_completed, successful_operations_count, failed_operations_count

    thread_id = threading.current_thread().ident
    print(f"[Monitor {thread_id}] Starting system monitoring thread...")

    for monitoring_cycle in range(1, 11):  
        print(f"\n[Monitor {thread_id}] === Monitoring Cycle #{monitoring_cycle} ===")
        print(f"[Monitor {thread_id}] Database Ready: {database_ready}")
        print(f"[Monitor {thread_id}] Network Active: {network_connection_active}")
        print(f"[Monitor {thread_id}] Authentication Ready: {user_authentication_complete}")
        print(f"[Monitor {thread_id}] System Initialized: {system_initialization_finished}")
        print(f"[Monitor {thread_id}] Total Operations: {total_operations_completed}")
        print(f"[Monitor {thread_id}] Successful Operations: {successful_operations_count}")
        print(f"[Monitor {thread_id}] Failed Operations: {failed_operations_count}")

        
        if total_operations_completed >= 17:
            print(f"[Monitor {thread_id}] All operations appear complete. Stopping monitoring.")
            break

        time.sleep(2.0)  

    print(f"[Monitor {thread_id}] Monitoring completed")

def main():
    """
    Main function that creates and manages all threads.
    Now demonstrates proper thread synchronization without deadlock.
    """
    print("="*80)
    print("THREADING DEADLOCK FIXED - PROPER SYNCHRONIZATION")
    print("="*80)
    print("This program demonstrates:")
    print("1. Multiple worker threads waiting for resources")
    print("2. Initialization thread properly notifies waiters")
    print("3. All threads complete successfully without deadlock")
    print("="*80)

    
    db_worker = threading.Thread(target=database_worker_thread, name="DatabaseWorker")
    net_worker = threading.Thread(target=network_worker_thread, name="NetworkWorker")
    auth_worker = threading.Thread(target=authentication_worker_thread, name="AuthWorker")

    
    system_init = threading.Thread(target=system_initialization_thread, name="SystemInit")

    
    monitor = threading.Thread(target=monitoring_thread, name="Monitor", daemon=True)

    
    print("\nStarting all threads...")
    db_worker.start()
    net_worker.start()
    auth_worker.start()
    system_init.start()
    monitor.start()

    print("All threads have been started!")
    print("\nWaiting for threads to complete...")

    try:
        
        system_init.join(timeout=10)
        print(f"[Main] System initialization completed.")

        
        db_worker.join(timeout=10)
        net_worker.join(timeout=10)
        auth_worker.join(timeout=10)

        if db_worker.is_alive() or net_worker.is_alive() or auth_worker.is_alive():
            print("\n" + "="*80)
            print("⚠️ WARNING: Some worker threads still alive after timeout!")
            print("="*80)
        else:
            print("\n" + "="*80)
            print("✅ SUCCESS: All threads completed successfully!")
            print("="*80)

    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user (Ctrl+C)")

    print(f"\nFinal Statistics:")
    print(f"Total Operations Completed: {total_operations_completed}")
    print(f"Successful Operations: {successful_operations_count}")
    print(f"Failed Operations: {failed_operations_count}")

if __name__ == "__main__":
    main()
