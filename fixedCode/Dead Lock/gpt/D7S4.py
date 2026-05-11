import threading
import time
import random


resource_condition = threading.Condition()


stats_lock = threading.Lock()


database_ready = False
network_connection_active = False
user_authentication_complete = False
system_initialization_finished = False


total_operations_completed = 0
failed_operations_count = 0
successful_operations_count = 0


max_retry_attempts = 3
operation_timeout_seconds = 5


def _record_success():
    global total_operations_completed, successful_operations_count
    with stats_lock:
        total_operations_completed += 1
        successful_operations_count += 1


def _record_failure():
    global failed_operations_count
    with stats_lock:
        failed_operations_count += 1


def database_worker_thread():
    global database_ready

    thread_id = threading.current_thread().ident
    print(f"[Database Worker {thread_id}] Starting database worker thread...")

    
    with resource_condition:
        print(f"[Database Worker {thread_id}] Waiting for database to be ready...")
        ok = resource_condition.wait_for(
            lambda: database_ready,
            timeout=operation_timeout_seconds * max_retry_attempts
        )

    if not ok:
        print(f"[Database Worker {thread_id}] ERROR: Timed out waiting for database readiness.")
        _record_failure()
        return

    print(f"[Database Worker {thread_id}] Database is now ready! Proceeding with operations...")

    for operation_number in range(1, 6):
        print(f"[Database Worker {thread_id}] Executing database operation #{operation_number}")
        time.sleep(random.uniform(0.1, 0.3))
        _record_success()

    print(f"[Database Worker {thread_id}] All database operations completed successfully")


def network_worker_thread():
    global network_connection_active

    thread_id = threading.current_thread().ident
    print(f"[Network Worker {thread_id}] Starting network worker thread...")

    with resource_condition:
        print(f"[Network Worker {thread_id}] Waiting for network connection to be active...")
        ok = resource_condition.wait_for(
            lambda: network_connection_active,
            timeout=operation_timeout_seconds * max_retry_attempts
        )

    if not ok:
        print(f"[Network Worker {thread_id}] ERROR: Timed out waiting for network activation.")
        _record_failure()
        return

    print(f"[Network Worker {thread_id}] Network connection is now active! Proceeding with operations...")

    for request_number in range(1, 4):
        print(f"[Network Worker {thread_id}] Processing network request #{request_number}")
        time.sleep(random.uniform(0.2, 0.4))
        _record_success()

    print(f"[Network Worker {thread_id}] All network operations completed successfully")


def authentication_worker_thread():
    global user_authentication_complete

    thread_id = threading.current_thread().ident
    print(f"[Auth Worker {thread_id}] Starting authentication worker thread...")

    with resource_condition:
        print(f"[Auth Worker {thread_id}] Waiting for authentication system to be ready...")
        ok = resource_condition.wait_for(
            lambda: user_authentication_complete,
            timeout=operation_timeout_seconds * max_retry_attempts
        )

    if not ok:
        print(f"[Auth Worker {thread_id}] ERROR: Timed out waiting for authentication readiness.")
        _record_failure()
        return

    print(f"[Auth Worker {thread_id}] Authentication system is ready! Proceeding with operations...")

    user_accounts = ["user1", "user2", "admin", "guest"]
    for username in user_accounts:
        print(f"[Auth Worker {thread_id}] Authenticating user: {username}")
        time.sleep(random.uniform(0.1, 0.2))
        _record_success()

    print(f"[Auth Worker {thread_id}] All authentication operations completed successfully")


def system_initialization_thread():
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
        resource_condition.notify_all()  
        print(f"[System Init {thread_id}] Database connection established!")

        print(f"[System Init {thread_id}] Establishing network connection...")
        time.sleep(0.3)
        network_connection_active = True
        resource_condition.notify_all()  
        print(f"[System Init {thread_id}] Network connection established!")

        print(f"[System Init {thread_id}] Setting up authentication system...")
        time.sleep(0.4)
        user_authentication_complete = True
        resource_condition.notify_all()  
        print(f"[System Init {thread_id}] Authentication system ready!")

        system_initialization_finished = True
        resource_condition.notify_all()
        print(f"[System Init {thread_id}] All system resources have been initialized!")


def monitoring_thread():
    global total_operations_completed, successful_operations_count, failed_operations_count
    global database_ready, network_connection_active, user_authentication_complete, system_initialization_finished

    thread_id = threading.current_thread().ident
    print(f"[Monitor {thread_id}] Starting system monitoring thread...")

    for monitoring_cycle in range(1, 11):
        with stats_lock:
            t = total_operations_completed
            s = successful_operations_count
            f = failed_operations_count

        print(f"\n[Monitor {thread_id}] === Monitoring Cycle #{monitoring_cycle} ===")
        print(f"[Monitor {thread_id}] Database Ready: {database_ready}")
        print(f"[Monitor {thread_id}] Network Active: {network_connection_active}")
        print(f"[Monitor {thread_id}] Authentication Ready: {user_authentication_complete}")
        print(f"[Monitor {thread_id}] System Initialized: {system_initialization_finished}")
        print(f"[Monitor {thread_id}] Total Operations: {t}")
        print(f"[Monitor {thread_id}] Successful Operations: {s}")
        print(f"[Monitor {thread_id}] Failed Operations: {f}")

        time.sleep(2.0)

    print(f"[Monitor {thread_id}] Monitoring completed")


def main():
    print("=" * 80)
    print("THREADING DEADLOCK DEMONSTRATION (FIXED)")
    print("=" * 80)

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

    
    system_init.join()
    db_worker.join()
    net_worker.join()
    auth_worker.join()

    print("\nAll threads completed successfully!")

    with stats_lock:
        t = total_operations_completed
        s = successful_operations_count
        f = failed_operations_count

    print(f"\nFinal Statistics:")
    print(f"Total Operations Completed: {t}")
    print(f"Successful Operations: {s}")
    print(f"Failed Operations: {f}")


if __name__ == "__main__":
    main()
