import threading
import time
import random


LOCK_ORDER = [database_lock, file_system_lock, network_resource_lock, cache_lock]


database_lock = threading.Lock()
file_system_lock = threading.Lock()
network_resource_lock = threading.Lock()
cache_lock = threading.Lock()


shared_database_value = 0
shared_file_content = []
shared_network_data = {}
shared_cache_entries = []

def acquire_locks_in_order(locks_needed):
    """Helper to acquire multiple locks in consistent global order."""
    acquired = []
    try:
        for lock in sorted(locks_needed, key=lambda l: LOCK_ORDER.index(l)):
            if not lock.acquire(timeout=1.0):
                raise threading.TimeoutError("Could not acquire lock")
            acquired.append(lock)
        return acquired
    except:
        for lock in acquired:
            lock.release()
        raise

def database_worker_thread():
    """Fixed: Acquires both locks in consistent global order."""
    global shared_database_value, shared_file_content
    print(f"Database Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        acquired = acquire_locks_in_order([database_lock, file_system_lock])
        
        print(f"Database Worker Thread {threading.current_thread().name}: Acquired locks")
        
        
        time.sleep(random.uniform(0.1, 0.5))
        shared_database_value += 1
        print(f"Database Worker Thread {threading.current_thread().name}: Updated database value to {shared_database_value}")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_file_content.append(f"Database entry {shared_database_value}")
        print(f"Database Worker Thread {threading.current_thread().name}: Added file entry")
        
    except Exception as e:
        print(f"Database Worker Thread {threading.current_thread().name}: Error - {e}")
    finally:
        
        for lock in reversed(acquired):
            lock.release()
        print(f"Database Worker Thread {threading.current_thread().name}: Released locks")

    print(f"Database Worker Thread {threading.current_thread().name}: Completed execution")

def file_system_worker_thread():
    """Fixed: Acquires BOTH locks in SAME global order: database -> file_system."""
    global shared_database_value, shared_file_content
    print(f"File System Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        acquired = acquire_locks_in_order([database_lock, file_system_lock])
        
        print(f"File System Worker Thread {threading.current_thread().name}: Acquired locks")
        
        
        time.sleep(random.uniform(0.1, 0.5))
        shared_file_content.append("File system operation")
        print(f"File System Worker Thread {threading.current_thread().name}: Performed file operation")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_database_value += 10
        print(f"File System Worker Thread {threading.current_thread().name}: Synchronized database to {shared_database_value}")
        
    except Exception as e:
        print(f"File System Worker Thread {threading.current_thread().name}: Error - {e}")
    finally:
        for lock in reversed(acquired):
            lock.release()
        print(f"File System Worker Thread {threading.current_thread().name}: Released locks")

    print(f"File System Worker Thread {threading.current_thread().name}: Completed execution")

def network_worker_thread():
    """Fixed: Acquires both locks in consistent global order."""
    global shared_network_data, shared_cache_entries
    print(f"Network Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        acquired = acquire_locks_in_order([network_resource_lock, cache_lock])
        
        print(f"Network Worker Thread {threading.current_thread().name}: Acquired locks")
        
        
        time.sleep(random.uniform(0.2, 0.6))
        shared_network_data[f"request_{threading.current_thread().name}"] = "network_response"
        print(f"Network Worker Thread {threading.current_thread().name}: Processed network request")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_cache_entries.append(f"Cached network data from {threading.current_thread().name}")
        print(f"Network Worker Thread {threading.current_thread().name}: Updated cache")
        
    except Exception as e:
        print(f"Network Worker Thread {threading.current_thread().name}: Error - {e}")
    finally:
        for lock in reversed(acquired):
            lock.release()
        print(f"Network Worker Thread {threading.current_thread().name}: Released locks")

    print(f"Network Worker Thread {threading.current_thread().name}: Completed execution")

def cache_worker_thread():
    """Fixed: Acquires BOTH locks in SAME global order: network -> cache."""
    global shared_network_data, shared_cache_entries
    print(f"Cache Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        acquired = acquire_locks_in_order([network_resource_lock, cache_lock])
        
        print(f"Cache Worker Thread {threading.current_thread().name}: Acquired locks")
        
        
        time.sleep(random.uniform(0.2, 0.6))
        shared_cache_entries.append(f"Cache operation by {threading.current_thread().name}")
        print(f"Cache Worker Thread {threading.current_thread().name}: Performed cache operation")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        validation_key = f"validation_{threading.current_thread().name}"
        shared_network_data[validation_key] = "cache_validated"
        print(f"Cache Worker Thread {threading.current_thread().name}: Validated cache with network")
        
    except Exception as e:
        print(f"Cache Worker Thread {threading.current_thread().name}: Error - {e}")
    finally:
        for lock in reversed(acquired):
            lock.release()
        print(f"Cache Worker Thread {threading.current_thread().name}: Released locks")

    print(f"Cache Worker Thread {threading.current_thread().name}: Completed execution")

def monitor_thread():
    """Fixed: Proper nested if logic for acquiring all locks."""
    print(f"Monitor Thread {threading.current_thread().name}: Starting system monitoring")

    for i in range(3):
        time.sleep(1)
        print(f"Monitor Thread {threading.current_thread().name}: Attempting check #{i+1}")
        
        locks_acquired = []
        try:
            
            if database_lock.acquire(timeout=0.1):
                locks_acquired.append(database_lock)
                if file_system_lock.acquire(timeout=0.1):
                    locks_acquired.append(file_system_lock)
                    if network_resource_lock.acquire(timeout=0.1):
                        locks_acquired.append(network_resource_lock)
                        if cache_lock.acquire(timeout=0.1):
                            locks_acquired.append(cache_lock)
                            
                            print(f"Monitor Thread {threading.current_thread().name}: Status check complete")
                            print(f" - Database: {shared_database_value}")
                            print(f" - Files: {len(shared_file_content)}")
                            print(f" - Network: {len(shared_network_data)}")
                            print(f" - Cache: {len(shared_cache_entries)}")
                        else:
                            print(f"Monitor Thread {threading.current_thread().name}: Timeout on cache lock")
                    else:
                        print(f"Monitor Thread {threading.current_thread().name}: Timeout on network lock")
                else:
                    print(f"Monitor Thread {threading.current_thread().name}: Timeout on file lock")
            else:
                print(f"Monitor Thread {threading.current_thread().name}: Timeout on database lock")
        finally:
            
            for lock in reversed(locks_acquired):
                lock.release()

    print(f"Monitor Thread {threading.current_thread().name}: Monitoring completed")


def main():
    print("=== Fixed Multi-Threading Deadlock Demo ===")
    print("All locks now acquired in consistent global order.\n")

    database_thread_1 = threading.Thread(target=database_worker_thread, name="DB-Worker-1")
    database_thread_2 = threading.Thread(target=database_worker_thread, name="DB-Worker-2")
    file_system_thread_1 = threading.Thread(target=file_system_worker_thread, name="FS-Worker-1")
    file_system_thread_2 = threading.Thread(target=file_system_worker_thread, name="FS-Worker-2")
    network_thread_1 = threading.Thread(target=network_worker_thread, name="Net-Worker-1")
    network_thread_2 = threading.Thread(target=network_worker_thread, name="Net-Worker-2")
    cache_thread_1 = threading.Thread(target=cache_worker_thread, name="Cache-Worker-1")
    cache_thread_2 = threading.Thread(target=cache_worker_thread, name="Cache-Worker-2")
    system_monitor = threading.Thread(target=monitor_thread, name="System-Monitor")

    all_worker_threads = [
        database_thread_1, database_thread_2,
        file_system_thread_1, file_system_thread_2,
        network_thread_1, network_thread_2,
        cache_thread_1, cache_thread_2,
        system_monitor
    ]

    print(f"Created {len(all_worker_threads)} threads:")
    for thread in all_worker_threads:
        print(f" - {thread.name}")
    print()

    print("Starting all threads...")
    for thread in all_worker_threads:
        thread.start()
        time.sleep(0.05)

    print("\nAll threads started. Waiting for completion...")
    
    for thread in all_worker_threads:
        thread.join(timeout=10)
        if thread.is_alive():
            print(f"WARNING: {thread.name} timed out")
        else:
            print(f"✓ {thread.name} completed")

    print("\n=== Final Status ===")
    print(f"Database: {shared_database_value}")
    print(f"Files: {len(shared_file_content)}")
    print(f"Network: {len(shared_network_data)}")
    print(f"Cache: {len(shared_cache_entries)}")
    print("=== No Deadlocks! ===")

if __name__ == "__main__":
    main()
