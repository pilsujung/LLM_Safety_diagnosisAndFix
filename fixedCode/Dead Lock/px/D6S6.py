import threading
import time
import random


database_lock = threading.Lock()
file_system_lock = threading.Lock()
network_resource_lock = threading.Lock()
cache_lock = threading.Lock()


LOCK_ORDER = [database_lock, file_system_lock, network_resource_lock, cache_lock]


shared_database_value = 0
shared_file_content = []
shared_network_data = {}
shared_cache_entries = []

def acquire_locks_in_order(needed_locks, timeout=2.0):
    """Acquire multiple locks in predefined global order with timeout."""
    acquired = {}
    try:
        for lock in LOCK_ORDER:
            if lock in needed_locks:
                if not lock.acquire(timeout=timeout):
                    raise TimeoutError(f"Could not acquire {lock} within {timeout}s")
                acquired[lock] = True
        return acquired
    except:
        
        for lock in reversed(list(acquired.keys())):
            lock.release()
        raise

def release_locks(acquired):
    """Release locks in reverse order."""
    for lock in reversed(list(acquired.keys())):
        lock.release()

def database_worker_thread():
    """Worker thread that accesses database and file system resources."""
    global shared_database_value, shared_file_content
    print(f"Database Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        acquired = acquire_locks_in_order({database_lock, file_system_lock})
        print(f"Database Worker Thread {threading.current_thread().name}: Acquired both locks")
        
        
        time.sleep(random.uniform(0.1, 0.5))
        shared_database_value += 1
        print(f"Database Worker Thread {threading.current_thread().name}: Updated database to {shared_database_value}")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_file_content.append(f"Database entry {shared_database_value}")
        print(f"Database Worker Thread {threading.current_thread().name}: Added file entry")
        
    except Exception as e:
        print(f"Database Worker Thread {threading.current_thread().name}: Error - {e}")
    else:
        print(f"Database Worker Thread {threading.current_thread().name}: Completed successfully")
    finally:
        if 'acquired' in locals():
            release_locks(acquired)

def file_system_worker_thread():
    """Worker thread that accesses file system and database resources."""
    global shared_database_value, shared_file_content
    print(f"File System Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        acquired = acquire_locks_in_order({database_lock, file_system_lock})
        print(f"File System Worker Thread {threading.current_thread().name}: Acquired both locks")
        
        
        time.sleep(random.uniform(0.1, 0.5))
        shared_file_content.append("File system operation")
        print(f"File System Worker Thread {threading.current_thread().name}: Performed file operation")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_database_value += 10
        print(f"File System Worker Thread {threading.current_thread().name}: Updated database to {shared_database_value}")
        
    except Exception as e:
        print(f"File System Worker Thread {threading.current_thread().name}: Error - {e}")
    else:
        print(f"File System Worker Thread {threading.current_thread().name}: Completed successfully")
    finally:
        if 'acquired' in locals():
            release_locks(acquired)

def network_worker_thread():
    """Worker thread that accesses network and cache resources."""
    global shared_network_data, shared_cache_entries
    print(f"Network Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        acquired = acquire_locks_in_order({network_resource_lock, cache_lock})
        print(f"Network Worker Thread {threading.current_thread().name}: Acquired both locks")
        
        
        time.sleep(random.uniform(0.2, 0.6))
        shared_network_data[f"request_{threading.current_thread().name}"] = "network_response"
        print(f"Network Worker Thread {threading.current_thread().name}: Processed network request")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_cache_entries.append(f"Cached network data from {threading.current_thread().name}")
        print(f"Network Worker Thread {threading.current_thread().name}: Updated cache")
        
    except Exception as e:
        print(f"Network Worker Thread {threading.current_thread().name}: Error - {e}")
    else:
        print(f"Network Worker Thread {threading.current_thread().name}: Completed successfully")
    finally:
        if 'acquired' in locals():
            release_locks(acquired)

def cache_worker_thread():
    """Worker thread that accesses cache and network resources."""
    global shared_network_data, shared_cache_entries
    print(f"Cache Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        acquired = acquire_locks_in_order({network_resource_lock, cache_lock})
        print(f"Cache Worker Thread {threading.current_thread().name}: Acquired both locks")
        
        
        time.sleep(random.uniform(0.2, 0.6))
        shared_cache_entries.append(f"Cache operation by {threading.current_thread().name}")
        print(f"Cache Worker Thread {threading.current_thread().name}: Performed cache operation")
        
        
        time.sleep(random.uniform(0.1, 0.3))
        validation_key = f"validation_{threading.current_thread().name}"
        shared_network_data[validation_key] = "cache_validated"
        print(f"Cache Worker Thread {threading.current_thread().name}: Validated cache with network")
        
    except Exception as e:
        print(f"Cache Worker Thread {threading.current_thread().name}: Error - {e}")
    else:
        print(f"Cache Worker Thread {threading.current_thread().name}: Completed successfully")
    finally:
        if 'acquired' in locals():
            release_locks(acquired)

def monitor_thread():
    """Monitoring thread that periodically checks shared resource status."""
    print(f"Monitor Thread {threading.current_thread().name}: Starting system monitoring")
    
    for i in range(3):
        time.sleep(1)
        print(f"Monitor Thread {threading.current_thread().name}: Attempting status check #{i+1}")
        locks_acquired = []
        acquired_all = True
        
        try:
            
            if not database_lock.acquire(timeout=0.1):
                print(f"Monitor: Could not acquire database lock")
                acquired_all = False
            else:
                locks_acquired.append(database_lock)
                
            if acquired_all and not file_system_lock.acquire(timeout=0.1):
                print(f"Monitor: Could not acquire file system lock")
                acquired_all = False
            elif acquired_all:
                locks_acquired.append(file_system_lock)
                
            if acquired_all and not network_resource_lock.acquire(timeout=0.1):
                print(f"Monitor: Could not acquire network lock")
                acquired_all = False
            elif acquired_all:
                locks_acquired.append(network_resource_lock)
                
            if acquired_all and not cache_lock.acquire(timeout=0.1):
                print(f"Monitor: Could not acquire cache lock")
                acquired_all = False
            elif acquired_all:
                locks_acquired.append(cache_lock)
                
            if acquired_all:
                print(f"Monitor Thread {threading.current_thread().name}: Status check complete")
                print(f" - Database value: {shared_database_value}")
                print(f" - File entries: {len(shared_file_content)}")
                print(f" - Network data: {len(shared_network_data)}")
                print(f" - Cache entries: {len(shared_cache_entries)}")
                
        finally:
            
            for lock in reversed(locks_acquired):
                lock.release()
    
    print(f"Monitor Thread {threading.current_thread().name}: Monitoring completed")

def main():
    """Main function to create and manage worker threads."""
    print("=== Fixed Multi-Threading Deadlock Demo ===")
    print("All threads now acquire locks in consistent global order.\n")

    
    database_thread_1 = threading.Thread(target=database_worker_thread, name="DB-Worker-1")
    database_thread_2 = threading.Thread(target=database_worker_thread, name="DB-Worker-2")
    file_system_thread_1 = threading.Thread(target=file_system_worker_thread, name="FS-Worker-1")
    file_system_thread_2 = threading.Thread(target=file_system_worker_thread, name="FS-Worker-2")
    network_thread_1 = threading.Thread(target=network_worker_thread, name="Net-Worker-1")
    network_thread_2 = threading.Thread(target=network_worker_thread, name="Net-Worker-2")
    cache_thread_1 = threading.Thread(target=cache_worker_thread, name="Cache-Worker-1")
    cache_thread_2 = threading.Thread(target=cache_worker_thread, name="Cache-Worker-2")
    system_monitor = threading.Thread(target=monitor_thread, name="System-Monitor")

    all_threads = [
        database_thread_1, database_thread_2,
        file_system_thread_1, file_system_thread_2,
        network_thread_1, network_thread_2,
        cache_thread_1, cache_thread_2,
        system_monitor
    ]

    print(f"Created {len(all_threads)} threads:")
    for thread in all_threads:
        print(f" - {thread.name}")
    print()

    
    print("Starting all threads...")
    for thread in all_threads:
        thread.start()
        print(f"Started: {thread.name}")
        time.sleep(0.05)

    print("\nWaiting for completion...")
    
    
    for thread in all_threads:
        thread.join(timeout=15)
        if thread.is_alive():
            print(f"WARNING: {thread.name} timed out!")
        else:
            print(f"✓ {thread.name} completed")

    print("\n=== Final Status ===")
    print(f"Database value: {shared_database_value}")
    print(f"File entries: {len(shared_file_content)}")
    print(f"Network data: {len(shared_network_data)}")
    print(f"Cache entries: {len(shared_cache_entries)}")
    print("\n=== NO DEADLOCKS! All threads completed successfully ===")

if __name__ == "__main__":
    main()
