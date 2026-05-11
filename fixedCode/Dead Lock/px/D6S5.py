import threading
import time
import random


LOCK_ORDER = {
    'database': 0,
    'file_system': 1,
    'network': 2,
    'cache': 3
}


database_lock = threading.Lock()
file_system_lock = threading.Lock()
network_resource_lock = threading.Lock()
cache_lock = threading.Lock()


shared_database_value = 0
shared_file_content = []
shared_network_data = {}
shared_cache_entries = []

def acquire_locks_in_order(*lock_names):
    """Helper function to acquire multiple locks in predefined global order"""
    locks = []
    lock_objects = {
        'database': database_lock,
        'file_system': file_system_lock,
        'network': network_resource_lock,
        'cache': cache_lock
    }
    
    
    sorted_locks = sorted([(LOCK_ORDER[name], lock_objects[name]) for name in lock_names])
    
    try:
        for _, lock in sorted_locks:
            lock.acquire()
            locks.append(lock)
        return locks
    except:
        
        for lock in locks:
            lock.release()
        raise

def database_worker_thread():
    """
    Worker thread that accesses database and file system resources.
    Now acquires locks in consistent global order: database -> file_system
    """
    global shared_database_value, shared_file_content

    print(f"Database Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        locks = acquire_locks_in_order('database', 'file_system')
        
        print(f"Database Worker Thread {threading.current_thread().name}: Acquired database & file system locks")
        
        
        time.sleep(random.uniform(0.1, 0.5))
        shared_database_value += 1
        print(f"Database Worker Thread {threading.current_thread().name}: Updated database value to {shared_database_value}")

        
        time.sleep(random.uniform(0.1, 0.3))
        shared_file_content.append(f"Database entry {shared_database_value}")
        print(f"Database Worker Thread {threading.current_thread().name}: Added file entry")

    except Exception as e:
        print(f"Database Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    finally:
        
        for lock in reversed(locks):
            lock.release()
        print(f"Database Worker Thread {threading.current_thread().name}: Released all locks")
        print(f"Database Worker Thread {threading.current_thread().name}: Completed execution")

def file_system_worker_thread():
    """
    Worker thread that accesses file system and database resources.
    Now acquires locks in consistent global order: database -> file_system
    (same order regardless of access pattern)
    """
    global shared_database_value, shared_file_content

    print(f"File System Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        locks = acquire_locks_in_order('database', 'file_system')
        
        print(f"File System Worker Thread {threading.current_thread().name}: Acquired database & file system locks")
        
        
        time.sleep(random.uniform(0.1, 0.5))
        shared_file_content.append("File system operation")
        print(f"File System Worker Thread {threading.current_thread().name}: Performed file operation")

        
        time.sleep(random.uniform(0.1, 0.3))
        shared_database_value += 10
        print(f"File System Worker Thread {threading.current_thread().name}: Synchronized database value to {shared_database_value}")

    except Exception as e:
        print(f"File System Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    finally:
        
        for lock in reversed(locks):
            lock.release()
        print(f"File System Worker Thread {threading.current_thread().name}: Released all locks")
        print(f"File System Worker Thread {threading.current_thread().name}: Completed execution")

def network_worker_thread():
    """
    Worker thread that accesses network and cache resources.
    Now acquires locks in consistent global order: network -> cache
    """
    global shared_network_data, shared_cache_entries

    print(f"Network Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        locks = acquire_locks_in_order('network', 'cache')
        
        print(f"Network Worker Thread {threading.current_thread().name}: Acquired network & cache locks")
        
        
        time.sleep(random.uniform(0.2, 0.6))
        shared_network_data[f"request_{threading.current_thread().name}"] = "network_response"
        print(f"Network Worker Thread {threading.current_thread().name}: Processed network request")

        
        time.sleep(random.uniform(0.1, 0.3))
        shared_cache_entries.append(f"Cached network data from {threading.current_thread().name}")
        print(f"Network Worker Thread {threading.current_thread().name}: Updated cache")

    except Exception as e:
        print(f"Network Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    finally:
        
        for lock in reversed(locks):
            lock.release()
        print(f"Network Worker Thread {threading.current_thread().name}: Released all locks")
        print(f"Network Worker Thread {threading.current_thread().name}: Completed execution")

def cache_worker_thread():
    """
    Worker thread that accesses cache and network resources.
    Now acquires locks in consistent global order: network -> cache
    (same order regardless of access pattern)
    """
    global shared_network_data, shared_cache_entries

    print(f"Cache Worker Thread {threading.current_thread().name}: Starting execution")

    try:
        
        locks = acquire_locks_in_order('network', 'cache')
        
        print(f"Cache Worker Thread {threading.current_thread().name}: Acquired network & cache locks")
        
        
        time.sleep(random.uniform(0.2, 0.6))
        shared_cache_entries.append(f"Cache operation by {threading.current_thread().name}")
        print(f"Cache Worker Thread {threading.current_thread().name}: Performed cache operation")

        
        time.sleep(random.uniform(0.1, 0.3))
        validation_key = f"validation_{threading.current_thread().name}"
        shared_network_data[validation_key] = "cache_validated"
        print(f"Cache Worker Thread {threading.current_thread().name}: Validated cache with network")

    except Exception as e:
        print(f"Cache Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    finally:
        
        for lock in reversed(locks):
            lock.release()
        print(f"Cache Worker Thread {threading.current_thread().name}: Released all locks")
        print(f"Cache Worker Thread {threading.current_thread().name}: Completed execution")

def monitor_thread():
    """
    Monitoring thread that acquires all locks in global order for system monitoring.
    """
    print(f"Monitor Thread {threading.current_thread().name}: Starting system monitoring")

    for i in range(3):
        time.sleep(1)
        print(f"Monitor Thread {threading.current_thread().name}: Attempting system status check #{i+1}")

        try:
            
            locks = acquire_locks_in_order('database', 'file_system', 'network', 'cache')
            
            print(f"Monitor Thread {threading.current_thread().name}: System status check complete")
            print(f" - Database value: {shared_database_value}")
            print(f" - File entries: {len(shared_file_content)}")
            print(f" - Network data entries: {len(shared_network_data)}")
            print(f" - Cache entries: {len(shared_cache_entries)}")
            
        except threading.TimeoutError:
            print(f"Monitor Thread {threading.current_thread().name}: Could not acquire all locks for monitoring")
        except Exception as e:
            print(f"Monitor Thread {threading.current_thread().name}: Monitoring error - {e}")
        finally:
            if 'locks' in locals():
                for lock in reversed(locks):
                    lock.release()

    print(f"Monitor Thread {threading.current_thread().name}: Monitoring completed")

def main():
    """
    Main function with deadlock-free multi-threaded resource access.
    All threads now follow consistent global lock ordering.
    """
    print("=== Deadlock-Free Multi-Threading Demonstration ===")
    print("All threads acquire locks in consistent global order to prevent deadlocks.\n")

    
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
        print(f"Started thread: {thread.name}")
        time.sleep(0.05)

    print("\nAll threads started. Waiting for completion...")
    print("Deadlock prevention ensures all threads complete successfully!\n")

    
    for thread in all_worker_threads:
        try:
            thread.join(timeout=15)
            if thread.is_alive():
                print(f"WARNING: Thread {thread.name} is still running after timeout")
            else:
                print(f"Thread {thread.name} completed successfully")
        except Exception as e:
            print(f"Error waiting for thread {thread.name}: {e}")

    print("\n=== Final Resource Status ===")
    print(f"Database value: {shared_database_value}")
    print(f"File content entries: {len(shared_file_content)}")
    print(f"Network data entries: {len(shared_network_data)}")
    print(f"Cache entries: {len(shared_cache_entries)}")
    print("\n=== Deadlock-Free Execution Complete ===")

if __name__ == "__main__":
    main()
