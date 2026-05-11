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

def database_worker_thread():
    """
    Worker thread that accesses database and file system resources.
    Now acquires locks in consistent order: database_lock, then file_system_lock.
    """
    global shared_database_value, shared_file_content
    print(f"Database Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        with database_lock:
            print(f"Database Worker Thread {threading.current_thread().name}: Acquired database lock")
            
            time.sleep(random.uniform(0.1, 0.5))
            shared_database_value += 1
            print(f"Database Worker Thread {threading.current_thread().name}: Updated database value to {shared_database_value}")
            
            
            with file_system_lock:
                print(f"Database Worker Thread {threading.current_thread().name}: Acquired file system lock")
                
                time.sleep(random.uniform(0.1, 0.3))
                shared_file_content.append(f"Database entry {shared_database_value}")
                print(f"Database Worker Thread {threading.current_thread().name}: Added file entry")
                print(f"Database Worker Thread {threading.current_thread().name}: Released file system lock")
            
            print(f"Database Worker Thread {threading.current_thread().name}: Released database lock")
    
    except Exception as e:
        print(f"Database Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    
    print(f"Database Worker Thread {threading.current_thread().name}: Completed execution")

def file_system_worker_thread():
    """
    Worker thread that accesses file system and database resources.
    FIXED: Now acquires locks in consistent order: database_lock FIRST, then file_system_lock.
    This prevents deadlock with database_worker_thread.
    """
    global shared_database_value, shared_file_content
    print(f"File System Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        with database_lock:
            print(f"File System Worker Thread {threading.current_thread().name}: Acquired database lock")
            
            
            with file_system_lock:
                print(f"File System Worker Thread {threading.current_thread().name}: Acquired file system lock")
                
                time.sleep(random.uniform(0.1, 0.5))
                shared_file_content.append("File system operation")
                print(f"File System Worker Thread {threading.current_thread().name}: Performed file operation")
                
                
                time.sleep(random.uniform(0.1, 0.3))
                shared_database_value += 10
                print(f"File System Worker Thread {threading.current_thread().name}: Synchronized database value to {shared_database_value}")
                
                print(f"File System Worker Thread {threading.current_thread().name}: Released file system lock")
            
            print(f"File System Worker Thread {threading.current_thread().name}: Released database lock")
    
    except Exception as e:
        print(f"File System Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    
    print(f"File System Worker Thread {threading.current_thread().name}: Completed execution")

def network_worker_thread():
    """
    Worker thread that accesses network and cache resources.
    Now acquires locks in consistent order: network_resource_lock, then cache_lock.
    """
    global shared_network_data, shared_cache_entries
    print(f"Network Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        with network_resource_lock:
            print(f"Network Worker Thread {threading.current_thread().name}: Acquired network resource lock")
            
            time.sleep(random.uniform(0.2, 0.6))
            shared_network_data[f"request_{threading.current_thread().name}"] = "network_response"
            print(f"Network Worker Thread {threading.current_thread().name}: Processed network request")
            
            
            with cache_lock:
                print(f"Network Worker Thread {threading.current_thread().name}: Acquired cache lock")
                
                time.sleep(random.uniform(0.1, 0.3))
                shared_cache_entries.append(f"Cached network data from {threading.current_thread().name}")
                print(f"Network Worker Thread {threading.current_thread().name}: Updated cache")
                print(f"Network Worker Thread {threading.current_thread().name}: Released cache lock")
            
            print(f"Network Worker Thread {threading.current_thread().name}: Released network resource lock")
    
    except Exception as e:
        print(f"Network Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    
    print(f"Network Worker Thread {threading.current_thread().name}: Completed execution")

def cache_worker_thread():
    """
    Worker thread that accesses cache and network resources.
    FIXED: Now acquires locks in consistent order: network_resource_lock FIRST, then cache_lock.
    This prevents deadlock with network_worker_thread.
    """
    global shared_network_data, shared_cache_entries
    print(f"Cache Worker Thread {threading.current_thread().name}: Starting execution")
    
    try:
        
        with network_resource_lock:
            print(f"Cache Worker Thread {threading.current_thread().name}: Acquired network resource lock")
            
            
            with cache_lock:
                print(f"Cache Worker Thread {threading.current_thread().name}: Acquired cache lock")
                
                time.sleep(random.uniform(0.2, 0.6))
                shared_cache_entries.append(f"Cache operation by {threading.current_thread().name}")
                print(f"Cache Worker Thread {threading.current_thread().name}: Performed cache operation")
                
                
                time.sleep(random.uniform(0.1, 0.3))
                validation_key = f"validation_{threading.current_thread().name}"
                shared_network_data[validation_key] = "cache_validated"
                print(f"Cache Worker Thread {threading.current_thread().name}: Validated cache with network")
                
                print(f"Cache Worker Thread {threading.current_thread().name}: Released cache lock")
            
            print(f"Cache Worker Thread {threading.current_thread().name}: Released network resource lock")
    
    except Exception as e:
        print(f"Cache Worker Thread {threading.current_thread().name}: Error occurred - {e}")
    
    print(f"Cache Worker Thread {threading.current_thread().name}: Completed execution")

def monitor_thread():
    """
    Monitoring thread that periodically checks the status of shared resources.
    This thread tries to acquire all locks to perform system monitoring.
    """
    print(f"Monitor Thread {threading.current_thread().name}: Starting system monitoring")
    
    for i in range(3):
        time.sleep(1)
        print(f"Monitor Thread {threading.current_thread().name}: Attempting system status check #{i+1}")
        
        
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
                            
                            
                            print(f"Monitor Thread {threading.current_thread().name}: System status check complete")
                            print(f"  - Database value: {shared_database_value}")
                            print(f"  - File entries: {len(shared_file_content)}")
                            print(f"  - Network data entries: {len(shared_network_data)}")
                            print(f"  - Cache entries: {len(shared_cache_entries)}")
                        else:
                            print(f"Monitor Thread {threading.current_thread().name}: Could not acquire cache lock for monitoring")
                    else:
                        print(f"Monitor Thread {threading.current_thread().name}: Could not acquire network lock for monitoring")
                else:
                    print(f"Monitor Thread {threading.current_thread().name}: Could not acquire file system lock for monitoring")
            else:
                print(f"Monitor Thread {threading.current_thread().name}: Could not acquire database lock for monitoring")
        
        finally:
            
            for lock in reversed(locks_acquired):
                lock.release()
    
    print(f"Monitor Thread {threading.current_thread().name}: Monitoring completed")

def main():
    """
    Main function that creates and manages multiple worker threads.
    FIXED: All threads now acquire locks in consistent order, preventing deadlock.
    """
    print("=== Starting Multi-Threading Fixed Deadlock Demonstration ===")
    print("This program demonstrates DEADLOCK PREVENTION with multiple threads and resources.")
    print("All threads acquire locks in a consistent order to prevent deadlocks.\n")
    print("Lock ordering: database_lock -> file_system_lock -> network_resource_lock -> cache_lock\n")
    
    
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
        database_thread_1,
        database_thread_2,
        file_system_thread_1,
        file_system_thread_2,
        network_thread_1,
        network_thread_2,
        cache_thread_1,
        cache_thread_2,
        system_monitor
    ]
    
    print(f"Created {len(all_worker_threads)} threads:")
    for thread in all_worker_threads:
        print(f"  - {thread.name}")
    print()
    
    
    print("Starting all threads...")
    for thread in all_worker_threads:
        thread.start()
        print(f"Started thread: {thread.name}")
        
        time.sleep(0.05)
    
    print("\nAll threads started. Waiting for completion...")
    print("With consistent lock ordering, no deadlock should occur!\n")
    
    
    for thread in all_worker_threads:
        try:
            
            thread.join(timeout=10)
            if thread.is_alive():
                print(f"WARNING: Thread {thread.name} is still running after timeout - possible deadlock!")
            else:
                print(f"Thread {thread.name} completed successfully")
        except Exception as e:
            print(f"Error waiting for thread {thread.name}: {e}")
    
    print("\n=== Final Resource Status ===")
    print(f"Database value: {shared_database_value}")
    print(f"File content entries: {len(shared_file_content)}")
    print(f"Network data entries: {len(shared_network_data)}")
    print(f"Cache entries: {len(shared_cache_entries)}")
    
    print("\n=== Deadlock Prevention Demonstration Complete ===")
    print("SUCCESS: All threads completed without deadlock!")

if __name__ == "__main__":
    main()