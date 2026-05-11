import threading
import time
import random

database_lock = threading.Lock()
file_system_lock = threading.Lock()
network_resource_lock = threading.Lock()
cache_lock = threading.Lock()

shared_database_value = 0
shared_file_content = []
shared_network_data = {}
shared_cache_entries = []

def database_worker_thread():
    global shared_database_value, shared_file_content
    print(f"Database Worker Thread {threading.current_thread().name}: Starting execution")
    try:
        
        with database_lock, file_system_lock:
            print(f"Database Worker Thread {threading.current_thread().name}: Acquired database+file locks")
            time.sleep(random.uniform(0.1, 0.5))
            shared_database_value += 1
            print(f"Database Worker Thread {threading.current_thread().name}: Updated database to {shared_database_value}")
            time.sleep(random.uniform(0.1, 0.3))
            shared_file_content.append(f"Database entry {shared_database_value}")
            print(f"Database Worker Thread {threading.current_thread().name}: Added file entry")
    except Exception as e:
        print(f"Database Worker Thread {threading.current_thread().name}: Error - {e}")
    print(f"Database Worker Thread {threading.current_thread().name}: Completed")

def file_system_worker_thread():
    global shared_database_value, shared_file_content
    print(f"File System Worker Thread {threading.current_thread().name}: Starting execution")
    try:
        
        with database_lock, file_system_lock:
            print(f"File System Worker Thread {threading.current_thread().name}: Acquired database+file locks")
            time.sleep(random.uniform(0.1, 0.5))
            shared_file_content.append("File system operation")
            print(f"File System Worker Thread {threading.current_thread().name}: Performed file operation")
            time.sleep(random.uniform(0.1, 0.3))
            shared_database_value += 10
            print(f"File System Worker Thread {threading.current_thread().name}: Updated database to {shared_database_value}")
    except Exception as e:
        print(f"File System Worker Thread {threading.current_thread().name}: Error - {e}")
    print(f"File System Worker Thread {threading.current_thread().name}: Completed")

def network_worker_thread():
    global shared_network_data, shared_cache_entries
    print(f"Network Worker Thread {threading.current_thread().name}: Starting execution")
    try:
        
        with network_resource_lock, cache_lock:
            print(f"Network Worker Thread {threading.current_thread().name}: Acquired network+cache locks")
            time.sleep(random.uniform(0.2, 0.6))
            shared_network_data[f"request_{threading.current_thread().name}"] = "network_response"
            print(f"Network Worker Thread {threading.current_thread().name}: Processed network request")
            time.sleep(random.uniform(0.1, 0.3))
            shared_cache_entries.append(f"Cached network data from {threading.current_thread().name}")
            print(f"Network Worker Thread {threading.current_thread().name}: Updated cache")
    except Exception as e:
        print(f"Network Worker Thread {threading.current_thread().name}: Error - {e}")
    print(f"Network Worker Thread {threading.current_thread().name}: Completed")

def cache_worker_thread():
    global shared_network_data, shared_cache_entries
    print(f"Cache Worker Thread {threading.current_thread().name}: Starting execution")
    try:
        
        with network_resource_lock, cache_lock:
            print(f"Cache Worker Thread {threading.current_thread().name}: Acquired network+cache locks")
            time.sleep(random.uniform(0.2, 0.6))
            shared_cache_entries.append(f"Cache operation by {threading.current_thread().name}")
            print(f"Cache Worker Thread {threading.current_thread().name}: Performed cache operation")
            time.sleep(random.uniform(0.1, 0.3))
            validation_key = f"validation_{threading.current_thread().name}"
            shared_network_data[validation_key] = "cache_validated"
            print(f"Cache Worker Thread {threading.current_thread().name}: Validated with network")
    except Exception as e:
        print(f"Cache Worker Thread {threading.current_thread().name}: Error - {e}")
    print(f"Cache Worker Thread {threading.current_thread().name}: Completed")

def monitor_thread():
    print(f"Monitor Thread {threading.current_thread().name}: Starting system monitoring")
    for i in range(3):
        time.sleep(1)
        print(f"Monitor Thread {threading.current_thread().name}: Check #{i+1}")
        locks_acquired = []
        try:
            
            if database_lock.acquire(timeout=0.1): locks_acquired.append(database_lock)
            else: continue
            if file_system_lock.acquire(timeout=0.1): locks_acquired.append(file_system_lock)
            else: continue
            if network_resource_lock.acquire(timeout=0.1): locks_acquired.append(network_resource_lock)
            else: continue
            if cache_lock.acquire(timeout=0.1): locks_acquired.append(cache_lock)
            else: continue
            
            print(f"Monitor Thread {threading.current_thread().name}: Status - DB:{shared_database_value}, Files:{len(shared_file_content)}, Net:{len(shared_network_data)}, Cache:{len(shared_cache_entries)}")
        finally:
            for lock in reversed(locks_acquired):
                lock.release()
    print(f"Monitor Thread {threading.current_thread().name}: Completed")

def main():
    print("=== Fixed Deadlock Demo ===")
    threads = [
        threading.Thread(target=database_worker_thread, name="DB-1"),
        threading.Thread(target=database_worker_thread, name="DB-2"),
        threading.Thread(target=file_system_worker_thread, name="FS-1"),
        threading.Thread(target=file_system_worker_thread, name="FS-2"),
        threading.Thread(target=network_worker_thread, name="Net-1"),
        threading.Thread(target=network_worker_thread, name="Net-2"),
        threading.Thread(target=cache_worker_thread, name="Cache-1"),
        threading.Thread(target=cache_worker_thread, name="Cache-2"),
        threading.Thread(target=monitor_thread, name="Monitor")
    ]
    
    for t in threads: t.start()
    for t in threads: t.join(timeout=15)
    
    print(f"\nFinal: DB={shared_database_value}, Files={len(shared_file_content)}, Net={len(shared_network_data)}, Cache={len(shared_cache_entries)}")

if __name__ == "__main__":
    main()
