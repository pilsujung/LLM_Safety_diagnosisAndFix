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

def acquire_locks_in_order(needed_locks):
    """Helper to acquire multiple locks in consistent global order."""
    acquired = []
    lock_order = {
        'db': database_lock,
        'fs': file_system_lock,
        'net': network_resource_lock,
        'cache': cache_lock
    }
    ordered_locks = sorted([lock_order[name] for name in needed_locks], 
                          key=lambda x: id(x))  
    
    for lock in ordered_locks:
        if not lock.acquire(timeout=1.0):
            
            for acquired_lock in reversed(acquired):
                acquired_lock.release()
            return None
        acquired.append(lock)
    return acquired

def database_worker_thread():
    global shared_database_value, shared_file_content
    print(f"DB-Worker-{threading.current_thread().name}: Starting")
    
    locks = acquire_locks_in_order(['db', 'fs'])
    if not locks:
        print(f"DB-Worker-{threading.current_thread().name}: Failed to acquire locks")
        return
    
    try:
        time.sleep(random.uniform(0.1, 0.5))
        shared_database_value += 1
        print(f"DB-Worker-{threading.current_thread().name}: DB={shared_database_value}")
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_file_content.append(f"DB entry {shared_database_value}")
        print(f"DB-Worker-{threading.current_thread().name}: File updated")
        
    finally:
        for lock in reversed(locks):
            lock.release()
    print(f"DB-Worker-{threading.current_thread().name}: Completed")

def file_system_worker_thread():
    global shared_database_value, shared_file_content
    print(f"FS-Worker-{threading.current_thread().name}: Starting")
    
    locks = acquire_locks_in_order(['db', 'fs'])
    if not locks:
        print(f"FS-Worker-{threading.current_thread().name}: Failed to acquire locks")
        return
    
    try:
        time.sleep(random.uniform(0.1, 0.5))
        shared_file_content.append("FS operation")
        print(f"FS-Worker-{threading.current_thread().name}: File updated")
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_database_value += 10
        print(f"FS-Worker-{threading.current_thread().name}: DB={shared_database_value}")
        
    finally:
        for lock in reversed(locks):
            lock.release()
    print(f"FS-Worker-{threading.current_thread().name}: Completed")

def network_worker_thread():
    global shared_network_data, shared_cache_entries
    print(f"Net-Worker-{threading.current_thread().name}: Starting")
    
    locks = acquire_locks_in_order(['net', 'cache'])
    if not locks:
        print(f"Net-Worker-{threading.current_thread().name}: Failed to acquire locks")
        return
    
    try:
        time.sleep(random.uniform(0.2, 0.6))
        shared_network_data[f"req_{threading.current_thread().name}"] = "response"
        print(f"Net-Worker-{threading.current_thread().name}: Network done")
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_cache_entries.append(f"Net cache {threading.current_thread().name}")
        print(f"Net-Worker-{threading.current_thread().name}: Cache updated")
        
    finally:
        for lock in reversed(locks):
            lock.release()
    print(f"Net-Worker-{threading.current_thread().name}: Completed")

def cache_worker_thread():
    global shared_network_data, shared_cache_entries
    print(f"Cache-Worker-{threading.current_thread().name}: Starting")
    
    locks = acquire_locks_in_order(['net', 'cache'])
    if not locks:
        print(f"Cache-Worker-{threading.current_thread().name}: Failed to acquire locks")
        return
    
    try:
        time.sleep(random.uniform(0.2, 0.6))
        shared_cache_entries.append(f"Cache op {threading.current_thread().name}")
        print(f"Cache-Worker-{threading.current_thread().name}: Cache done")
        
        time.sleep(random.uniform(0.1, 0.3))
        shared_network_data[f"val_{threading.current_thread().name}"] = "validated"
        print(f"Cache-Worker-{threading.current_thread().name}: Network validated")
        
    finally:
        for lock in reversed(locks):
            lock.release()
    print(f"Cache-Worker-{threading.current_thread().name}: Completed")

def monitor_thread():
    print("Monitor: Starting")
    for i in range(3):
        time.sleep(1)
        locks = acquire_locks_in_order(['db', 'fs', 'net', 'cache'])
        if locks:
            print(f"Monitor #{i+1}: DB={shared_database_value}, Files={len(shared_file_content)}, "
                  f"Net={len(shared_network_data)}, Cache={len(shared_cache_entries)}")
            for lock in reversed(locks):
                lock.release()
        else:
            print(f"Monitor #{i+1}: Locks busy")
    print("Monitor: Done")


def main():
    print("=== FIXED Multi-Threading Demo (No Deadlock) ===")
    
    pass  

if __name__ == "__main__":
    main()
