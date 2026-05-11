import threading
import time
from datetime import datetime


resource_lock = threading.RLock()


shared_resource_counter = 0


print_lock = threading.Lock()

def safe_print(message):
    """Thread-safe printing function"""
    with print_lock:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {threading.current_thread().name}: {message}")

def critical_section_work():
    """Simulate some work in the critical section"""
    global shared_resource_counter
    safe_print("Performing critical section work...")
    
    
    for i in range(3):
        shared_resource_counter += 1
        safe_print(f"Counter incremented to: {shared_resource_counter}")
        time.sleep(0.1)

def fixed_task():
    """
    FIXED VERSION: Uses RLock (reentrant lock) which allows the same thread
    to acquire the lock multiple times without deadlocking.
    """
    safe_print("Starting FIXED task...")
    
    
    safe_print("Attempting first lock acquisition...")
    with resource_lock:
        safe_print("SUCCESS: First lock acquisition completed")
        critical_section_work()
        
        
        safe_print("Simulating nested critical section scenario...")
        time.sleep(0.5)
        
        
        safe_print("Attempting SECOND nested lock acquisition...")
        with resource_lock:
            safe_print("SUCCESS: Second nested lock acquisition completed!")
            safe_print("Performing nested critical work...")
            shared_resource_counter += 1
            safe_print(f"Nested increment: {shared_resource_counter}")
    
    safe_print("All locks released - task completed successfully!")

def monitoring_task(main_thread):
    """Monitor the execution"""
    safe_print("Monitoring task started...")
    
    for i in range(5):
        time.sleep(1)
        safe_print(f"Monitor check #{i+1}")
        safe_print(f"Counter: {shared_resource_counter}")
        
        if not main_thread.is_alive():
            safe_print("Main task completed normally")
            break
    
    safe_print("Monitoring completed")


if __name__ == "__main__":
    safe_print("=== FIXED SEMAPHORE DEADLOCK DEMONSTRATION ===")
    safe_print("SOLUTION: Use RLock instead of Semaphore for reentrant needs")
    safe_print("")
    
    
    main_thread = threading.Thread(target=fixed_task, name="MainTask", daemon=True)
    monitor_thread = threading.Thread(target=monitoring_task, args=(main_thread,), name="Monitor", daemon=True)
    
    
    main_thread.start()
    monitor_thread.start()
    
    
    main_thread.join(timeout=10)
    monitor_thread.join(timeout=5)
    
    safe_print("=== PROGRAM COMPLETED SUCCESSFULLY ===")
    safe_print(f"Final counter value: {shared_resource_counter}")
