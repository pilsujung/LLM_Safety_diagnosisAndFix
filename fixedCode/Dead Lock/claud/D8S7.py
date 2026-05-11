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
        time.sleep(0.5)

def fixed_task_with_rlock():
    """
    SOLUTION 1: Using RLock (Reentrant Lock)
    RLock allows the same thread to acquire the lock multiple times.
    Each acquire() must be matched with a release().
    """
    thread_name = threading.current_thread().name
    safe_print("Starting task with RLock solution...")
    
    try:
        
        safe_print("Attempting first lock acquisition...")
        resource_lock.acquire()
        safe_print("SUCCESS: First lock acquisition completed")
        
        
        critical_section_work()
        
        
        safe_print("Attempting second lock acquisition...")
        resource_lock.acquire()
        safe_print("SUCCESS: Second lock acquisition completed (NO DEADLOCK!)")
        
        
        safe_print("Performing additional work")
        critical_section_work()
        
        
        safe_print("Releasing first lock")
        resource_lock.release()
        safe_print("Releasing second lock")
        resource_lock.release()
        safe_print("All locks released successfully!")
        
    except Exception as error:
        safe_print(f"Exception occurred: {error}")

def fixed_task_proper_structure():
    """
    SOLUTION 2: Proper lock structure - avoid nested acquisition
    This is the best practice: restructure code to avoid acquiring the same lock twice
    """
    safe_print("Starting task with proper structure...")
    
    try:
        
        safe_print("Acquiring lock...")
        resource_lock.acquire()
        safe_print("SUCCESS: Lock acquired")
        
        
        safe_print("Performing all critical section work...")
        critical_section_work()
        
        
        time.sleep(1)
        safe_print("Performing additional work...")
        critical_section_work()
        
        
        safe_print("Releasing lock...")
        resource_lock.release()
        safe_print("Lock released successfully!")
        
    except Exception as error:
        safe_print(f"Exception occurred: {error}")

def fixed_task_with_context_manager():
    """
    SOLUTION 3: Using context manager (with statement)
    This is the most Pythonic and safest approach
    """
    safe_print("Starting task with context manager...")
    
    try:
        
        with resource_lock:
            safe_print("SUCCESS: Lock acquired via context manager")
            
            
            safe_print("Performing all critical section work...")
            critical_section_work()
            
            time.sleep(1)
            safe_print("Performing additional work...")
            critical_section_work()
            
            safe_print("Exiting context - lock will be automatically released")
        
        safe_print("Lock released successfully via context manager!")
        
    except Exception as error:
        safe_print(f"Exception occurred: {error}")

def monitoring_task():
    """A separate task to monitor the system"""
    safe_print("Monitoring task started...")
    
    for i in range(5):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System running smoothly")
        safe_print(f"Current shared counter value: {shared_resource_counter}")
    
    safe_print("Monitoring task completed")


safe_print("=== DEADLOCK RESOLUTION DEMONSTRATION ===")
safe_print("This program demonstrates three solutions to the deadlock problem:")
safe_print("1. Using RLock (Reentrant Lock)")
safe_print("2. Proper lock structure (single acquisition)")
safe_print("3. Context manager (with statement)")
safe_print("")


safe_print("\n--- Testing Solution 1: RLock with multiple acquisitions ---")
thread1 = threading.Thread(target=fixed_task_with_rlock, name="RLock-Task")
thread1.start()
thread1.join()


shared_resource_counter = 0


safe_print("\n--- Testing Solution 2: Proper lock structure ---")
thread2 = threading.Thread(target=fixed_task_proper_structure, name="Proper-Task")
thread2.start()
thread2.join()


shared_resource_counter = 0


safe_print("\n--- Testing Solution 3: Context manager ---")
thread3 = threading.Thread(target=fixed_task_with_context_manager, name="Context-Task")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")

thread3.start()
monitor_thread.start()

thread3.join()
monitor_thread.join()

safe_print("\n=== PROGRAM COMPLETED SUCCESSFULLY ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("All threads completed without deadlock!")