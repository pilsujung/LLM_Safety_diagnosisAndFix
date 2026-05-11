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

def fixed_task():
    """
    Fixed version using RLock (Reentrant Lock).
    RLock allows the same thread to acquire it multiple times.
    Each acquire() must be matched with a release().
    """
    thread_name = threading.current_thread().name
    safe_print("Starting fixed task...")
    
    try:
        
        safe_print("Attempting first lock acquisition...")
        resource_lock.acquire()
        safe_print("SUCCESS: First lock acquisition completed")
        
        
        critical_section_work()
        
        
        safe_print("Attempting second lock acquisition...")
        time.sleep(1)
        
        
        resource_lock.acquire()
        safe_print("SUCCESS: Second lock acquisition completed (NO DEADLOCK!)")
        
        
        safe_print("Performing additional work...")
        critical_section_work()
        
        
        safe_print("Releasing second lock...")
        resource_lock.release()
        safe_print("Releasing first lock...")
        resource_lock.release()
        
        safe_print("Task completed successfully!")
        
    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        safe_print("Task cleanup completed")

def monitoring_task():
    """A separate task to monitor the execution"""
    safe_print("Monitoring task started...")
    
    for i in range(10):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System still running...")
        safe_print(f"Current shared counter value: {shared_resource_counter}")
        
        if not main_thread.is_alive():
            safe_print("Main task thread has completed successfully!")
            break
    
    safe_print("Monitoring task completed")


safe_print("=== SEMAPHORE DEADLOCK FIX DEMONSTRATION ===")
safe_print("This program demonstrates the fix using RLock (Reentrant Lock)")
safe_print("which allows the same thread to acquire it multiple times.")
safe_print("")

main_thread = threading.Thread(target=fixed_task, name="MainTask")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")

safe_print("Starting main task thread...")
main_thread.start()

safe_print("Starting monitoring thread...")
monitor_thread.start()


safe_print("Waiting for threads to complete...")
main_thread.join()
monitor_thread.join()

safe_print("=== PROGRAM COMPLETED SUCCESSFULLY ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("No deadlock occurred!")