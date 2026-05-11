import threading
import time
import random
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
    Fixed version using RLock (Reentrant Lock) which allows the same thread 
    to acquire the lock multiple times without deadlocking.
    The thread must release the lock the same number of times it acquired it.
    """
    thread_name = threading.current_thread().name
    safe_print("Starting fixed task...")
    
    try:
        
        safe_print("Attempting first lock acquisition...")
        resource_lock.acquire()
        safe_print("SUCCESS: First lock acquisition completed")
        
        
        critical_section_work()
        
        
        safe_print("Simulating second acquisition attempt...")
        time.sleep(1)
        
        
        safe_print("Attempting second lock acquisition...")
        resource_lock.acquire()
        safe_print("SUCCESS: Second lock acquisition completed (NO DEADLOCK!)")
        
        
        safe_print("Performing additional work")
        critical_section_work()
        
        
        safe_print("Releasing second lock")
        resource_lock.release()
        safe_print("Releasing first lock")
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
        
        
        if main_thread.is_alive():
            safe_print("Main task thread is still alive")
        else:
            safe_print("Main task thread has completed")
            break
    
    safe_print("Monitoring task completed")

def timeout_handler():
    """Handler to check for completion"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)
    
    if main_thread.is_alive():
        safe_print("WARNING: Main thread still running after timeout")
    else:
        safe_print("Main thread completed normally - NO DEADLOCK!")


safe_print("=== SEMAPHORE DEADLOCK RESOLVED ===")
safe_print("This program demonstrates how RLock prevents self-deadlock")
safe_print("by allowing the same thread to acquire the lock multiple times.")
safe_print("")

main_thread = threading.Thread(target=fixed_task, name="MainTask")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")
timeout_thread = threading.Thread(target=timeout_handler, name="TimeoutHandler")


safe_print("Starting main task thread...")
main_thread.start()

safe_print("Starting monitoring thread...")
monitor_thread.start()

safe_print("Starting timeout handler thread...")
timeout_thread.start()


safe_print("Waiting for main thread to complete...")
main_thread.join(timeout=20)

if main_thread.is_alive():
    safe_print("WARNING: Main thread is still running!")
else:
    safe_print("SUCCESS: Main thread completed successfully - NO DEADLOCK!")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")