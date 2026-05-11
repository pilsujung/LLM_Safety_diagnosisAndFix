import threading
import time
import random
from datetime import datetime


resource_semaphore = threading.Semaphore(1)


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

def problematic_task():
    """
    FIXED VERSION:
    - Acquire the semaphore once (with timeout for safety).
    - Simulate a mistaken second acquisition using non-blocking acquire()
      so we *detect and avoid* the deadlock instead of causing it.
    - Ensure every successful acquire has a matching release in finally.
    """
    thread_name = threading.current_thread().name
    safe_print("Starting (fixed) problematic task...")

    first_acquired = False
    second_acquired = False

    try:
        
        safe_print("Attempting first semaphore acquisition...")
        first_acquired = resource_semaphore.acquire(timeout=5)

        if not first_acquired:
            safe_print("ERROR: Could not acquire semaphore within timeout. Aborting task.")
            return

        safe_print("SUCCESS: First semaphore acquisition completed")
        
        
        critical_section_work()
        
        
        safe_print("Simulating accidental second acquisition attempt...")
        time.sleep(1)  
        
        
        safe_print("Attempting second semaphore acquisition (non-blocking)...")
        second_acquired = resource_semaphore.acquire(blocking=False)

        if not second_acquired:
            safe_print("AVOIDED DEADLOCK: Second acquisition failed immediately "
                       "(semaphore already held by this thread).")
        else:
            
            
            safe_print("Second semaphore acquisition unexpectedly succeeded.")
            safe_print("Performing additional work under second acquisition...")
            critical_section_work()

    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        
        if second_acquired:
            safe_print("Releasing second semaphore")
            resource_semaphore.release()
        if first_acquired:
            safe_print("Releasing first semaphore")
            resource_semaphore.release()

        safe_print("Task cleanup completed (no deadlock).")

def monitoring_task():
    """A separate task to monitor the situation"""
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
    """Handler to demonstrate timeout-based monitoring"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)
    
    if main_thread.is_alive():
        safe_print("WARNING: Main thread still running after timeout (but no deadlock expected).")
    else:
        safe_print("Main thread completed normally")


safe_print("=== SEMAPHORE DEADLOCK DEMONSTRATION (FIXED) ===")
safe_print("This program previously demonstrated how a thread can deadlock itself")
safe_print("by trying to acquire the same semaphore twice.")
safe_print("The code has been fixed to avoid that deadlock.")
safe_print("")

main_thread = threading.Thread(target=problematic_task, name="MainTask")
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
    safe_print("Main thread is still running (unexpected but not deadlocked).")
else:
    safe_print("Main thread completed successfully")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("No deadlock occurred in this run.")
