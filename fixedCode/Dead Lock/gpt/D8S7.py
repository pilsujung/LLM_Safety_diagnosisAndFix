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
    Fixed version: correctly acquires and releases the semaphore without self-deadlock.
    
    The main changes:
    - Acquire the semaphore, do work, and release it in a try/finally block.
    - If we want to use the resource again, we acquire it again *after* releasing it,
      instead of trying to acquire it twice in a row while still holding it.
    """
    thread_name = threading.current_thread().name
    safe_print("Starting task...")

    
    safe_print("Attempting first semaphore acquisition...")
    acquired = resource_semaphore.acquire(timeout=5)
    if not acquired:
        safe_print("ERROR: Could not acquire semaphore (first attempt), aborting task")
        return

    try:
        safe_print("SUCCESS: First semaphore acquisition completed")
        critical_section_work()
    finally:
        resource_semaphore.release()
        safe_print("Semaphore released after first critical section")

    
    safe_print("Doing some non-critical work between critical sections...")
    time.sleep(1)

    
    safe_print("Attempting second semaphore acquisition...")
    acquired = resource_semaphore.acquire(timeout=5)
    if not acquired:
        safe_print("ERROR: Could not acquire semaphore (second attempt), aborting task")
        return

    try:
        safe_print("SUCCESS: Second semaphore acquisition completed")
        critical_section_work()
    finally:
        resource_semaphore.release()
        safe_print("Semaphore released after second critical section")

    safe_print("Task completed without deadlock")

def monitoring_task():
    """A separate task to monitor the main task."""
    safe_print("Monitoring task started...")
    
    for i in range(10):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System still running...")
        safe_print(f"Current shared counter value: {shared_resource_counter}")
        
        
        if main_thread.is_alive():
            safe_print("Main task thread is still alive")
        else:
            safe_print("Main task thread has completed (no deadlock detected)")
            break
    
    safe_print("Monitoring task completed")

def timeout_handler():
    """Handler to demonstrate timeout-based monitoring (not deadlock now)."""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)
    
    if main_thread.is_alive():
        safe_print("WARNING: Main thread still running after 15 seconds.")
        safe_print("In a real application, you might investigate long-running operations.")
    else:
        safe_print("Main thread completed before timeout (no deadlock).")


safe_print("=== SEMAPHORE SYNCHRONIZATION DEMONSTRATION (FIXED, NO DEADLOCK) ===")
safe_print("This program now demonstrates correct semaphore usage without self-deadlock.")
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
    safe_print("ERROR: Main thread is still alive after join timeout (unexpected).")
else:
    safe_print("Main thread completed successfully")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("No deadlock occurred in this fixed version.")
