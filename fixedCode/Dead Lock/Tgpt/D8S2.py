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
    The deadlock happened because the same thread tried to acquire the same
    semaphore twice without releasing it in between (non-reentrant lock).

    To resolve this (analogous to enforcing a single, consistent lock order
    in your Java example), we enforce a simple rule:

        "This thread acquires resource_semaphore at most ONCE."

    All critical work is done while holding that single acquisition, and the
    semaphore is released exactly once in the finally block.
    """
    safe_print("Starting fixed task (no deadlock)...")
    acquired = False
    
    try:
        
        safe_print("Attempting semaphore acquisition...")
        resource_semaphore.acquire()
        acquired = True
        safe_print("SUCCESS: Semaphore acquisition completed")
        
        
        critical_section_work()
        
        
        safe_print("Simulating additional processing while still holding semaphore...")
        time.sleep(1)
        
        safe_print("Finished all work inside critical section")
        
    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        
        if acquired:
            safe_print("Releasing semaphore")
            resource_semaphore.release()
        safe_print("Task cleanup completed")

def monitoring_task():
    """A separate task to monitor the main task status"""
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
        safe_print("WARNING: Main thread still running after timeout (check for performance issues)")
    else:
        safe_print("Main thread completed normally before timeout")


safe_print("=== SEMAPHORE DEADLOCK FIXED DEMONSTRATION ===")
safe_print("This program now uses the semaphore correctly (no self-deadlock).")
safe_print("The task acquires the semaphore only once and releases it once.")
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
    safe_print("Main thread is still running after join timeout (but should not be deadlocked).")
else:
    safe_print("Main thread completed successfully")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
