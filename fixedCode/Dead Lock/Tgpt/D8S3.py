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
    FIXED VERSION (no deadlock):

    The old version tried to acquire the same semaphore twice from the
    same thread without releasing it in between, which caused a self-deadlock.

    The fixed version:
    - Acquires the semaphore exactly once.
    - Uses try/finally to guarantee release.
    - Does not perform a second nested acquire.
    """
    safe_print("Starting FIXED task (no deadlock)...")

    
    safe_print("Attempting semaphore acquisition...")
    acquired = resource_semaphore.acquire(timeout=5)
    if not acquired:
        safe_print("ERROR: Could not acquire semaphore within timeout; aborting task.")
        return

    try:
        safe_print("SUCCESS: Semaphore acquisition completed")

        
        critical_section_work()

        
        safe_print("Simulating additional processing that previously caused a second acquire...")
        time.sleep(1)

        
        safe_print("NOTE: Skipping second semaphore acquisition to avoid self-deadlock")

        
        safe_print("Performing some final work while still holding the semaphore...")
        critical_section_work()

    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        
        resource_semaphore.release()
        safe_print("Semaphore released in finally block")
        safe_print("Task cleanup completed")

def monitoring_task():
    """A separate task to monitor the (previous) deadlock situation"""
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
        safe_print("WARNING: Main thread still running after timeout (unexpected)")
    else:
        safe_print("Timeout handler: Main thread completed normally")


safe_print("=== SEMAPHORE DEADLOCK FIX DEMONSTRATION ===")
safe_print("This program shows a corrected version that does NOT deadlock.")
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
    safe_print("ERROR: Main thread is still running after join timeout (unexpected)")
else:
    safe_print("Main thread completed successfully (no deadlock)")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
