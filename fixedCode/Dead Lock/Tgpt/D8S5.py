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

    Previously, this function tried to acquire the same semaphore twice:
      - First acquire() succeeded
      - Second acquire() blocked forever because the same thread already held it

    Now we acquire the semaphore exactly once, perform all needed work
    while holding it, and then release it. This is analogous to the Java
    fixes where we enforce a correct locking strategy.
    """
    thread_name = threading.current_thread().name
    safe_print("Starting fixed task (no deadlock)...")

    try:
        safe_print("Attempting semaphore acquisition...")
        
        with resource_semaphore:
            safe_print("SUCCESS: Semaphore acquisition completed")

            
            critical_section_work()

            
            safe_print("Simulating additional processing inside critical section...")
            time.sleep(1)

            
            
            safe_print("Continuing work WITHOUT a second semaphore acquisition...")
            critical_section_work()

            safe_print("All work completed inside a single critical section")

        
        safe_print("Semaphore released normally (no deadlock)")

    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        safe_print("Task cleanup (this will always execute now)")

def monitoring_task():
    """A separate task to monitor the system state"""
    safe_print("Monitoring task started...")

    for i in range(10):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System still running...")
        safe_print(f"Current shared counter value: {shared_resource_counter}")

        
        if main_thread.is_alive():
            safe_print("Main task thread is still running (no deadlock expected)")
        else:
            safe_print("Main task thread has completed")
            break

    safe_print("Monitoring task completed")

def timeout_handler():
    """Handler to demonstrate timeout-based monitoring (not deadlock anymore)"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)

    if main_thread.is_alive():
        safe_print("WARNING: Main thread still running after timeout.")
        safe_print("This may indicate slow processing, but not a self-deadlock anymore.")
    else:
        safe_print("Main thread completed before timeout - no deadlock detected.")


safe_print("=== SEMAPHORE DEADLOCK DEMONSTRATION (FIXED VERSION) ===")
safe_print("Originally, this program showed a self-deadlock by acquiring")
safe_print("the same semaphore twice from the same thread.")
safe_print("This fixed version uses a single acquisition pattern.")
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
    safe_print("Main thread is still running after join timeout.")
    safe_print("In the original version this meant deadlock;")
    safe_print("now it would just indicate long-running work.")
else:
    safe_print("Main thread completed successfully (no deadlock).")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("No self-deadlock occurs in this fixed version.")
