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
    
    The original version tried to acquire the same semaphore twice in the
    same thread without releasing it in between, which caused a self-deadlock.
    
    This fixed version follows the same idea as the Java examples:
    - We keep a consistent locking discipline.
    - We never try to re-acquire the same lock while we already hold it.
    - If we need the same resource again, we release it first, then acquire it later.
    """
    safe_print("Starting fixed task (no deadlock)...")
    
    try:
        
        safe_print("Attempting first semaphore acquisition...")
        with resource_semaphore:
            safe_print("SUCCESS: First semaphore acquisition completed")
            critical_section_work()
            safe_print("First critical section completed; semaphore will be released now")

        
        safe_print("Doing some work outside the critical section...")
        time.sleep(1)

        
        safe_print("Attempting second semaphore acquisition (after releasing the first)...")
        with resource_semaphore:
            safe_print("SUCCESS: Second semaphore acquisition completed")
            safe_print("Performing additional work in second critical section...")
            critical_section_work()
            safe_print("Second critical section completed; semaphore will be released now")

        safe_print("Task finished without deadlock")

    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        safe_print("Task cleanup completed")

def monitoring_task():
    """A separate task to monitor the (previously) deadlock situation"""
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
    """Handler to demonstrate timeout-based deadlock detection"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)
    
    if main_thread.is_alive():
        safe_print("!!!!!!!!!!DEADLOCK DETECTED: Main thread still running after timeout!!!!!!!!!")
        safe_print("In a real application, this is where you'd implement recovery logic")
    else:
        safe_print("Main thread completed normally")


safe_print("=== SEMAPHORE DEADLOCK EXAMPLE (FIXED VERSION) ===")
safe_print("This program now avoids deadlock by not acquiring the same semaphore twice")
safe_print("without releasing it in between.")
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
    safe_print("Main thread is still running (unexpected in fixed version)")
else:
    safe_print("Main thread completed successfully (no deadlock)")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("In the fixed version, the main thread terminates cleanly without deadlock.")
