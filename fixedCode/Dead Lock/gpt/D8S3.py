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

def critical_section_work(phase_label=""):
    """Simulate some work in the critical section"""
    global shared_resource_counter
    if phase_label:
        safe_print(f"Performing critical section work ({phase_label})...")
    else:
        safe_print("Performing critical section work...")

    
    for i in range(3):
        shared_resource_counter += 1
        safe_print(f"[{phase_label}] Counter incremented to: {shared_resource_counter}")
        time.sleep(0.5)

def problematic_task():
    """
    Corrected version:
    - Acquire the semaphore ONCE.
    - Do all critical work while holding it.
    - Release it exactly once.
    This avoids self-deadlock.
    """
    safe_print("Starting fixed task (no deadlock)...")

    try:
        safe_print("Attempting semaphore acquisition...")
        
        with resource_semaphore:
            safe_print("SUCCESS: Semaphore acquisition completed")

            
            critical_section_work("phase 1")

            
            safe_print("Simulating additional processing inside critical section...")
            time.sleep(1)

            
            critical_section_work("phase 2")

            safe_print("All critical work completed, semaphore will be released automatically")

    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        
        safe_print("Task cleanup completed (no deadlock)")

def monitoring_task():
    """A separate task to monitor the main task"""
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
    """Handler to demonstrate timeout-based deadlock detection (now just a safety net)"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)

    if main_thread.is_alive():
        safe_print("WARNING: Main thread still running after timeout (long-running task)")
        safe_print("In a real application, this is where you'd implement recovery logic")
    else:
        safe_print("Main thread completed normally")


safe_print("=== SEMAPHORE DEADLOCK FIXED DEMONSTRATION ===")
safe_print("This program now uses the semaphore correctly and does NOT deadlock.")
safe_print("All work that needs exclusive access is done inside a single acquisition.")
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
    safe_print("UNEXPECTED: Main thread is still alive after timeout.")
else:
    safe_print("Main thread completed successfully")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
