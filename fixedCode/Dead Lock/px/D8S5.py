import threading
import time
import random
from datetime import datetime


first_semaphore = threading.Semaphore(1)

second_semaphore = threading.Semaphore(1)


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
    FIXED VERSION: Demonstrates proper semaphore usage by:
    1. Using TWO DIFFERENT semaphores instead of the same one twice
    2. Always acquiring semaphores in CONSISTENT ORDER (first -> second)
    3. Properly releasing both semaphores in finally block
    """
    thread_name = threading.current_thread().name
    safe_print("Starting FIXED task...")

    try:
        
        safe_print("Attempting first semaphore acquisition...")
        first_semaphore.acquire()
        safe_print("SUCCESS: First semaphore acquisition completed")

        
        critical_section_work()

        
        safe_print("Simulating legitimate second resource acquisition...")
        time.sleep(1)

        
        safe_print("Attempting second semaphore acquisition (DIFFERENT semaphore)...")
        second_semaphore.acquire()
        safe_print("SUCCESS: Second semaphore acquisition completed")

        
        safe_print("Performing additional protected work...")
        time.sleep(0.5)

    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        
        safe_print("Releasing second semaphore...")
        second_semaphore.release()
        safe_print("Releasing first semaphore...")
        first_semaphore.release()
        safe_print("All semaphores released successfully")

def monitoring_task():
    """A separate task to monitor the execution"""
    safe_print("Monitoring task started...")

    for i in range(10):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System running normally...")
        safe_print(f"Current shared counter value: {shared_resource_counter}")

    safe_print("Monitoring task completed")

def timeout_handler():
    """Handler to verify normal completion"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)

    if main_thread.is_alive():
        safe_print("Main thread still running after timeout")
    else:
        safe_print("Main thread completed normally - NO DEADLOCK!")


safe_print("=== SEMAPHORE DEADLOCK RESOLVED DEMONSTRATION ===")
safe_print("This program demonstrates the FIXED version using:")
safe_print("1. TWO DIFFERENT semaphores")
safe_print("2. CONSISTENT lock acquisition order")
safe_print("3. Proper release in finally block")
safe_print("")

main_thread = threading.Thread(target=fixed_task, name="MainTask")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")
timeout_thread = threading.Thread(target=timeout_handler, name="TimeoutHandler")


safe_print("Starting main FIXED task thread...")
main_thread.start()

safe_print("Starting monitoring thread...")
monitor_thread.start()

safe_print("Starting timeout handler thread...")
timeout_thread.start()


safe_print("Waiting for main thread to complete...")
main_thread.join(timeout=20)

if main_thread.is_alive():
    safe_print("Main thread still alive after timeout")
else:
    safe_print("✓ Main thread completed successfully - NO DEADLOCK!")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED SUCCESSFULLY ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
