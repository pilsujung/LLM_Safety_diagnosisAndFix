import threading
import time
import random
from datetime import datetime


resource_semaphore1 = threading.Semaphore(1)
resource_semaphore2 = threading.Semaphore(1)


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
    FIXED: Resolved self-deadlock by using TWO DIFFERENT semaphores
    following the same pattern as the Java example (consistent lock ordering)
    """
    thread_name = threading.current_thread().name
    safe_print("Starting fixed task...")

    try:
        
        safe_print("Attempting first semaphore acquisition (resource1)...")
        resource_semaphore1.acquire()
        safe_print("SUCCESS: First semaphore (resource1) acquisition completed")

        
        critical_section_work()

        
        safe_print("Simulating second resource acquisition attempt...")
        time.sleep(1)

        
        safe_print("Attempting second semaphore acquisition (resource2)...")
        resource_semaphore2.acquire()
        safe_print("SUCCESS: Second semaphore (resource2) acquisition completed")

        
        safe_print("Performing additional work with both resources")
        shared_resource_counter += 10
        time.sleep(0.5)

        safe_print("Releasing second semaphore (resource2)")
        resource_semaphore2.release()

    finally:
        
        safe_print("Releasing first semaphore (resource1)")
        resource_semaphore1.release()
        safe_print("Task cleanup completed successfully")

def monitoring_task():
    """A separate task to monitor the execution"""
    safe_print("Monitoring task started...")

    for i in range(5):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System running normally")
        safe_print(f"Current shared counter value: {shared_resource_counter}")

        
        if 'main_thread' in globals() and not main_thread.is_alive():
            safe_print("Main task thread has completed")
            break

    safe_print("Monitoring task completed")


safe_print("=== SEMAPHORE DEADLOCK RESOLVED (JAVA PATTERN) ===")
safe_print("Following Java example: Using consistent lock ordering with 2 resources")
safe_print("Both 'acquisitions' use DIFFERENT semaphores in SAME ORDER.")
safe_print("")

main_thread = threading.Thread(target=fixed_task, name="MainTask")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")


safe_print("Starting main task thread...")
main_thread.start()

safe_print("Starting monitoring thread...")
monitor_thread.start()


safe_print("Waiting for main thread to complete...")
main_thread.join(timeout=20)

if main_thread.is_alive():
    safe_print("Main thread still running (timeout)")
else:
    safe_print("Main thread completed successfully")


monitor_thread.join()

safe_print("=== PROGRAM COMPLETED SUCCESSFULLY ===")
safe_print(f"Final shared counter value: {shared_resource_counter}") [execute_python:1][execute_python:2]
