import threading
import time
from datetime import datetime


resource_lock = threading.RLock()
shared_resource_counter = 0
print_lock = threading.Lock()

def safe_print(message):
    with print_lock:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {threading.current_thread().name}: {message}")

def critical_section_work():
    global shared_resource_counter
    safe_print("Performing critical section work...")
    for i in range(3):
        shared_resource_counter += 1
        safe_print(f"Counter incremented to: {shared_resource_counter}")
        time.sleep(0.1)  

def fixed_task():
    safe_print("Starting fixed task...")
    
    
    with resource_lock:
        safe_print("SUCCESS: First lock acquisition")
        critical_section_work()
        
        safe_print("Simulating processing...")
        time.sleep(0.5)
        
        
        safe_print("Attempting second lock acquisition...")
        with resource_lock:
            safe_print("SUCCESS: Second lock acquisition completed")
            safe_print("Performing additional work")
            critical_section_work()

def monitoring_task():
    safe_print("Monitoring started")
    time.sleep(5)
    safe_print("Monitoring completed")


safe_print("=== FIXED SEMAPHORE DEADLOCK DEMO ===")
main_thread = threading.Thread(target=fixed_task, name="MainTask")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")

main_thread.start()
monitor_thread.start()

main_thread.join()
monitor_thread.join()

safe_print(f"=== COMPLETED SUCCESSFULLY === Final counter: {shared_resource_counter}")
