import threading
import time
from datetime import datetime

resource_semaphore = threading.Semaphore(1)
shared_resource_counter = 0
print_lock = threading.Lock()

def safe_print(message):
    with print_lock:
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f'[{timestamp}] {threading.current_thread().name}: {message}')

def critical_section_work():
    global shared_resource_counter
    safe_print('Performing critical section work...')
    for i in range(3):
        shared_resource_counter += 1
        safe_print(f'Counter incremented to: {shared_resource_counter}')
        time.sleep(0.1)

def fixed_task():
    safe_print('Starting FIXED task...')
    
    
    acquired = resource_semaphore.acquire(timeout=5)
    if acquired:
        try:
            safe_print('SUCCESS: First semaphore acquired')
            critical_section_work()
            time.sleep(0.5)
        finally:
            safe_print('Releasing first semaphore')
            resource_semaphore.release()
    else:
        safe_print('First acquisition timed out')
        return
    
    
    acquired = resource_semaphore.acquire(timeout=5)
    if acquired:
        try:
            safe_print('SUCCESS: Second semaphore acquired')
            shared_resource_counter += 2
            safe_print(f'Counter updated to: {shared_resource_counter}')
        finally:
            safe_print('Releasing second semaphore')
            resource_semaphore.release()
    else:
        safe_print('Second acquisition timed out')
    
    safe_print('Task completed successfully NO DEADLOCK')

def monitor_task():
    for i in range(5):
        time.sleep(1)
        safe_print(f'Monitor #{i+1}: counter={shared_resource_counter}')
        if not main_thread.is_alive():
            safe_print('Main task completed normally')
            break


print('=== FIXED SEMAPHORE DEADLOCK DEMO ===')
main_thread = threading.Thread(target=fixed_task, name='MainTask')
monitor_thread = threading.Thread(target=monitor_task, name='Monitor')

main_thread.start()
monitor_thread.start()

main_thread.join(timeout=10)
monitor_thread.join(timeout=5)

print('=== PROGRAM COMPLETED SUCCESSFULLY ===')
print(f'Final counter: {shared_resource_counter}')
