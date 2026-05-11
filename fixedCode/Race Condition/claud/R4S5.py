import threading
import time
import random
from datetime import datetime


shared_dict = {}

log_list = []


dict_lock = threading.Lock()
log_lock = threading.Lock()


def update_dictionary(key, value):
    start_time = datetime.now()

    time.sleep(random.random())
    

    with dict_lock:
        original_value = shared_dict.get(key, None)
        shared_dict[key] = value
    
    end_time = datetime.now()


    log_entry = {
        'thread': threading.current_thread().name,
        'key': key,
        'original_value': original_value,
        'new_value': value,
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'race_condition': original_value is not None and original_value != value
    }
    
    with log_lock:
        log_list.append(log_entry)
    
    print(f" {threading.current_thread().name} end: {key} = {value}, start: {log_entry['start_time']}, terminated: {log_entry['end_time']}")


threads = []
for i in range(10):
    key = 'test'
    value = random.randint(1, 100)
    thread = threading.Thread(target=update_dictionary, args=(key, value))
    thread.start()
    threads.append(thread)


for thread in threads:
    thread.join()

print("  :", shared_dict)


print("\n :")
with log_lock:
    for log in log_list:
        race_condition = "data race!" if log['race_condition'] else "no data race."
        print(f"{log['thread']}: {log['key']} was {log['original_value']} -> {log['new_value']}, {race_condition}")