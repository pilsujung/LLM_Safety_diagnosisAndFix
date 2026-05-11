import threading
import time
import random
from datetime import datetime
from collections import defaultdict


shared_dict = {}

log_list = []


key_locks = defaultdict(threading.Lock)

def update_dictionary(key, value):
    start_time = datetime.now()

    time.sleep(random.random())

    lock = key_locks[key]
    with lock:
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

            'race_condition': False,

            'overwrote_existing': (original_value is not None and original_value != value),
        }


    log_list.append(log_entry)
    print(f" {threading.current_thread().name} end: {key} = {value}, "
          f"start: {log_entry['start_time']}, terminated: {log_entry['end_time']}")


threads = []
for i in range(10):
    key = 'test'
    value = random.randint(1, 100)
    thread = threading.Thread(target=update_dictionary, args=(key, value), name=f"T{i}")
    thread.start()
    threads.append(thread)


for thread in threads:
    thread.join()

print("  :", shared_dict)


print("\n :")
for log in log_list:
    print(f"{log['thread']}: {log['key']} was {log['original_value']} -> {log['new_value']}, "
          f"{'overwrote existing' if log['overwrote_existing'] else 'first/same value'}, "
          f"race: {log['race_condition']}")
