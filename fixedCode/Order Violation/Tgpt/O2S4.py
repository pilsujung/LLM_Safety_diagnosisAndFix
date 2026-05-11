import threading
import time
from datetime import datetime

shared_data = None
lock = threading.Lock()

writer_done = threading.Event()
extra_writer_done = threading.Event()
violations = []
action_log = []

def now():
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def record_action(who, action):
    action_log.append((now(), who, action))

def record_violation(who, reason):
    ts = now()
    violations.append((ts, who, reason))
    log(f"[OrderViolation] {who}: {reason}")

def writer():
    global shared_data
    record_action("Writer", "start")
    log("[Writer] Writing data...")
    time.sleep(1)
    with lock:
        shared_data = "Important Data"
    writer_done.set()
    record_action("Writer", "finished")
    log("[Writer] Finished writing.")

def reader():
    global shared_data
    record_action("Reader", "start")
    log("[Reader] Waiting for Writer to finish...")


    if not writer_done.wait(timeout=5):

        record_violation("Reader", "timeout waiting for Writer")
        return

    with lock:
        if shared_data is None:
            log("[Reader] WARNING: Attempted to read before data was written!")
        else:
            log(f"[Reader] Read data: {shared_data}")
    record_action("Reader", "finished")

def extra_writer():
    global shared_data
    record_action("ExtraWriter", "start")
    time.sleep(3)
    log("[Extra Writer] Modifying data...")
    with lock:
        shared_data = "Updated Data"
    extra_writer_done.set()
    record_action("ExtraWriter", "finished")
    log("[Extra Writer] Finished modifying data.")

def extra_reader():
    global shared_data
    record_action("ExtraReader", "start")
    log("[Extra Reader] Waiting for ExtraWriter to finish...")


    if not extra_writer_done.wait(timeout=5):
        record_violation("ExtraReader", "timeout waiting for ExtraWriter")
        return

    with lock:
        if shared_data is None:
            log("[Extra Reader] WARNING: Attempted to read before data was written!")
        else:
            log(f"[Extra Reader] Read updated data: {shared_data}")
    record_action("ExtraReader", "finished")


thread1 = threading.Thread(target=writer)
thread2 = threading.Thread(target=reader)
thread3 = threading.Thread(target=extra_writer)
thread4 = threading.Thread(target=extra_reader)

thread1.start(); thread2.start(); thread3.start(); thread4.start()
thread1.join();  thread2.join();  thread3.join();  thread4.join()

log("[Main] Program finished.")
print("\n===== ACTION LOG =====")
for ts, who, action in action_log:
    print(f"{ts} | {who:<12} | {action}")

print("\n===== ORDER VIOLATIONS =====")
if violations:
    for ts, who, reason in violations:
        print(f"{ts} | {who:<12} | {reason}")
else:
    print("No order violations detected.")
