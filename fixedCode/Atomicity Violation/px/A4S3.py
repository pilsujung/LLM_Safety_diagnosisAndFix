import threading
import time
import random
import datetime

class ChatRoom:
    def __init__(self):
        self.message_log = []
        self.atomic_violations = []
        self.lock = threading.Lock()                              

    def send_message(self, user_id, message_content):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")
                                                       
        with self.lock:
            intended_position = len(self.message_log)
            message_entry = {
                "user_id": user_id,
                "content": message_content,
                "timestamp": timestamp,
                "intended_position": intended_position,
                "actual_position": intended_position
            }
                                                                     
            time.sleep(random.uniform(0.005, 0.02))
            self.message_log.append(message_entry)
            self.atomic_violations.append(False)                          

                                                        
        time.sleep(random.uniform(0.01, 0.1))

        return message_entry

    def display_messages(self):
        print("\n==== CHAT ROOM MESSAGE LOG ====")
        for idx, msg in enumerate(self.message_log):
            violation_status = "✓" if msg["intended_position"] == idx else "✗"
            print(f"[{violation_status}] User {msg['user_id']} ({msg['timestamp']}): {msg['content']}")
            if msg["intended_position"] != idx:
                print(f" ATOMIC VIOLATION! Intended position: {msg['intended_position']}, Actual: {idx}")

        total_messages = len(self.message_log)
        violations = sum(1 for v in self.atomic_violations if v)
        print(f"\n==== STATISTICS ====")
        print(f"Total messages: {total_messages}")
        print(f"Atomic violations: {violations} ({violations/total_messages*100:.1f}% of messages)")
        print(f"Correctly ordered: {total_messages - violations} ({(total_messages - violations)/total_messages*100:.1f}% of messages)")

def user_simulation(user_id, chat_room, num_messages):
    for i in range(num_messages):
        message = f"Message {i+1} from User {user_id}"
        chat_room.send_message(user_id, message)
        time.sleep(random.uniform(0.05, 0.2))

def run_simulation(num_users=5, messages_per_user=5):
    chat_room = ChatRoom()
    threads = []

    print(f"\n{'=' * 60}")
    print("SIMULATING FIXED ATOMIC VIOLATIONS IN MULTI-USER CHAT SYSTEM")
    print(f"{'=' * 60}")
    print(f"Users: {num_users}, Messages per user: {messages_per_user}")
    print("Each user runs in a separate thread, attempting to send messages concurrently.")
    print("With locking, atomic violations should be eliminated.")

    for user_id in range(1, num_users + 1):
        thread = threading.Thread(
            target=user_simulation,
            args=(user_id, chat_room, messages_per_user)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    chat_room.display_messages()

                          
run_simulation()
