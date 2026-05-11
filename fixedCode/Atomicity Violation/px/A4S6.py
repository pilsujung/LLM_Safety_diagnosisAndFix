import threading
import time
import random
from queue import Queue
import datetime

class ChatRoom:
    def __init__(self):
        self.message_log = []
        self.intended_order = Queue()
        self._lock = threading.Lock()

    def send_message(self, user_id, message_content):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")
        
                                                                        
        with self._lock:
            intended_position = self.intended_order.qsize()
            self.intended_order.put((user_id, message_content, timestamp, intended_position))
            
            message_entry = {
                "user_id": user_id,
                "content": message_content,
                "timestamp": timestamp,
                "intended_position": intended_position,
                "actual_position": len(self.message_log)                        
            }
            self.message_log.append(message_entry)
        
                                                  
        time.sleep(random.uniform(0.01, 0.1))
        return message_entry

    def display_messages(self):
        print("\n==== FIXED CHAT ROOM MESSAGE LOG ====")
        violations = 0
        for idx, msg in enumerate(self.message_log):
            violation_status = "✗" if msg["intended_position"] != idx else "✓"
            print(f"[{violation_status}] User {msg['user_id']} ({msg['timestamp']}): {msg['content']}")
            if msg["intended_position"] != idx:
                print(f"  VIOLATION! Expected: {msg['intended_position']}, Got: {idx}")
                violations += 1
        
        total = len(self.message_log)
        print(f"\n==== STATISTICS ====")
        print(f"Total messages: {total}")
        print(f"Atomic violations: {violations} ({violations/total*100:.1f}%)")
        print(f"Perfect order achieved: {'✓' if violations == 0 else '✗'}")

    def user_simulation(self, user_id, num_messages):
        for i in range(num_messages):
            self.send_message(user_id, f"Message {i+1} from User {user_id}")
            time.sleep(random.uniform(0.05, 0.2))

def run_simulation(num_users=10, messages_per_user=10):
    chat_room = ChatRoom()
    threads = []
    
    print(f"SIMULATION: {num_users} users, {messages_per_user} msgs each")
    for user_id in range(1, num_users + 1):
        t = threading.Thread(target=chat_room.user_simulation, args=(user_id, messages_per_user))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    chat_room.display_messages()

if __name__ == "__main__":
    run_simulation()
