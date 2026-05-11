import threading
import time
import random
from queue import Queue
import datetime

class ChatRoom:
    def __init__(self):
        self.message_log = []
        self.intended_order = Queue()
        self.atomic_violations = []
        self.lock = threading.Lock()
    
    def send_message(self, user_id, message_content):
        """
        Function to send a message to the chat room.
        This function will be called concurrently by multiple threads.
        
        Now with proper synchronization to prevent atomic violations.
        """
        timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")

        with self.lock:
            intended_position = self.intended_order.qsize()
            self.intended_order.put((user_id, message_content, timestamp, intended_position))
        
        processing_time = random.uniform(0.01, 0.1)
        time.sleep(processing_time)
        
        with self.lock:
            current_position = len(self.message_log)
            
            message_entry = {
                "user_id": user_id,
                "content": message_content,
                "timestamp": timestamp,
                "intended_position": intended_position,
                "actual_position": current_position
            }
            

            time.sleep(random.uniform(0.005, 0.02))
            

            self.message_log.append(message_entry)

            is_violation = intended_position != current_position
            self.atomic_violations.append(is_violation)
        
        return message_entry
    
    def display_messages(self):
        """Display all messages in the chat room and highlight atomic violations"""
        print("\n==== CHAT ROOM MESSAGE LOG ====")

        with self.lock:
            for idx, msg in enumerate(self.message_log):
                violation_status = "✗" if msg["intended_position"] != idx else "✓"
                print(f"[{violation_status}] User {msg['user_id']} ({msg['timestamp']}): {msg['content']}")
                if msg["intended_position"] != idx:
                    print(f"   ATOMIC VIOLATION! Intended position: {msg['intended_position']}, Actual: {idx}")

            total_messages = len(self.message_log)
            violations = sum(1 for msg in self.message_log if msg["intended_position"] != msg["actual_position"])
        
        print(f"\n==== STATISTICS ====")
        print(f"Total messages: {total_messages}")
        print(f"Atomic violations: {violations} ({violations/total_messages*100:.1f}% of messages)")
        print(f"Correctly ordered: {total_messages - violations} ({(total_messages - violations)/total_messages*100:.1f}% of messages)")

        if self.intended_order.qsize() > 0:
            print(f"WARNING: {self.intended_order.qsize()} messages were not delivered!")

def user_simulation(user_id, chat_room, num_messages):
    """Simulate a user sending multiple messages to the chat room"""
    for i in range(num_messages):
        message = f"Message {i+1} from User {user_id}"
        chat_room.send_message(user_id, message)

        time.sleep(random.uniform(0.05, 0.2))

def run_simulation(num_users=5, messages_per_user=5):
    """Run the full simulation with multiple users"""
    chat_room = ChatRoom()
    threads = []
    
    print(f"\n{'=' * 60}")
    print("SIMULATING THREAD-SAFE MULTI-USER CHAT SYSTEM")
    print(f"{'=' * 60}")
    print(f"Users: {num_users}, Messages per user: {messages_per_user}")
    print("Each user runs in a separate thread, attempting to send messages concurrently.")
    print("With proper synchronization, NO atomic violations should occur.")
    
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

    with chat_room.lock:
        violated_messages = [(idx, msg) for idx, msg in enumerate(chat_room.message_log) 
                             if msg["intended_position"] != idx]
    
    if violated_messages:
        print()
    
if __name__ == "__main__":
    run_simulation()

    print("\n\nRunning high-concurrency simulation to verify thread safety...")
    run_simulation(num_users=10, messages_per_user=10)