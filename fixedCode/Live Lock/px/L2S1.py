import threading
import time
import queue
from enum import Enum
from dataclasses import dataclass

class Priority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3

@dataclass
class Message:
    sender: str
    recipient: str
    priority: Priority
    content: str
    timestamp: float = 0.0


    id: int = 0


message_counter = 0

class MessageQueue:
    def __init__(self, name):
        self.name = name
        self.queue = queue.PriorityQueue()
        self.lock = threading.Lock()

    def enqueue(self, message):
        """Add a message to the queue with priority (lower number = higher priority)"""
        global message_counter
        message.timestamp = time.time()
        message.id = message_counter
        message_counter += 1


        priority_value = 10 - message.priority.value
        with self.lock:

            self.queue.put((priority_value, message.id, message))

    def dequeue(self):
        """Get the highest priority message from the queue"""
        try:
            with self.lock:
                if not self.queue.empty():
                    _, _, message = self.queue.get(block=False)
                    return message
            return None
        except queue.Empty:
            return None

    def size(self):
        return self.queue.qsize()

class System:
    def __init__(self, name, message_policy):
        """
        Initialize a system that processes messages according to a policy.

        Args:
            name: System identifier
            message_policy: Function that determines if a message can be processed
        """
        self.name = name
        self.inbox = MessageQueue(f"{name}_inbox")
        self.message_policy = message_policy
        self.last_processed_time = time.time()
        self.is_running = True
        self.messages_sent = 0
        self.messages_processed = 0
        self.blocked_count = 0
        self.can_receive_high_priority = True

    def send_message(self, recipient_queue, priority, content):
        """Send a message to another system's queue"""
        message = Message(self.name, recipient_queue.name.split('_')[0], priority, content)
        recipient_queue.enqueue(message)
        print(f"[{self.name}] Sent {priority.name} priority message to {message.recipient}: {content}")
        self.messages_sent += 1

    def process_messages(self):
        """Process messages from the inbox according to policy"""
        while self.is_running:
            message = self.inbox.dequeue()
            if message:

                if self.message_policy(self, message):
                    print(f"[{self.name}] Processing {message.priority.name} priority message from {message.sender}: {message.content}")
                    self.last_processed_time = time.time()
                    self.messages_processed += 1

                    time.sleep(0.1)
                else:

                    self.inbox.enqueue(message)
                    wait_time = round(time.time() - message.timestamp, 2)
                    self.blocked_count += 1
                    print(f"[{self.name}] BLOCKED {message.priority.name} message from {message.sender} (waiting {wait_time}s): {message.content}")

                    time.sleep(0.1)
            else:

                time.sleep(0.1)

def fixed_system_a_policy(system, message):
    if message.priority == Priority.HIGH:
        system.can_receive_high_priority = False
        return True
    elif message.priority == Priority.LOW:

        return True
    return True

def fixed_system_b_policy(system, message):
    if message.priority == Priority.LOW:
        system.can_receive_high_priority = True
        return True
    elif message.priority == Priority.HIGH:

        return True
    return True

def detect_livelock(systems, timeout=5):
    """
    Detect if systems are in a livelock by monitoring processing activity
    """
    while True:
        all_stuck = True

        for system in systems:
            time_since_last_processed = time.time() - system.last_processed_time

            if time_since_last_processed < timeout:
                all_stuck = False
                break


            if system.inbox.size() > 0 and system.blocked_count > 10:
                print(f"\n[LIVELOCK DETECTION] {system.name} has pending messages but hasn't processed any in {round(time_since_last_processed, 2)}s")
            else:
                all_stuck = False

        if all_stuck:
            print("\n===== LIVELOCK DETECTED =====")
            print("All systems have been inactive with pending messages for over 5 seconds.")
            print("Systems are actively working (trying to process messages) but not making progress.")
            for system in systems:
                print(f"{system.name}: Sent {system.messages_sent}, Processed {system.messages_processed}, Blocked {system.blocked_count}")
            print("===============================\n")

            time.sleep(5)

        time.sleep(1)

def simulate_communication(system_a, system_b):
    """Simulate the communication pattern that leads to livelock"""
    while True:

        system_a.send_message(system_b.inbox, Priority.HIGH, "Important request from A")


        system_b.send_message(system_a.inbox, Priority.LOW, "Regular update from B")


        time.sleep(1)

def main():

    system_a = System("SystemA", fixed_system_a_policy)
    system_b = System("SystemB", fixed_system_b_policy)


    process_a = threading.Thread(target=system_a.process_messages)
    process_b = threading.Thread(target=system_b.process_messages)


    detection_thread = threading.Thread(target=detect_livelock, args=([system_a, system_b],))


    comm_thread = threading.Thread(target=simulate_communication, args=(system_a, system_b))


    process_a.daemon = True
    process_b.daemon = True
    detection_thread.daemon = True
    comm_thread.daemon = True

    process_a.start()
    process_b.start()
    detection_thread.start()
    comm_thread.start()

    print("Simulation started. Press Ctrl+C to stop.")

    try:

        time.sleep(30)
        print("\nSimulation completed.")


        print(f"\nSystemA: Sent {system_a.messages_sent}, Processed {system_a.messages_processed}, Blocked {system_a.blocked_count}")
        print(f"SystemB: Sent {system_b.messages_sent}, Processed {system_b.messages_processed}, Blocked {system_b.blocked_count}")

    except KeyboardInterrupt:
        print("\nSimulation interrupted.")
    finally:

        system_a.is_running = False
        system_b.is_running = False

if __name__ == "__main__":
    main()
