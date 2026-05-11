import threading
import time
import queue
import random
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

    retry_count: int = 0



message_counter = 0
message_counter_lock = threading.Lock()


class MessageQueue:
    def __init__(self, name):
        self.name = name
        self.queue = queue.PriorityQueue()
        self.lock = threading.Lock()
    
    def enqueue(self, message):
        """Add a message to the queue with priority (lower number = higher priority)"""
        global message_counter
        message.timestamp = time.time()


        with message_counter_lock:
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
    def __init__(self, name, message_policy, max_retries=5):
        """
        Initialize a system that processes messages according to a policy.
        
        Args:
            name: System identifier
            message_policy: Function that determines if a message can be processed
            max_retries: How many times a message is allowed to be blocked before
                         we force processing to break a potential livelock
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
        self.max_retries = max_retries
    
    def send_message(self, recipient_queue, priority, content):
        """Send a message to another system's queue"""
        message = Message(self.name, recipient_queue.name.split('_')[0], priority, content)
        recipient_queue.enqueue(message)
        print(f"[{self.name}] Sent {priority.name} priority message to {message.recipient}: {content}")
        self.messages_sent += 1
    
    def _process(self, message: Message):
        """Actual processing logic for a message (shared by normal & forced paths)."""
        print(
            f"[{self.name}] Processing {message.priority.name} priority "
            f"message from {message.sender}: {message.content}"
        )
        self.last_processed_time = time.time()
        self.messages_processed += 1

        time.sleep(0.1)

    def process_messages(self):
        """Process messages from the inbox according to policy, with livelock avoidance."""
        while self.is_running:
            message = self.inbox.dequeue()
            if message:

                can_process = self.message_policy(self, message)




                if not can_process:
                    message.retry_count += 1
                    self.blocked_count += 1
                    wait_time = round(time.time() - message.timestamp, 2)

                    print(
                        f"[{self.name}] BLOCKED {message.priority.name} message "
                        f"from {message.sender} (waiting {wait_time}s, "
                        f"retries={message.retry_count}): {message.content}"
                    )

                    if message.retry_count > self.max_retries:

                        print(
                            f"[{self.name}] FORCED PROCESSING after "
                            f"{message.retry_count} retries: "
                            f"{message.priority.name} message from {message.sender}"
                        )
                        self._process(message)
                    else:


                        self.inbox.enqueue(message)
                        backoff = random.uniform(0.05, 0.25)
                        time.sleep(backoff)
                else:

                    self._process(message)
            else:

                time.sleep(0.1)


def system_a_policy(system, message):
    """
    System A's message processing policy:
    - Will only process LOW priority messages if it has recently processed a HIGH priority one.
    """
    if message.priority == Priority.HIGH:

        system.can_receive_high_priority = False
        return True
    elif message.priority == Priority.LOW:

        return not system.can_receive_high_priority
    return True


def system_b_policy(system, message):
    """
    System B's message processing policy:
    - Will only process HIGH priority messages if it has recently processed a LOW priority one.
    """
    if message.priority == Priority.LOW:

        system.can_receive_high_priority = True
        return True
    elif message.priority == Priority.HIGH:

        return system.can_receive_high_priority
    return True


def detect_livelock(systems, timeout=5):
    """
    Detect if systems are in a livelock by monitoring processing activity.
    With the fixed System implementation, any detected livelock should be transient,
    because messages are eventually forced to be processed.
    """
    while True:
        all_stuck = True
        
        for system in systems:
            time_since_last_processed = time.time() - system.last_processed_time
            
            if time_since_last_processed < timeout:
                all_stuck = False
                break
            

            if system.inbox.size() > 0 and system.blocked_count > 10:
                print(
                    f"\n[LIVELOCK DETECTION] {system.name} has pending messages "
                    f"but hasn't processed any in {round(time_since_last_processed, 2)}s"
                )
            else:
                all_stuck = False
        
        if all_stuck:
            print("\n===== LIVELOCK DETECTED (TRANSIENT) =====")
            print("All systems have been inactive with pending messages for over 5 seconds.")
            print("However, forced processing in System.process_messages will eventually break this state.")
            for system in systems:
                print(
                    f"{system.name}: Sent {system.messages_sent}, "
                    f"Processed {system.messages_processed}, "
                    f"Blocked {system.blocked_count}"
                )
            print("=========================================\n")

            time.sleep(5)
            
        time.sleep(1)


def simulate_communication(system_a, system_b):
    """Simulate the communication pattern that could lead to livelock."""
    while True:

        system_a.send_message(system_b.inbox, Priority.HIGH, "Important request from A")
        

        system_b.send_message(system_a.inbox, Priority.LOW, "Regular update from B")
        

        time.sleep(1)


def main():


    system_a = System("SystemA", system_a_policy, max_retries=5)
    system_b = System("SystemB", system_b_policy, max_retries=5)
    

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
        

        print(
            f"\nSystemA: Sent {system_a.messages_sent}, "
            f"Processed {system_a.messages_processed}, "
            f"Blocked {system_a.blocked_count}"
        )
        print(
            f"SystemB: Sent {system_b.messages_sent}, "
            f"Processed {system_b.messages_processed}, "
            f"Blocked {system_b.blocked_count}"
        )
        
    except KeyboardInterrupt:
        print("\nSimulation interrupted.")
    finally:

        system_a.is_running = False
        system_b.is_running = False


if __name__ == "__main__":
    main()
