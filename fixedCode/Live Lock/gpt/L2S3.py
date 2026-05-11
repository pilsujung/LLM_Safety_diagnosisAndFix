import threading
import time
import queue
import random
import itertools
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
    blocked_attempts: int = 0


_message_id_counter = itertools.count()

class MessageQueue:
    def __init__(self, name):
        self.name = name
        self.queue = queue.PriorityQueue()

    def enqueue(self, message: Message):
        """Add a message to the queue with priority (lower number = higher priority)"""
        if message.id == 0:

            message.timestamp = time.monotonic()
            message.id = next(_message_id_counter)


        base_priority = 10 - message.priority.value

        self.queue.put((base_priority, message.timestamp, message.id, message))

    def dequeue(self, timeout: float = 0.2):
        """Get the highest priority message from the queue (blocking briefly)"""
        try:
            _, _, _, message = self.queue.get(timeout=timeout)
            return message
        except queue.Empty:
            return None

    def size(self):
        return self.queue.qsize()

class System:
    def __init__(self, name, message_policy):
        """
        Args:
            name: System identifier
            message_policy: Function(system, message) -> bool
        """
        self.name = name
        self.inbox = MessageQueue(f"{name}_inbox")
        self.message_policy = message_policy
        self.last_processed_time = time.monotonic()
        self.is_running = True
        self.messages_sent = 0
        self.messages_processed = 0
        self.blocked_count = 0
        self.can_receive_high_priority = True


        self.starvation_seconds = 1.5
        self.max_blocked_attempts = 5

    def send_message(self, recipient_queue, priority, content):
        message = Message(self.name, recipient_queue.name.split('_')[0], priority, content)
        recipient_queue.enqueue(message)
        print(f"[{self.name}] Sent {priority.name} → {message.recipient}: {content}")
        self.messages_sent += 1

    def _update_state_after_processing(self, message: Message):
        """Maintain each system's state bit to preserve original intent."""
        if self.name == "SystemA":

            if message.priority == Priority.HIGH:
                self.can_receive_high_priority = False
        elif self.name == "SystemB":

            if message.priority == Priority.LOW:
                self.can_receive_high_priority = True

    def _should_override_policy(self, message: Message) -> bool:
        waited = time.monotonic() - message.timestamp
        return waited >= self.starvation_seconds or message.blocked_attempts >= self.max_blocked_attempts

    def process_messages(self):
        """Process messages from the inbox according to policy, with livelock escape."""
        while self.is_running:
            message = self.inbox.dequeue(timeout=0.2)
            if not message:
                time.sleep(0.05)
                continue

            allowed = self.message_policy(self, message)
            if allowed or self._should_override_policy(message):
                if not allowed:
                    print(f"[{self.name}] ⚠️ Overriding policy to break potential livelock "
                          f"({message.priority.name} from {message.sender})")
                print(f"[{self.name}] Processing {message.priority.name} from {message.sender}: {message.content}")
                self.last_processed_time = time.monotonic()
                self.messages_processed += 1
                self._update_state_after_processing(message)
                time.sleep(0.05)
            else:

                message.blocked_attempts += 1
                self.blocked_count += 1
                waited = time.monotonic() - message.timestamp
                print(f"[{self.name}] BLOCKED {message.priority.name} from {message.sender} "
                      f"(wait {waited:.2f}s, attempts {message.blocked_attempts})")
                self.inbox.enqueue(message)


                base = min(0.2, 0.02 * (2 ** min(message.blocked_attempts, 4)))
                time.sleep(base + random.uniform(0, 0.03))

def system_a_policy(system: System, message: Message) -> bool:
    """
    System A's policy:
    - Processes HIGH anytime.
    - Processes LOW only if it has recently processed a HIGH (tracked via can_receive_high_priority flag).
    """
    if message.priority == Priority.HIGH:
        return True
    if message.priority == Priority.LOW:
        return not system.can_receive_high_priority
    return True

def system_b_policy(system: System, message: Message) -> bool:
    """
    System B's policy:
    - Processes LOW anytime.
    - Processes HIGH only if it has recently processed a LOW (tracked via can_receive_high_priority flag).
    """
    if message.priority == Priority.LOW:
        return True
    if message.priority == Priority.HIGH:
        return system.can_receive_high_priority
    return True

def detect_livelock(systems, timeout=5.0):
    """
    Detect if systems are in a livelock by monitoring processing activity.
    With the new overrides, this should not continuously trigger.
    """
    while any(s.is_running for s in systems):
        all_stuck = True
        now = time.monotonic()

        for system in systems:
            time_since_last = now - system.last_processed_time
            has_pending = system.inbox.size() > 0
            if time_since_last >= timeout and has_pending and system.blocked_count > 10:
                print(f"\n[LIVELOCK DETECTION] {system.name} has pending messages but "
                      f"no progress for {time_since_last:.2f}s (blocked={system.blocked_count})")
            else:
                all_stuck = False

        if all_stuck:
            print("\n===== LIVELOCK WARNING =====")
            for s in systems:
                print(f"{s.name}: Sent {s.messages_sent}, Processed {s.messages_processed}, "
                      f"Blocked {s.blocked_count}, Inbox {s.inbox.size()}")
            print("============================\n")
        time.sleep(1.0)

def simulate_communication(system_a: System, system_b: System):
    """Original livelock-inducing pattern (kept to prove the fix)."""
    while system_a.is_running and system_b.is_running:

        system_a.send_message(system_b.inbox, Priority.HIGH, "Important request from A")
        system_b.send_message(system_a.inbox, Priority.LOW, "Regular update from B")
        time.sleep(1.0)

def main():
    system_a = System("SystemA", system_a_policy)
    system_b = System("SystemB", system_b_policy)

    threads = [
        threading.Thread(target=system_a.process_messages, daemon=True),
        threading.Thread(target=system_b.process_messages, daemon=True),
        threading.Thread(target=detect_livelock, args=([system_a, system_b],), daemon=True),
        threading.Thread(target=simulate_communication, args=(system_a, system_b), daemon=True),
    ]
    for t in threads:
        t.start()

    print("Simulation started. Press Ctrl+C to stop.")
    try:
        time.sleep(30)
        print("\nSimulation completed.")
    except KeyboardInterrupt:
        print("\nSimulation interrupted.")
    finally:
        system_a.is_running = False
        system_b.is_running = False

        time.sleep(0.5)

        print(f"\nSystemA: Sent {system_a.messages_sent}, "
              f"Processed {system_a.messages_processed}, Blocked {system_a.blocked_count}")
        print(f"SystemB: Sent {system_b.messages_sent}, "
              f"Processed {system_b.messages_processed}, Blocked {system_b.blocked_count}")

if __name__ == "__main__":
    main()
