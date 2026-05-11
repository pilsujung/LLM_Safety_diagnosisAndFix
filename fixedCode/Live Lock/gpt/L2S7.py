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



_message_id_gen = itertools.count()
_message_id_lock = threading.Lock()

def next_message_id() -> int:
    with _message_id_lock:
        return next(_message_id_gen)


@dataclass
class Message:
    sender: str
    recipient: str
    priority: Priority
    content: str


    created_at: float = 0.0
    last_enqueued_at: float = 0.0
    id: int = -1
    attempts: int = 0

    def age(self) -> float:
        return (time.time() - self.created_at) if self.created_at else 0.0


class MessageQueue:
    def __init__(self, name):
        self.name = name
        self.queue = queue.PriorityQueue()

    def enqueue(self, message: Message):
        now = time.time()


        if message.created_at == 0.0:
            message.created_at = now
        if message.id < 0:
            message.id = next_message_id()

        message.last_enqueued_at = now


        priority_value = 10 - message.priority.value


        self.queue.put((priority_value, message.id, message))

    def dequeue(self):
        try:
            _, _, message = self.queue.get(block=False)
            return message
        except queue.Empty:
            return None

    def size(self):
        return self.queue.qsize()


class System:
    def __init__(self, name, message_policy, *, starvation_seconds=2.0, max_attempts=25):
        """
        starvation_seconds: max time a message may be deferred before forced processing
        max_attempts: max times a message may be requeued before forced processing
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


        self.starvation_seconds = starvation_seconds
        self.max_attempts = max_attempts

    def send_message(self, recipient_queue, priority, content):
        message = Message(self.name, recipient_queue.name.split('_')[0], priority, content)
        recipient_queue.enqueue(message)
        print(f"[{self.name}] Sent {priority.name} priority message to {message.recipient}: {content}")
        self.messages_sent += 1

    def _process(self, message: Message, forced: bool = False):
        tag = " (FORCED)" if forced else ""
        print(f"[{self.name}] Processing{tag} {message.priority.name} priority message "
              f"from {message.sender}: {message.content}")
        self.last_processed_time = time.time()
        self.messages_processed += 1
        time.sleep(0.1)

    def process_messages(self):
        while self.is_running:
            message = self.inbox.dequeue()
            if not message:
                time.sleep(0.1)
                continue

            if self.message_policy(self, message):
                self._process(message, forced=False)
                continue


            message.attempts += 1
            self.blocked_count += 1


            if message.age() >= self.starvation_seconds or message.attempts >= self.max_attempts:
                wait_time = round(message.age(), 2)
                print(f"[{self.name}] STARVATION OVERRIDE after {wait_time}s / {message.attempts} attempts "
                      f"for {message.priority.name} from {message.sender}: {message.content}")
                self._process(message, forced=True)
                continue


            self.inbox.enqueue(message)
            wait_time = round(message.age(), 2)
            print(f"[{self.name}] BLOCKED {message.priority.name} message from {message.sender} "
                  f"(age {wait_time}s, attempts {message.attempts}): {message.content}")


            backoff = min(0.5, 0.05 * (2 ** min(message.attempts, 5))) + random.uniform(0.0, 0.05)
            time.sleep(backoff)


def system_a_policy(system, message):
    """
    System A's policy:
    - Will only process LOW priority messages if it has recently processed a HIGH priority one
    """
    if message.priority == Priority.HIGH:
        system.can_receive_high_priority = False
        return True
    elif message.priority == Priority.LOW:
        return not system.can_receive_high_priority
    return True


def system_b_policy(system, message):
    """
    System B's policy:
    - Will only process HIGH priority messages if it has recently processed a LOW priority one
    """
    if message.priority == Priority.LOW:
        system.can_receive_high_priority = True
        return True
    elif message.priority == Priority.HIGH:
        return system.can_receive_high_priority
    return True


def detect_livelock(systems, timeout=5):
    while True:
        all_stuck = True

        for system in systems:
            time_since_last_processed = time.time() - system.last_processed_time


            if time_since_last_processed < timeout:
                all_stuck = False
                break


            if system.inbox.size() > 0 and system.blocked_count > 10:
                print(f"\n[LIVELOCK DETECTION] {system.name} has pending messages but hasn't processed any in "
                      f"{round(time_since_last_processed, 2)}s")
            else:
                all_stuck = False

        if all_stuck:
            print("\n===== LIVELOCK DETECTED =====")
            print("All systems have been inactive with pending messages for over 5 seconds.")
            for system in systems:
                print(f"{system.name}: Sent {system.messages_sent}, Processed {system.messages_processed}, "
                      f"Blocked {system.blocked_count}")
            print("===============================\n")
            time.sleep(5)

        time.sleep(1)


def simulate_communication(system_a, system_b):
    while True:
        system_a.send_message(system_b.inbox, Priority.HIGH, "Important request from A")
        system_b.send_message(system_a.inbox, Priority.LOW, "Regular update from B")
        time.sleep(1)


def main():
    system_a = System("SystemA", system_a_policy, starvation_seconds=2.0, max_attempts=25)
    system_b = System("SystemB", system_b_policy, starvation_seconds=2.0, max_attempts=25)

    process_a = threading.Thread(target=system_a.process_messages, daemon=True)
    process_b = threading.Thread(target=system_b.process_messages, daemon=True)
    detection_thread = threading.Thread(target=detect_livelock, args=([system_a, system_b],), daemon=True)
    comm_thread = threading.Thread(target=simulate_communication, args=(system_a, system_b), daemon=True)

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
