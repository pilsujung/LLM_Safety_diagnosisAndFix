import threading
import time

class ResourceAllocator:
    def __init__(self):

        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

        self.access_count = {
            'priority_thread': 0,
            'starved_thread': 0
        }
        self.resource_usage_log = []


        self.turn = 'priority'

    def priority_thread(self):
        """Behavior of the high-priority thread."""
        while self.access_count['priority_thread'] < 500:
            with self.condition:

                while self.turn != 'priority':
                    self.condition.wait()


                self.access_count['priority_thread'] += 1
                self.resource_usage_log.append(
                    f"Priority thread accessed at count {self.access_count['priority_thread']}"
                )
                print(f"Priority thread accessed resource {self.access_count['priority_thread']} times")


                self.turn = 'starved'
                self.condition.notify_all()


            time.sleep(0.01)

    def starved_thread(self):
        """Behavior of the thread that previously had difficulty accessing the resource."""
        while self.access_count['starved_thread'] < 500:
            with self.condition:

                while self.turn != 'starved':
                    self.condition.wait()


                self.access_count['starved_thread'] += 1
                self.resource_usage_log.append(
                    f"Starved thread accessed at count {self.access_count['starved_thread']}"
                )
                print(f"Starved thread accessed resource {self.access_count['starved_thread']} times")


                self.turn = 'priority'
                self.condition.notify_all()


            time.sleep(0.1)

    def simulate_starvation(self):
        """Simulate resource access between threads."""
        priority = threading.Thread(target=self.priority_thread)
        starved = threading.Thread(target=self.starved_thread)

        priority.start()
        starved.start()

        priority.join()
        starved.join()

        print("Final access counts:", self.access_count)
        self.log_resource_usage()

    def log_resource_usage(self):
        """Print resource usage log."""
        print("\nResource Usage Log:")
        for log_entry in self.resource_usage_log:
            print(log_entry)

if __name__ == "__main__":
    allocator = ResourceAllocator()
    allocator.simulate_starvation()
