import threading
import time
import random


class NetworkNode:
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.lock = threading.Lock()

        
        self.input_buffer = threading.BoundedSemaphore(1)
        self.output_buffer = threading.BoundedSemaphore(1)



_KIND_ORDER = {"in": 0, "out": 1, "lock": 2}


def _acquire_resources_ordered(resources, *, timeout=0.2, max_attempts=2000, stress_sleep=0.0):
    """
    resources: list of tuples (kind, node_id, obj)
      - kind: "in" / "out" / "lock"
      - obj: semaphore or lock (must support acquire(timeout=...))
    Returns: list of acquired resources in acquisition order, so caller can release in reverse.
    """
    resources = sorted(resources, key=lambda r: (r[1], _KIND_ORDER[r[0]]))

    for attempt in range(1, max_attempts + 1):
        acquired = []
        ok = True

        for idx, (_, __, obj) in enumerate(resources):
            if not obj.acquire(timeout=timeout):
                ok = False
                break

            acquired.append(obj)

            
            if stress_sleep and idx == 0:
                time.sleep(stress_sleep)

        if ok:
            return acquired

        
        for obj in reversed(acquired):
            obj.release()

        time.sleep(random.uniform(0.001, 0.01))

    raise TimeoutError("Could not acquire hop resources after many retries (extreme contention).")


class Packet(threading.Thread):
    def __init__(self, packet_id, route, deadlock_prob=0.2):
        super().__init__()
        self.packet_id = packet_id
        self.route = route
        self.deadlock_prob = deadlock_prob

    def run(self):
        try:
            for i in range(len(self.route) - 1):
                current_node = self.route[i]
                next_node = self.route[i + 1]

                
                stress = random.random() < self.deadlock_prob
                stress_sleep = random.uniform(0.05, 0.15) if stress else 0.0

                
                
                
                
                acquired_objs = _acquire_resources_ordered(
                    [
                        ("in", current_node.node_id, current_node.input_buffer),
                        ("out", next_node.node_id, next_node.output_buffer),
                        ("lock", current_node.node_id, current_node.lock),
                        ("lock", next_node.node_id, next_node.lock),
                    ],
                    timeout=0.2,
                    max_attempts=2000,
                    stress_sleep=stress_sleep,
                )

                try:
                    if stress:
                        print(
                            f"Stress (was deadlock-prone): Packet {self.packet_id} safely acquired "
                            f"buffers+locks for nodes {current_node.node_id}->{next_node.node_id}"
                        )

                    print(f"Packet {self.packet_id} routing from node {current_node.node_id} to {next_node.node_id}")
                    time.sleep(random.uniform(0.1, 0.3))

                finally:
                    
                    for obj in reversed(acquired_objs):
                        obj.release()

        except Exception as e:
            print(f"Packet {self.packet_id} routing error: {e}")


def create_network_topology(num_nodes=6):
    return [NetworkNode(i) for i in range(num_nodes)]


def simulate_network_traffic(num_packets=30, num_nodes=6, deadlock_prob=0.2):
    network = create_network_topology(num_nodes)
    packets = []

    for i in range(num_packets):
        
        route = random.sample(network, random.randint(3, len(network)))
        packet = Packet(i, route, deadlock_prob=deadlock_prob)
        packets.append(packet)
        packet.start()

    for packet in packets:
        packet.join()


if __name__ == "__main__":
    simulate_network_traffic(num_packets=30, num_nodes=6, deadlock_prob=0.6)
