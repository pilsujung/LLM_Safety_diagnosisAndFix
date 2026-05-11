import threading
import time
import random


class NetworkNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.lock = threading.Lock()
        self.input_buffer = threading.Semaphore(1)
        self.output_buffer = threading.Semaphore(1)


def acquire_two_node_locks(node_a: NetworkNode,
                           node_b: NetworkNode,
                           simulate_delay: bool = False):
    """
    Acquire locks on two nodes in a globally consistent order (by node_id)
    to avoid circular waits and therefore deadlocks.
    """
    
    first, second = (node_a, node_b) if node_a.node_id <= node_b.node_id else (node_b, node_a)

    first.lock.acquire()
    if simulate_delay:
        
        
        time.sleep(random.uniform(0.1, 0.3))
    second.lock.acquire()

    return first, second


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

                
                high_contention = random.random() < self.deadlock_prob

                
                current_node.output_buffer.acquire()
                next_node.input_buffer.acquire()

                
                first_locked, second_locked = acquire_two_node_locks(
                    current_node,
                    next_node,
                    simulate_delay=high_contention,
                )

                try:
                    if high_contention:
                        print(
                            f"High contention: Packet {self.packet_id} locked nodes "
                            f"{current_node.node_id} and {next_node.node_id}"
                        )

                    print(
                        f"Packet {self.packet_id} routing from node "
                        f"{current_node.node_id} to {next_node.node_id}"
                    )
                    time.sleep(random.uniform(0.1, 0.3))

                finally:
                    
                    second_locked.lock.release()
                    first_locked.lock.release()

                    
                    next_node.input_buffer.release()
                    current_node.output_buffer.release()

        except Exception as e:
            print(f"Packet {self.packet_id} routing error: {e}")


def create_network_topology(num_nodes=6):
    return [NetworkNode(i) for i in range(num_nodes)]


def simulate_network_traffic(num_packets=30):
    network = create_network_topology()
    packets = []

    for i in range(num_packets):
        
        route_length = random.randint(3, len(network))
        route = random.sample(network, route_length)
        packet = Packet(i, route)
        packets.append(packet)
        packet.start()

    for packet in packets:
        packet.join()


if __name__ == "__main__":
    simulate_network_traffic()
