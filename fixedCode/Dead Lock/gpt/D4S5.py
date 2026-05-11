import threading
import time
import random

class NetworkNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.lock = threading.Lock()
        self.input_buffer = threading.Semaphore(1)
        self.output_buffer = threading.Semaphore(1)

class Packet(threading.Thread):
    def __init__(self, packet_id, route, deadlock_prob=0.2):
        threading.Thread.__init__(self)
        self.packet_id = packet_id
        self.route = route
        self.deadlock_prob = deadlock_prob

    def run(self):
        try:
            for i in range(len(self.route) - 1):
                current_node = self.route[i]
                next_node = self.route[i + 1]

                
                current_node.input_buffer.acquire()
                next_node.output_buffer.acquire()

                
                if current_node.node_id < next_node.node_id:
                    first_lock_node = current_node
                    second_lock_node = next_node
                else:
                    first_lock_node = next_node
                    second_lock_node = current_node

                
                high_contention = random.random() < self.deadlock_prob

                try:
                    
                    first_lock_node.lock.acquire()
                    if high_contention:
                        
                        time.sleep(random.uniform(0.1, 0.3))
                    second_lock_node.lock.acquire()

                    try:
                        
                        print(
                            f"Packet {self.packet_id} routing from node "
                            f"{current_node.node_id} to {next_node.node_id}"
                        )
                        time.sleep(random.uniform(0.1, 0.3))
                    finally:
                        
                        second_lock_node.lock.release()
                        first_lock_node.lock.release()
                finally:
                    
                    current_node.input_buffer.release()
                    next_node.output_buffer.release()

        except Exception as e:
            print(f"Packet {self.packet_id} routing error: {e}")

def create_network_topology(num_nodes=6):
    return [NetworkNode(i) for i in range(num_nodes)]

def simulate_network_traffic(num_packets=30):
    network = create_network_topology()
    packets = []

    for i in range(num_packets):
        
        route = random.sample(network, random.randint(3, len(network)))
        packet = Packet(i, route)
        packets.append(packet)
        packet.start()

    for packet in packets:
        packet.join()

if __name__ == "__main__":
    simulate_network_traffic()
