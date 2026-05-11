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

                
                first, second = sorted([current_node, next_node], key=lambda n: n.node_id)

                
                
                acquired_in = False
                acquired_out = False

                try:
                    
                    current_node.input_buffer.acquire()
                    acquired_in = True
                    next_node.output_buffer.acquire()
                    acquired_out = True

                    
                    first.lock.acquire()
                    try:
                        
                        time.sleep(random.uniform(0.01, 0.05))
                        second.lock.acquire()
                        try:
                            
                            print(
                                f"Packet {self.packet_id} routing "
                                f"from node {current_node.node_id} to {next_node.node_id}"
                            )
                            time.sleep(random.uniform(0.05, 0.15))
                        finally:
                            second.lock.release()
                    finally:
                        first.lock.release()

                finally:
                    
                    if acquired_out:
                        next_node.output_buffer.release()
                    if acquired_in:
                        current_node.input_buffer.release()

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
