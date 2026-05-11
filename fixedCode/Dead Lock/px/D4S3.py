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

                
                
                node1, node2 = sorted([current_node, next_node], key=lambda n: n.node_id)
                node1.lock.acquire()
                node2.lock.acquire()

                try:
                    
                    current_node.input_buffer.acquire()
                    next_node.output_buffer.acquire()

                    print(f"Packet {self.packet_id} routing from node {current_node.node_id} to {next_node.node_id}")
                    time.sleep(random.uniform(0.01, 0.05))  

                    current_node.input_buffer.release()
                    next_node.output_buffer.release()

                finally:
                    
                    node2.lock.release()
                    node1.lock.release()

        except Exception as e:
            print(f"Packet {self.packet_id} routing error: {e}")

def create_network_topology(num_nodes=6):
    return [NetworkNode(i) for i in range(num_nodes)]

def simulate_network_traffic(num_packets=20):
    network = create_network_topology()
    packets = []

    for i in range(num_packets):
        
        route = random.sample(network, random.randint(3, len(network)))
        packet = Packet(i, route)
        packets.append(packet)
        packet.start()
        time.sleep(0.001)  

    print("All packets started. Waiting for completion...")
    completed = 0
    for packet in packets:
        packet.join(timeout=10)
        if not packet.is_alive():
            completed += 1

    print(f"Simulation completed: {completed}/{len(packets)} packets finished successfully")

if __name__ == "__main__":
    simulate_network_traffic()
