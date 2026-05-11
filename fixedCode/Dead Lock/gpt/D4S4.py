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
        threading.Thread.__init__(self, name=f"Packet-{packet_id}")
        self.packet_id = packet_id
        self.route = route
        self.deadlock_prob = deadlock_prob

    def _acquire_node_pair(self, a: NetworkNode, b: NetworkNode):
        """
        Acquire two node locks using a global ordering (by node_id) to prevent deadlock.
        Returns (first, second) in the order they were acquired.
        """
        first, second = (a, b) if a.node_id <= b.node_id else (b, a)
        first.lock.acquire()
        try:
            second.lock.acquire()
        except Exception:
            first.lock.release()
            raise
        return first, second

    def run(self):
        try:
            for i in range(len(self.route) - 1):
                current_node = self.route[i]
                next_node = self.route[i + 1]

                
                if random.random() < self.deadlock_prob:
                    time.sleep(random.uniform(0.05, 0.15))

                got_in = got_out = False
                first = second = None
                try:
                    
                    current_node.input_buffer.acquire()
                    got_in = True
                    next_node.output_buffer.acquire()
                    got_out = True

                    first, second = self._acquire_node_pair(current_node, next_node)

                    print(f"Packet {self.packet_id} routing {current_node.node_id} -> {next_node.node_id}")
                    time.sleep(random.uniform(0.1, 0.3))

                finally:
                    
                    if second is not None:
                        second.lock.release()
                    if first is not None:
                        first.lock.release()

                    
                    if got_out:
                        next_node.output_buffer.release()
                    if got_in:
                        current_node.input_buffer.release()

        except Exception as e:
            print(f"Packet {self.packet_id} routing error: {e}")

def create_network_topology(num_nodes=6):
    return [NetworkNode(i) for i in range(num_nodes)]

def simulate_network_traffic(num_packets=30):
    network = create_network_topology()
    packets = []

    for i in range(num_packets):
        
        route_len = random.randint(3, len(network))
        route = random.sample(network, route_len)
        packet = Packet(i, route)
        packets.append(packet)
        packet.start()

    for packet in packets:
        packet.join()

if __name__ == "__main__":
    simulate_network_traffic()
