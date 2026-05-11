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
    def __init__(self, packet_id, route, deadlock_prob=0.2, acquire_timeout=0.5):
        threading.Thread.__init__(self)
        self.packet_id = packet_id
        self.route = route
        self.deadlock_prob = deadlock_prob  
        self.acquire_timeout = acquire_timeout

    def run(self):
        try:
            for i in range(len(self.route) - 1):
                current_node = self.route[i]
                next_node = self.route[i + 1]

                while True:  
                    got_out = got_in = got_l1 = got_l2 = False
                    
                    try:
                        
                        got_out = current_node.output_buffer.acquire(timeout=self.acquire_timeout)
                        if not got_out:
                            raise TimeoutError("output buffer busy")
                        got_in = next_node.input_buffer.acquire(timeout=self.acquire_timeout)
                        if not got_in:
                            raise TimeoutError("input buffer busy")

                        
                        first, second = (current_node, next_node) if current_node.node_id < next_node.node_id else (next_node, current_node)
                        got_l1 = first.lock.acquire(timeout=self.acquire_timeout)
                        if not got_l1:
                            raise TimeoutError(f"node {first.node_id} lock busy")
                        got_l2 = second.lock.acquire(timeout=self.acquire_timeout)
                        if not got_l2:
                            raise TimeoutError(f"node {second.node_id} lock busy")

                        
                        print(f"Packet {self.packet_id} routing {current_node.node_id} -> {next_node.node_id}")
                        time.sleep(random.uniform(0.05, 0.15))
                        break  

                    except TimeoutError:
                        
                        time.sleep(random.uniform(0.02, 0.08))
                    finally:
                        
                        if got_l2:
                            second.lock.release()
                        if got_l1:
                            first.lock.release()
                        if got_in:
                            next_node.input_buffer.release()
                        if got_out:
                            current_node.output_buffer.release()
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
    for p in packets:
        p.join()

if __name__ == "__main__":
    simulate_network_traffic()
