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
 
    def acquire_locks_ordered(self, node1, node2):
        """Acquire locks in a consistent order based on node_id to prevent deadlock"""
        if node1.node_id < node2.node_id:
            first, second = node1, node2
        else:
            first, second = node2, node1
        
        first.lock.acquire()
        second.lock.acquire()
        return first, second
    
    def release_locks_ordered(self, node1, node2):
        """Release locks in reverse order of acquisition"""
        if node1.node_id < node2.node_id:
            second, first = node2, node1
        else:
            second, first = node1, node2
        
        second.lock.release()
        first.lock.release()

    def run(self): 
        try: 
            for i in range(len(self.route) - 1): 
                current_node = self.route[i] 
                next_node = self.route[i + 1] 
 
                
                if random.random() < self.deadlock_prob: 
                    
                    self.acquire_locks_ordered(current_node, next_node)
                    print(f"Packet {self.packet_id} locked nodes {current_node.node_id} and {next_node.node_id}") 
                    time.sleep(random.uniform(0.1, 0.3))
                else: 
                    
                    current_node.input_buffer.acquire() 
                    next_node.output_buffer.acquire() 
                     
                    self.acquire_locks_ordered(current_node, next_node)
 
                try: 
                    print(f"Packet {self.packet_id} routing from node {current_node.node_id} to {next_node.node_id}") 
                    time.sleep(random.uniform(0.1, 0.3)) 
                 
                finally: 
                    
                    self.release_locks_ordered(current_node, next_node)
                     
                    
                    if random.random() >= self.deadlock_prob or i > 0:
                        try:
                            current_node.input_buffer.release() 
                            next_node.output_buffer.release()
                        except:
                            pass  
 
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