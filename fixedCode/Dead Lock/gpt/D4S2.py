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

    def _ordered_acquire(self, current_node, next_node):
        """
        Acquire all required primitives in a single, consistent global order:
        sort by (node_id, kind_order). Kind order: input < output < lock.
        This prevents cyclical wait graphs across threads.
        """
        KIND_ORDER = {"input": 0, "output": 1, "lock": 2}

        resources = [
            (current_node.node_id, KIND_ORDER["input"], current_node.input_buffer, "sem"),
            (next_node.node_id,    KIND_ORDER["output"], next_node.output_buffer, "sem"),
            (current_node.node_id, KIND_ORDER["lock"],   current_node.lock,        "lock"),
            (next_node.node_id,    KIND_ORDER["lock"],   next_node.lock,           "lock"),
        ]
        resources.sort(key=lambda x: (x[0], x[1]))

        acquired = []
        try:
            for _, _, obj, typ in resources:
                if typ == "lock":
                    obj.acquire()
                else:
                    obj.acquire()  
                acquired.append((obj, typ))
            return acquired  
        except:
            
            for obj, typ in reversed(acquired):
                if typ == "lock":
                    obj.release()
                else:
                    obj.release()
            raise

    def _ordered_release(self, acquired):
        
        for obj, typ in reversed(acquired):
            if typ == "lock":
                obj.release()
            else:
                obj.release()

    def run(self):
        try:
            for i in range(len(self.route) - 1):
                current_node = self.route[i]
                next_node = self.route[i + 1]

                
                
                acquired = self._ordered_acquire(current_node, next_node)
                try:
                    
                    print(f"Packet {self.packet_id} routing from node {current_node.node_id} to {next_node.node_id}")
                    time.sleep(random.uniform(0.05, 0.15))
                finally:
                    self._ordered_release(acquired)

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
