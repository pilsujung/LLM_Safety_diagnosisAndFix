import random
import time
import threading
from collections import defaultdict
from queue import PriorityQueue

class PacketOrderViolationFixed:
    def __init__(self, total_packet_count=15, max_delay_ms=500, loss_probability=0.1):
        """
        Initialize the packet order violation simulator with proper ordering mechanisms
        
        Args:
            total_packet_count: Total number of packets to simulate
            max_delay_ms: Maximum delay in milliseconds for packet transmission
            loss_probability: Probability of packet loss (0.0 to 1.0)
        """
        self.total_packet_count = total_packet_count
        self.max_delay_ms = max_delay_ms
        self.loss_probability = loss_probability
        

        self.reception_buffer = {}
        self.buffer_lock = threading.Lock()
        self.packet_received_event = threading.Event()
        

        self.expected_sequence_number = 1
        self.sequence_lock = threading.Lock()
        

        self.max_wait_time_seconds = 5.0
        self.buffer_timeout = {}
        
        self.statistics = {
            'packets_sent': 0,
            'packets_received': 0,
            'packets_lost': 0,
            'out_of_order_packets': 0,
            'duplicate_packets': 0,
            'total_delay_ms': 0,
            'packets_recovered': 0
        }
        
        self.duplicate_detection_set = set()
        self.successfully_processed_packets = []
    
    def generate_original_packets(self):
        """Generate the original sequence of packets to be transmitted"""
        original_packet_sequence = []
        
        for sequence_number in range(1, self.total_packet_count + 1):
            packet_payload = f"DATA_CHUNK_{sequence_number:03d}"
            packet_size_bytes = random.randint(64, 1500)
            timestamp_created = time.time()
            
            packet_info = {
                'sequence_number': sequence_number,
                'payload': packet_payload,
                'size_bytes': packet_size_bytes,
                'timestamp_created': timestamp_created,
                'checksum': hash(packet_payload) % 10000
            }
            original_packet_sequence.append(packet_info)
        
        return original_packet_sequence
    
    def simulate_network_transmission(self, original_packets):
        """
        Simulate network transmission with threading to represent concurrent packet arrival
        """
        transmitted_packet_list = []
        
        print("=== NETWORK TRANSMISSION SIMULATION ===")
        
        for packet in original_packets:
            self.statistics['packets_sent'] += 1
            

            if random.random() < self.loss_probability:
                print(f"❌ Packet {packet['sequence_number']} LOST during transmission")
                self.statistics['packets_lost'] += 1
                continue
            

            transmission_delay_ms = random.randint(10, self.max_delay_ms)
            self.statistics['total_delay_ms'] += transmission_delay_ms
            

            transmitted_packet = packet.copy()
            transmitted_packet['transmission_delay_ms'] = transmission_delay_ms
            transmitted_packet['timestamp_transmitted'] = time.time()
            
            transmitted_packet_list.append(transmitted_packet)
            

            if random.random() < 0.05:
                duplicate_packet = transmitted_packet.copy()
                duplicate_packet['is_duplicate'] = True
                transmitted_packet_list.append(duplicate_packet)
                self.statistics['duplicate_packets'] += 1
                print(f"🔄 Packet {packet['sequence_number']} DUPLICATED during transmission")
        
        return transmitted_packet_list
    
    def receive_packet_thread(self, packet, delay_ms):
        """
        Thread function to simulate packet arrival with network delay
        """

        time.sleep(delay_ms / 1000.0)
        
        sequence_num = packet['sequence_number']
        
        print(f"\n📦 Received packet {sequence_num} (delay: {delay_ms}ms)")
        

        with self.buffer_lock:
            packet_identifier = (sequence_num, packet['checksum'])
            if packet_identifier in self.duplicate_detection_set:
                print(f"   ⚠️  DUPLICATE packet {sequence_num} detected and discarded")
                return
            self.duplicate_detection_set.add(packet_identifier)
            

            if sequence_num < self.expected_sequence_number:
                print(f"   ❌ Late arrival: packet {sequence_num} already processed")
                return
            elif sequence_num > self.expected_sequence_number:
                print(f"   ⚠️  OUT-OF-ORDER: received {sequence_num}, expected {self.expected_sequence_number}")
                self.statistics['out_of_order_packets'] += 1
            else:
                print(f"   ✅ IN-ORDER packet received")
            

            self.reception_buffer[sequence_num] = packet
            self.buffer_timeout[sequence_num] = time.time()
            self.statistics['packets_received'] += 1
            

            self.packet_received_event.set()
    
    def process_packets_in_order(self):
        """
        Background thread that processes packets in the correct order
        Waits for missing packets with timeout mechanism
        """
        print("\n=== PACKET PROCESSING THREAD STARTED ===")
        
        while True:
            with self.sequence_lock:

                if (self.expected_sequence_number > self.total_packet_count and 
                    len(self.reception_buffer) == 0):
                    print("\n✅ All packets processed or accounted for")
                    break
            

            self.packet_received_event.wait(timeout=1.0)
            self.packet_received_event.clear()
            
            with self.buffer_lock:
                processed_count = 0
                

                while self.expected_sequence_number in self.reception_buffer:
                    packet_to_process = self.reception_buffer.pop(self.expected_sequence_number)
                    self.buffer_timeout.pop(self.expected_sequence_number, None)
                    self.successfully_processed_packets.append(packet_to_process)
                    print(f"   ✅ Processing packet {self.expected_sequence_number} in order")
                    
                    with self.sequence_lock:
                        self.expected_sequence_number += 1
                    processed_count += 1
                
                if processed_count > 1:
                    print(f"   📤 Batch processed {processed_count} consecutive packets")
                

                current_time = time.time()
                timed_out_packets = []
                
                for seq_num in list(self.buffer_timeout.keys()):
                    if current_time - self.buffer_timeout[seq_num] > self.max_wait_time_seconds:
                        timed_out_packets.append(seq_num)
                

                if timed_out_packets:
                    print(f"\n⏱️  TIMEOUT: Skipping missing packet(s) before {min(timed_out_packets)}")
                    with self.sequence_lock:

                        self.expected_sequence_number = min(timed_out_packets)
                        self.statistics['packets_recovered'] += 1
    
    def simulate_concurrent_reception(self, transmitted_packets):
        """
        Simulate concurrent packet reception using threads
        """
        print("\n=== CONCURRENT PACKET RECEPTION SIMULATION ===")
        

        processing_thread = threading.Thread(target=self.process_packets_in_order)
        processing_thread.daemon = True
        processing_thread.start()
        

        reception_threads = []
        for packet in transmitted_packets:
            thread = threading.Thread(
                target=self.receive_packet_thread,
                args=(packet, packet['transmission_delay_ms'])
            )
            thread.daemon = True
            reception_threads.append(thread)
            thread.start()
        

        for thread in reception_threads:
            thread.join()
        

        processing_thread.join(timeout=self.max_wait_time_seconds + 2)
        
        return self.successfully_processed_packets, self.reception_buffer
    
    def display_simulation_results(self, original_packets, processed_packets, remaining_buffer):
        """Display comprehensive simulation results and statistics"""
        
        print("\n" + "="*60)
        print("SIMULATION RESULTS SUMMARY")
        print("="*60)
        

        print("\n📋 ORIGINAL PACKET SEQUENCE:")
        for i, packet in enumerate(original_packets[:8]):
            print(f"   {packet['sequence_number']:2d}. {packet['payload']} "
                  f"({packet['size_bytes']} bytes)")
        if len(original_packets) > 8:
            print(f"   ... and {len(original_packets) - 8} more packets")
        

        print(f"\n✅ SUCCESSFULLY PROCESSED PACKETS ({len(processed_packets)}):")
        for packet in processed_packets[:8]:
            print(f"   {packet['sequence_number']:2d}. {packet['payload']} "
                  f"(delay: {packet['transmission_delay_ms']}ms)")
        if len(processed_packets) > 8:
            print(f"   ... and {len(processed_packets) - 8} more packets")
        

        if remaining_buffer:
            print(f"\n⚠️  UNPROCESSED PACKETS ({len(remaining_buffer)}):")
            for seq_num in sorted(remaining_buffer.keys()):
                packet = remaining_buffer[seq_num]
                print(f"   {seq_num:2d}. {packet['payload']}")
        

        print(f"\n📊 TRANSMISSION STATISTICS:")
        print(f"   Total packets generated:     {self.statistics['packets_sent']}")
        print(f"   Packets successfully sent:   {self.statistics['packets_sent'] - self.statistics['packets_lost']}")
        print(f"   Packets lost in transit:     {self.statistics['packets_lost']}")
        print(f"   Packets received:            {self.statistics['packets_received']}")
        print(f"   Duplicate packets detected:  {self.statistics['duplicate_packets']}")
        print(f"   Out-of-order packets:        {self.statistics['out_of_order_packets']}")
        print(f"   Successfully processed:      {len(processed_packets)}")
        print(f"   Packets recovered (timeout): {self.statistics['packets_recovered']}")
        print(f"   Packets stuck in buffer:     {len(remaining_buffer)}")
        

        if self.statistics['packets_received'] > 0:
            avg_delay = self.statistics['total_delay_ms'] / self.statistics['packets_received']
            success_rate = len(processed_packets) / self.statistics['packets_sent'] * 100
            print(f"   Average transmission delay:  {avg_delay:.1f}ms")
            print(f"   End-to-end success rate:     {success_rate:.1f}%")
    
    def run_complete_simulation(self):
        """Execute the complete packet order violation simulation with fixes"""
        print("🚀 STARTING FIXED PACKET ORDER SIMULATION")
        print(f"Configuration: {self.total_packet_count} packets, "
              f"max delay {self.max_delay_ms}ms, {self.loss_probability*100:.1f}% loss probability")
        print(f"Fix applied: Thread-safe ordering with timeout mechanism\n")
        

        original_packet_sequence = self.generate_original_packets()
        

        transmitted_packets = self.simulate_network_transmission(original_packet_sequence)
        

        processed_packets, remaining_buffer = self.simulate_concurrent_reception(transmitted_packets)
        

        self.display_simulation_results(original_packet_sequence, processed_packets, remaining_buffer)
        
        return {
            'original_packets': original_packet_sequence,
            'processed_packets': processed_packets,
            'remaining_buffer': remaining_buffer,
            'statistics': self.statistics
        }


def main():
    """Main function to run the fixed packet order simulation"""
    simulator = PacketOrderViolationFixed(
        total_packet_count=20,
        max_delay_ms=800,
        loss_probability=0.15
    )
    
    simulation_results = simulator.run_complete_simulation()

if __name__ == "__main__":
    main()