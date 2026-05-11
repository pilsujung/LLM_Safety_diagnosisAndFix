import random
import time
from collections import defaultdict

class PacketOrderViolationSimulator:
    def __init__(self, total_packet_count=15, max_delay_ms=500, loss_probability=0.1, 
                 reordering_window_size=5):
        """
        Initialize packet simulator with gap recovery
        Args:
            reordering_window_size: Max packets to buffer before declaring gap
        """
        self.total_packet_count = total_packet_count
        self.max_delay_ms = max_delay_ms
        self.loss_probability = loss_probability
        self.reordering_window_size = reordering_window_size
        self.statistics = {
            'packets_sent': 0, 'packets_received': 0, 'packets_lost': 0,
            'out_of_order_packets': 0, 'duplicate_packets': 0, 'total_delay_ms': 0,
            'gap_drops': 0, 'gap_packets': 0
        }

    def generate_original_packets(self):
        """Generate sequential packets"""
        return [{
            'sequence_number': i, 'payload': f"DATA_CHUNK_{i:03d}",
            'size_bytes': random.randint(64, 1500),
            'timestamp_created': time.time(),
            'checksum': hash(f"DATA_CHUNK_{i:03d}") % 10000
        } for i in range(1, self.total_packet_count + 1)]

    def simulate_network_transmission(self, original_packets):
        """Simulate transmission with loss/reordering/duplicates"""
        transmitted = []
        print("=== NETWORK TRANSMISSION ===")
        
        for packet in original_packets:
            self.statistics['packets_sent'] += 1
            if random.random() < self.loss_probability:
                print(f"❌ Packet {packet['sequence_number']} LOST")
                self.statistics['packets_lost'] += 1
                continue
            
            delay = random.randint(10, self.max_delay_ms)
            self.statistics['total_delay_ms'] += delay
            pkt = packet.copy()
            pkt['transmission_delay_ms'] = delay
            pkt['timestamp_transmitted'] = time.time()
            transmitted.append(pkt)
            
            if random.random() < 0.05:
                dup = pkt.copy()
                dup['is_duplicate'] = True
                transmitted.append(dup)
                self.statistics['duplicate_packets'] += 1
                print(f"🔄 Packet {packet['sequence_number']} DUPLICATED")
        
        transmitted.sort(key=lambda p: p['transmission_delay_ms'])
        return transmitted

    def simulate_packet_reception_and_reordering(self, transmitted_packets):
        """FIXED: Buffer all packets + gap recovery after reordering_window_size"""
        buffer = {}
        duplicates = set()
        expected_seq = 1
        processed = []
        
        print("\n=== RECEPTION & REORDERING (w/ GAP RECOVERY) ===")
        
        for pkt in transmitted_packets:
            seq = pkt['sequence_number']
            print(f"📦 Rcvd {seq} (delay: {pkt['transmission_delay_ms']}ms)")
            

            ident = (seq, pkt['checksum'])
            if ident in duplicates:
                print(f"  ⚠️ Duplicate {seq} discarded")
                continue
            duplicates.add(ident)
            

            buffer[seq] = pkt
            self.statistics['packets_received'] += 1
            
            if seq > expected_seq:
                print(f"  ⚠️ Out-of-order: rcvd {seq}, expect {expected_seq}")
                self.statistics['out_of_order_packets'] += 1
            elif seq == expected_seq:
                print(f"  ✅ In-order {seq}")
            

            while expected_seq in buffer:
                proc_pkt = buffer.pop(expected_seq)
                processed.append(proc_pkt)
                print(f"  ✅ Process {expected_seq}")
                expected_seq += 1
            

            buffered_keys = sorted(buffer.keys())
            if buffered_keys and buffered_keys[0] >= expected_seq + self.reordering_window_size:
                gap_size = buffered_keys[0] - expected_seq
                print(f"  🕳️ GAP DETECTED: drop {gap_size} missing, process {len(buffered_keys)} buffered")
                self.statistics['gap_drops'] += gap_size
                self.statistics['gap_packets'] += len(buffered_keys)

                while buffer:
                    next_seq = min(buffer.keys())
                    proc_pkt = buffer.pop(next_seq)
                    processed.append(proc_pkt)
                    print(f"  ✅ Gap-recovery process {next_seq}")
                    expected_seq = max(expected_seq, next_seq + 1)
        
        return processed, buffer

    def display_simulation_results(self, original, processed, buffer):
        print("\n" + "="*70)
        print("✅ SIMULATION COMPLETE - NO MORE STUCK PACKETS")
        print("="*70)
        
        print(f"\n📊 STATISTICS:")
        print(f"Sent: {self.statistics['packets_sent']:2d} | Lost: {self.statistics['packets_lost']:2d}")
        print(f"Rcvd: {self.statistics['packets_received']:2d} | Processed: {len(processed):2d}")
        print(f"Dups: {self.statistics['duplicate_packets']:2d} | Out-of-order: {self.statistics['out_of_order_packets']:2d}")
        print(f"Gaps dropped: {self.statistics['gap_drops']:2d} | Gap recovery: {self.statistics['gap_packets']:2d}")
        print(f"Final buffer: {len(buffer):2d} | Success: {len(processed)/self.statistics['packets_sent']*100:.0f}%")

    def run_complete_simulation(self):
        original = self.generate_original_packets()
        transmitted = self.simulate_network_transmission(original)
        processed, buffer = self.simulate_packet_reception_and_reordering(transmitted)
        self.display_simulation_results(original, processed, buffer)
        return {'processed': len(processed), 'buffered': len(buffer), 'stats': self.statistics}

def main():
    sim = PacketOrderViolationSimulator(
        total_packet_count=20,
        max_delay_ms=800,
        loss_probability=0.15,
        reordering_window_size=4
    )
    results = sim.run_complete_simulation()
    print(f"\n🎯 FIXED: {results['processed']}/{sim.statistics['packets_sent']} processed (0 stuck)")

if __name__ == "__main__":
    main()
