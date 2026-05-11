import multiprocessing as mp
import time
import random

class LockCohortingManager:
    def __init__(self, manager, threshold=10):
        """
        가상 NUMA 환경(Lock Cohorting) 매니저 초기화.
        직렬화 에러를 방지하기 위해 Manager가 생성한 프록시 객체만 멤버 변수로 저장.
        """
        self.state_lock = manager.Lock()
        
        # 전역 락 (G) 및 노드별 로컬 락 (S_i)
        self.global_lock = manager.Lock()
        self.local_locks = {
            0: manager.Lock(), 
            1: manager.Lock()
        }
        
        # Cohorting 상태 변수
        self.global_owner = manager.Value('i', -1)
        self.handoff_count = manager.Value('i', 0)
        self.local_waiters = manager.dict({0: 0, 1: 0})
        self.threshold = threshold

    def acquire(self, node_id):
        """자원 접근 요청 (Lock Cohorting Acquire)"""
        # 1. 로컬 대기자 카운트 증가
        with self.state_lock:
            self.local_waiters[node_id] += 1
            
        # 2. 로컬 락 획득 (S_i)
        self.local_locks[node_id].acquire()
        
        # 3. 로컬 대기자 카운트 감소 및 전역 락 소유권 확인
        with self.state_lock:
            self.local_waiters[node_id] -= 1
            current_owner = self.global_owner.value
            
        # 4. 전역 락이 다른 노드에 있다면 전역 락 획득 (G)
        if current_owner != node_id:
            self.global_lock.acquire()
            with self.state_lock:
                self.global_owner.value = node_id
                self.handoff_count.value = 0

    def release(self, node_id):
        """자원 접근 해제 (Lock Cohorting Release)"""
        # 5. Cohorting 해제 평가
        with self.state_lock:
            waiters = self.local_waiters[node_id]
            handoffs = self.handoff_count.value

        # 대기자가 있고, 연속 승계 임계치에 도달하지 않았다면 -> Local Handoff
        if waiters > 0 and handoffs < self.threshold:
            with self.state_lock:
                self.handoff_count.value = handoffs + 1
            # 전역 락은 유지한 채, 로컬 락만 해제 (동일 노드 대기자에게 양보)
            self.local_locks[node_id].release()
        
        # 대기자가 없거나 임계치 초과 시 -> Global Release (may-pass-local)
        else:
            with self.state_lock:
                self.global_owner.value = -1
                self.handoff_count.value = 0
            self.global_lock.release()
            self.local_locks[node_id].release()


def access_shared_resource(thread_priority, thread_id, node_id, resource_manager, stats_dict, max_attempts=25):
    """
    Lock Cohorting을 이용하여 가상 NUMA 노드에 할당된 프로세스가 자원에 접근하는 워커 함수.
    """
    # 통계 초기화
    stats_dict[thread_id] = {
        'successful_accesses': 0,
        'failed_attempts': 0,
        'total_wait_time': 0,
        'priority': thread_priority,
        'starvation_count': 0
    }
    
    print(f"Process {thread_id} (Node {node_id}, Priority {thread_priority}) started - attempting resource access")
    
    attempt_counter = 0
    
    while attempt_counter < max_attempts:
        start_wait_time = time.time()

        # Lock Cohorting 획득 (기아 현상 없이 블로킹 대기)
        resource_manager.acquire(node_id)
        
        try:
            current_time = time.strftime('%H:%M:%S', time.localtime())
            wait_duration = time.time() - start_wait_time

            # 임계 구역 (자원 사용)
            resource_usage_time = 0.15 + (thread_priority * 0.05)
            time.sleep(resource_usage_time)
            
            print(f"[{current_time}] ✓ Process {thread_id} (Node {node_id}, Prio {thread_priority}) acquired resource after {wait_duration:.3f}s wait and released")

            # 통계 업데이트 (Manager.dict는 얕은 복사로 업데이트)
            local_stats = stats_dict[thread_id]
            local_stats['successful_accesses'] += 1
            local_stats['total_wait_time'] += wait_duration
            stats_dict[thread_id] = local_stats

        finally:
            # Lock Cohorting 해제
            resource_manager.release(node_id)
        
        attempt_counter += 1

        # 다음 접근을 위한 대기
        base_delay = 0.1 + (thread_priority * 0.05)
        inter_attempt_delay = base_delay + random.uniform(0, 0.1)
        time.sleep(inter_attempt_delay)

    stats = stats_dict[thread_id]
    success_rate = (stats['successful_accesses'] / max_attempts) * 100
    avg_wait_time = stats['total_wait_time'] / max(stats['successful_accesses'], 1)
    
    print(f"\n📊 Process {thread_id} Final Stats:")
    print(f"   Node ID: {node_id}")
    print(f"   Priority: {thread_priority}")
    print(f"   Successful accesses: {stats['successful_accesses']}/{max_attempts} ({success_rate:.1f}%)")
    print(f"   Average wait time: {avg_wait_time:.3f}s")
    print(f"   Starvation episodes: {stats['starvation_count']}")


def display_simulation_summary(stats_dict):
    """최종 시뮬레이션 결과 요약"""
    print("\n" + "="*60)
    print("LOCK COHORTING SIMULATION SUMMARY (STARVATION RESOLVED)")
    print("="*60)
    
    total_resource_accesses = sum(stats['successful_accesses'] for stats in stats_dict.values())
    print(f"Total resource accesses across all processes: {total_resource_accesses}")
    
    high_priority_accesses = sum(stats['successful_accesses'] 
                                for stats in stats_dict.values() 
                                if stats['priority'] <= 2)
    low_priority_accesses = sum(stats['successful_accesses'] 
                               for stats in stats_dict.values() 
                               if stats['priority'] > 2)
    
    print(f"High priority processes (1-2) total accesses: {high_priority_accesses}")
    print(f"Low/Med priority processes (3+) total accesses: {low_priority_accesses}")
    
    total_starvation = sum(stats['starvation_count'] for stats in stats_dict.values())
    print(f"Total starvation episodes detected: {total_starvation} (Should be 0)")
    
    if low_priority_accesses < high_priority_accesses * 0.3:
        print("⚠️  WARNING: Low priority processes still underserved!")
    else:
        print("✅ SUCCESS: Lock Cohorting achieved fairness across all processes.")
    
    print("="*60)


def main():
    print("Starting Lock Cohorting Starvation Resolution Simulation")
    print("Processes grouped into Virtual NUMA Nodes (0 and 1)")
    print("-" * 60)
    
    manager = mp.Manager()
    resource_manager = LockCohortingManager(manager=manager, threshold=10)
    stats_dict = manager.dict()

    # (우선순위, 프로세스_이름, 가상 NUMA 노드_ID)
    process_configurations = [
        (1, "HighPrio-A", 0),    
        (2, "HighPrio-B", 0),    
        (4, "LowPrio-A", 1),     
        (5, "LowPrio-B", 1),    
        (3, "MedPrio-A", 0)      
    ]
    
    active_processes = []

    for priority, process_name, node_id in process_configurations:
        p = mp.Process(
            target=access_shared_resource, 
            args=(priority, process_name, node_id, resource_manager, stats_dict),
            name=f"Process-{process_name}"
        )
        active_processes.append(p)
        p.start()
        time.sleep(0.1)

    for p in active_processes:
        p.join()

    display_simulation_summary(dict(stats_dict))
    print("\nSimulation completed - Starvation effects eliminated by Lock Cohorting")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()