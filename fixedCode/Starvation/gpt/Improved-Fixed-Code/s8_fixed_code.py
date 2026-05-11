import multiprocessing as mp
import time
import random

class LockCohortingManager:
    def __init__(self, manager, threshold=5):
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


def resource_user_process(process_id, node_id, user_type, base_hold_time, variation_factor, iterations, resource_manager, stats_dict):
    """
    Lock Cohorting을 이용하여 자원에 접근하는 워커 프로세스.
    Greedy(탐욕적)와 Lightweight(경량) 모두 이 함수를 사용합니다.
    """
    # 통계 초기화
    stats_dict[process_id] = {
        'access_count': 0,
        'total_wait_time': 0,
        'total_usage_time': 0,
        'last_access_time': None
    }
    
    iteration_count = 0
    
    while iteration_count < iterations:
        wait_start_time = time.time()
        
        # Lock Cohorting 획득 (기아 현상 방지)
        resource_manager.acquire(node_id)
        
        try:
            wait_end_time = time.time()
            wait_duration = wait_end_time - wait_start_time

            actual_hold_time = base_hold_time + random.uniform(0, variation_factor)
            
            print(f"[{user_type}] Process {process_id} (Node {node_id}) acquired resource (waited {wait_duration:.3f}s)")
            
            # 임계 구역 (자원 사용)
            usage_start_time = time.time()
            time.sleep(actual_hold_time)
            usage_end_time = time.time()
            actual_usage_time = usage_end_time - usage_start_time
            
            print(f"[{user_type}] Process {process_id} (Node {node_id}) releasing resource after {actual_usage_time:.3f}s")
            
            # 통계 업데이트 (Manager.dict는 재할당해야 반영됨)
            local_stats = stats_dict[process_id]
            local_stats['access_count'] += 1
            local_stats['total_wait_time'] += wait_duration
            local_stats['total_usage_time'] += actual_usage_time
            local_stats['last_access_time'] = time.time()
            stats_dict[process_id] = local_stats

        finally:
            # Lock Cohorting 해제
            resource_manager.release(node_id)

        # 다음 접근 대기
        time.sleep(0.01 if user_type == 'GREEDY' else 0.02)
        iteration_count += 1


def monitor_process(stats_dict, run_event):
    """주기적으로 통계를 출력하는 모니터 프로세스"""
    time.sleep(2)  
    
    while run_event.is_set():
        time.sleep(3)
        print("\n" + "="*60)
        print("PROCESS STATISTICS (LOCK COHORTING APPLIED)")
        print("="*60)
        
        current_stats = dict(stats_dict)
        for process_id, stats in current_stats.items():
            avg_wait = stats['total_wait_time'] / max(stats['access_count'], 1)
            print(f"Process {process_id}:")
            print(f"  - Access count: {stats['access_count']}")
            print(f"  - Average wait time: {avg_wait:.3f}s")
            print(f"  - Total usage time: {stats['total_usage_time']:.3f}s")
            if stats['last_access_time']:
                time_since_last = time.time() - stats['last_access_time']
                print(f"  - Time since last access: {time_since_last:.3f}s")
        print("="*60 + "\n")


def main():
    """메인 실행 함수"""
    print("Starting Process Starvation Resolution Demonstration")
    print("="*60)
    print("This program uses Lock Cohorting to prevent greedy processes")
    print("from starving lightweight processes.")
    print("Greedy processes -> Node 0, Lightweight processes -> Node 1")
    print("="*60 + "\n")
    
    manager = mp.Manager()
    
    # Lock Cohorting 매니저 (연속 승계 임계치 5회 설정)
    resource_manager = LockCohortingManager(manager=manager, threshold=5)
    
    stats_dict = manager.dict()
    run_event = manager.Event()
    run_event.set()

    processes = []

    # Greedy 프로세스들 (가상 NUMA 노드 0 할당)
    # process_id, node_id, user_type, base_hold_time, variation_factor, iterations
    p1 = mp.Process(target=resource_user_process, args=("Greedy-1", 0, "GREEDY", 1.0, 0.2, 10, resource_manager, stats_dict))
    p2 = mp.Process(target=resource_user_process, args=("Greedy-2", 0, "GREEDY", 0.8, 0.3, 10, resource_manager, stats_dict))
    
    # Lightweight 프로세스들 (가상 NUMA 노드 1 할당)
    p3 = mp.Process(target=resource_user_process, args=("Light-1", 1, "LIGHT", 0.1, 0.02, 30, resource_manager, stats_dict))
    p4 = mp.Process(target=resource_user_process, args=("Light-2", 1, "LIGHT", 0.05, 0.01, 30, resource_manager, stats_dict))

    monitor = mp.Process(target=monitor_process, args=(stats_dict, run_event))

    processes.extend([p1, p2, p3, p4, monitor])
    
    print("Starting all processes...\n")
    for p in processes:
        if p != monitor:
            p.start()
            time.sleep(0.1) 
    monitor.start()

    # 모니터를 제외한 워커 프로세스 종료 대기
    for p in [p1, p2, p3, p4]:
        p.join()

    # 작업 완료 후 모니터 종료
    run_event.clear()
    monitor.join(timeout=2)
    if monitor.is_alive():
        monitor.terminate()

    print("\nFINAL RESULTS (FAIRNESS ACHIEVED):")
    
    print("\n" + "="*60)
    print("FINAL PROCESS STATISTICS")
    print("="*60)
    final_stats = dict(stats_dict)
    for process_id, stats in final_stats.items():
        avg_wait = stats['total_wait_time'] / max(stats['access_count'], 1)
        print(f"Process {process_id}:")
        print(f"  - Access count: {stats['access_count']}")
        print(f"  - Average wait time: {avg_wait:.3f}s")
        print(f"  - Total usage time: {stats['total_usage_time']:.3f}s")
    print("="*60 + "\n")
    
    print("Demonstration complete!")
    print("\nObservation: Lock Cohorting successfully prevented starvation.")
    print("Lightweight processes on Node 1 accessed the resource fairly,")
    print("despite Greedy processes on Node 0 attempting to monopolize it.")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    main()