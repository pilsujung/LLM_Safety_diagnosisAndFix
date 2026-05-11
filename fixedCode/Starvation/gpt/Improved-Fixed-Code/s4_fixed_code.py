import multiprocessing as mp
import time
import random
from enum import Enum

class Priority(Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3

    def __str__(self) -> str:
        return self.name

class ResourceManager:
    """
    Lock Cohorting (2012) 메커니즘을 적용한 계층적 락 기반 자원 관리자.
    가상의 NUMA 노드 그룹 간의 공정성을 유지하면서 기아(Starvation) 현상을 방지.
    """
    def __init__(self, manager, threshold=10, starvation_threshold=10.0):
        # 공유 상태 보호용 락
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
        
        # 제어 임계치 설정
        self.threshold = threshold
        self.starvation_threshold = starvation_threshold

        # 통계 데이터 기록용 프록시 객체
        self.access_logs = manager.list()
        self.wait_times = manager.list()
        self.starvation_events = manager.list()

    def request_access(self, process_id, priority, node_id):
        """자원 접근 요청 (Lock Cohorting Acquire)"""
        req_time = time.time()
        
        log_entry = f"[{req_time % 100:.2f}] + Process #{process_id} (Node {node_id}, {priority}) requested"
        self.access_logs.append(log_entry)
        print(log_entry)

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

        # === Critical Section 진입 ===
        wait_time = time.time() - req_time
        
        # 기아 현상 체크 (Starvation threshold 초과 여부)
        if wait_time > self.starvation_threshold:
            self.starvation_events.append((process_id, priority.name, wait_time))
            
        self.wait_times.append((process_id, priority.name, wait_time))
        
        log_entry = f"[{time.time() % 100:.2f}] ▶ Start Process #{process_id} (Node {node_id}, {priority}) wait={wait_time:.2f}"
        self.access_logs.append(log_entry)
        print(log_entry)

    def release_access(self, process_id, priority, node_id, duration):
        """자원 접근 해제 (Lock Cohorting Release)"""
        log_entry = f"[{time.time() % 100:.2f}] ✓ Done Process #{process_id} (Node {node_id}, {priority}) dur={duration:.2f}"
        self.access_logs.append(log_entry)
        print(log_entry)

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

def worker_process(process_id, node_id, priority, num_requests, rm, delay_range):
    """
    할당된 가상 NUMA 노드에서 Lock Cohorting을 이용해 자원에 접근하는 워커 프로세스
    """
    random.seed(time.time() + process_id)
    try:
        for _ in range(num_requests):
            duration = random.uniform(0.5, 2.0)
            
            # 임계 구역 요청
            rm.request_access(process_id, priority, node_id)
            
            # 실제 작업 수행 (임계 구역)
            time.sleep(duration)
            
            # 임계 구역 해제
            rm.release_access(process_id, priority, node_id, duration)

            # 다음 요청까지 대기
            time.sleep(random.uniform(*delay_range))
            
    except Exception as e:
        print(f"Error in worker process {process_id}: {e}")

def run_simulation():
    print("=== LOCK COHORTING RESOURCE ALLOCATION SIMULATION ===")
    manager = mp.Manager()
    
    # 임계치(handoff threshold) = 10, 기아 임계치(starvation threshold) = 10.0초
    rm = ResourceManager(manager=manager, threshold=10, starvation_threshold=10.0)

    # 프로세스 스펙: (process_id, node_id, priority, num_requests)
    specs = [
        (1, 0, Priority.HIGH, 5),
        (2, 0, Priority.MEDIUM, 4),
        (3, 0, Priority.LOW, 3),
        (4, 1, Priority.HIGH, 5),
        (5, 1, Priority.LOW, 6),
        (6, 1, Priority.MEDIUM, 4)
    ]

    processes = []
    for pid, nid, prio, reqs in specs:
        p = mp.Process(
            target=worker_process,
            args=(pid, nid, prio, reqs, rm, (0.5, 1.5))
        )
        processes.append(p)

    # 시뮬레이션 시작
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    # --- 통계 분석 및 출력 ---
    wait_times_list = list(rm.wait_times)
    starvation_list = list(rm.starvation_events)
    
    total_requests = len(wait_times_list)
    by_priority = {p.name: 0 for p in Priority}
    wait_sum_by_priority = {p.name: 0.0 for p in Priority}
    
    for _, prio_name, wait_time in wait_times_list:
        by_priority[prio_name] += 1
        wait_sum_by_priority[prio_name] += wait_time

    avg_wait_time = sum(w for _, _, w in wait_times_list) / total_requests if total_requests else 0.0
    
    print("\n=== STATISTICS ===")
    print(f"Total requests     : {total_requests}")
    print(f"Completed          : {total_requests}")
    print("Requests by priority:")
    for p in Priority:
        print(f"  - {p.name}: {by_priority[p.name]}")
        
    print(f"Average wait time  : {avg_wait_time:.2f}s")
    print("Average wait by priority:")
    for p in Priority:
        count = by_priority[p.name]
        avg = wait_sum_by_priority[p.name] / count if count else 0.0
        print(f"  - {p.name}: {avg:.2f}s")
        
    print(f"Starving requests  : {len(starvation_list)}")
    if starvation_list:
        print("Starving events detected:")
        for pid, prio, wait in starvation_list:
            print(f"  - Process #{pid} ({prio}) waited {wait:.2f}s")
    else:
        print("  - 기아 현상 없음 (Lock Cohorting을 통한 공정성 확보 성공)")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    run_simulation()