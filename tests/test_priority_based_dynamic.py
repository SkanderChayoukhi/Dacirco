import pytest
from datetime import datetime, timedelta
from uuid import uuid4
from random import choice
from typing import Callable
from dacirco.dataclasses import (
    PodParameters,
    SchedulerParameters,
    TCWorker,
    VMParameters,
    TCRequest,
)
from dacirco.controller.priority_based_dynamic import PriorityBasedDynamic
from dacirco.enums import AlgorithmName, RunnerType, TCRequestStatus, TCWorkerStatus


# Fixture to create Scheduler, Pod, and VM parameters
@pytest.fixture
def params() -> tuple[SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]]:
    sched_params = SchedulerParameters(
        max_workers=5,
        call_interval=10,
        max_idle_time=20,
        algorithm=AlgorithmName.priority_based_dynamic,
        concentrate_workers=True,
        spread_workers=False,
        runner_type=RunnerType.vm,
    )
    pod_params = {
        "default": PodParameters(
            image="dacirco-worker",
            cpu_request="100m",
            memory_request="100Mi",
            cpu_limit="500m",
            memory_limit="500Mi",
            node_selector="",
        )
    }
    vm_params = {
        "default": VMParameters(
            image="dacirco-worker",
            flavor="m1.small",
            key="dacirco",
            security_group="dacirco",
            network="dacirco",
        )
    }
    return sched_params, pod_params, vm_params


# Fixture to create PriorityBasedDynamic instance
@pytest.fixture
def priority_based_dynamic(params: tuple[SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]]) -> PriorityBasedDynamic:
    return PriorityBasedDynamic(*params)


# Fixture to create TCRequest
@pytest.fixture
def new_request() -> Callable[[int, TCRequestStatus], TCRequest]:
    def _new_request(n: int, status: TCRequestStatus = TCRequestStatus.waiting) -> TCRequest:
        request_id = uuid4()
        speed = choice(["fast", "medium", "faster", "veryfast", "ultrafast"])
        
        # Correctly format the time to handle minutes over 60
        hours = (n // 60) % 24  # Make sure hours wrap around
        minutes = n % 60
        t_received = datetime.fromisoformat(f"2024-10-01T{hours:02}:{minutes:02}:00")
        
        return TCRequest(
            request_id=request_id,
            input_video=f"input_{n}.mp4",
            output_video=f"output_{n}.mp4",
            t_received=t_received,
            bitrate=n,
            speed=speed,
            status=status,
        )
    return _new_request


# Fixture to create TCWorker
@pytest.fixture
def new_worker() -> Callable[[int, TCWorkerStatus], TCWorker]:
    def _new_worker(n: int, status: TCWorkerStatus) -> TCWorker:
        worker_id = uuid4()
        return TCWorker(
            worker_id=worker_id,
            name=f"worker_{n}",
            param_key="default",
            t_created=datetime.fromisoformat(f"2024-10-01T00:{n:02}:00"),
            status=status,
        )
    return _new_worker


# Test case: No waiting requests
def test_new_request_no_request(priority_based_dynamic: PriorityBasedDynamic):
    waiting_requests = []
    active_requests = []
    running_workers = []
    new_request_id = uuid4()
    # No requests, no workers created
    assert priority_based_dynamic.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


# Test case: New request with high-priority
def test_new_request_with_high_priority(priority_based_dynamic: PriorityBasedDynamic, new_request: Callable[[int, TCRequestStatus], TCRequest]):
    waiting_requests = [new_request(1000), new_request(60000)]  # Ensure bitrate > 5000 for "large"
    active_requests = []
    running_workers = []
    new_request_id = waiting_requests[1].request_id  # High-priority request
    # The second request has a higher bitrate and should trigger "large" worker creation
    result = priority_based_dynamic.new_request(new_request_id, waiting_requests, active_requests, running_workers)
    assert result[0] == ["large"]
    assert result[1] == []


# Test case: Worker available with waiting requests
def test_worker_available(priority_based_dynamic: PriorityBasedDynamic, new_request: Callable[[int, TCRequestStatus], TCRequest], new_worker: Callable[[int, TCWorkerStatus], TCWorker]):
    waiting_requests = [new_request(50000), new_request(700)]  # Higher bitrate for the second request
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id

    workers_to_delete, request_worker_pairs = priority_based_dynamic.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    )

    # Expect the highest-priority request (bitrate 7000) to be assigned
    assert workers_to_delete == []
    assert request_worker_pairs == [(worker.worker_id, waiting_requests[1].request_id)]


# Test case: Periodic update with scaling
# def test_periodic_update(priority_based_dynamic: PriorityBasedDynamic, new_request: Callable[[int, TCRequestStatus], TCRequest], new_worker: Callable[[int, TCWorkerStatus], TCWorker]):
#     waiting_requests = [new_request(8000), new_request(6000), new_request(9000),new_request(10000),new_request(65000),new_request(7000),
#         new_request(1000), new_request(2000), new_request(5000), new_request(12000)]  # High-priority request for scaling
#     active_requests = [new_request(60000, TCRequestStatus.transcoding)]
#     running_workers = [
#         new_worker(1, TCWorkerStatus.idle),  # Idle, exceeds grace period
#         new_worker(2, TCWorkerStatus.busy)
#     ]
    
#     # Simulate idle worker exceeding grace period
#     for worker in running_workers:
#         worker.t_idle_start = datetime.now() - timedelta(seconds=25)
    
#     result = priority_based_dynamic.periodic_update(waiting_requests, active_requests, running_workers)
    
#     # Expect scaling to create a "large" worker due to high-priority requests
#     assert result[0] == ["large"]
#     # Expect idle worker to be removed
#     assert result[1] == [running_workers[0].worker_id]
#     # No request-worker assignments in periodic update
#     assert result[2] == []


def test_periodic_update(priority_based_dynamic: PriorityBasedDynamic, new_request: Callable[[int, TCRequestStatus], TCRequest], new_worker: Callable[[int, TCWorkerStatus], TCWorker]):
    # A larger set of waiting requests to ensure scaling is triggered
    waiting_requests = [
        new_request(8000), new_request(6000), new_request(9000), new_request(10000),
        new_request(65000), new_request(7000), new_request(500), new_request(3000),
        new_request(2000), new_request(4000), new_request(12000), new_request(11000),
        new_request(9500), new_request(1000), new_request(5000), new_request(4500)
    ]
    
    # Active requests, ensuring we have enough to trigger the scaling condition (worker utilization > 80%)
    active_requests = [
        new_request(60000, TCRequestStatus.transcoding),
        new_request(15000, TCRequestStatus.transcoding),
        new_request(25000, TCRequestStatus.transcoding),
        new_request(18000, TCRequestStatus.transcoding),
        new_request(22000, TCRequestStatus.transcoding)
    ]
    
    # 3 running workers, where 1 is idle and 2 are busy
    running_workers = [
        new_worker(1, TCWorkerStatus.idle),  # Idle worker, should exceed grace period
        new_worker(2, TCWorkerStatus.busy),
        new_worker(3, TCWorkerStatus.busy)
    ]
    
    # Simulate idle worker exceeding grace period
    for worker in running_workers:
        worker.t_idle_start = datetime.now() - timedelta(seconds=25)
    
    # Perform the periodic update to check the scaling
    result = priority_based_dynamic.periodic_update(waiting_requests, active_requests, running_workers)
    
    # Debugging: Output the result for inspection
    print(f"Waiting Requests: {len(waiting_requests)}")
    print(f"Running Workers: {len(running_workers)}")
    print(f"Active Requests: {len(active_requests)}")
    
    # Check worker utilization
    utilization = len(active_requests) / max(1, len(running_workers))
    print(f"Worker Utilization: {utilization * 100}%")
    
    # Check if scaling should trigger based on conditions
    should_scale = len(waiting_requests) > len(running_workers) and utilization > 0.8
    print(f"Should Scale (More Requests and > 80% Utilization): {should_scale}")
    
    # Check elapsed time for periodic update scaling condition
    elapsed_time = (datetime.now() - priority_based_dynamic._last_update_time).total_seconds()
    print(f"Elapsed Time: {elapsed_time}s")
    
    # Check scaling decision
    print(f"Scaling Decision: {result[0]} (Expected ['large'])")
    
    # Expect scaling to create a "large" worker due to high-priority requests
    assert result[0] == ["large"], f"Expected ['large'], got {result[0]}"
    
    # Expect idle worker (worker 1) to be removed due to exceeding grace period
    assert result[1] == [running_workers[0].worker_id], f"Expected [{running_workers[0].worker_id}], got {result[1]}"
    
    # No request-worker assignments in periodic update
    assert result[2] == [], f"Expected [], got {result[2]}"



