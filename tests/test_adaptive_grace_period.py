import pytest
from datetime import datetime, timedelta
from random import choice
from uuid import uuid4
from collections.abc import Callable
from dacirco.dataclasses import (
    PodParameters,
    SchedulerParameters,
    TCWorker,
    VMParameters,
    TCRequest,
)
from dacirco.controller.adaptive_grace_period import AdaptiveGracePeriod
from dacirco.enums import AlgorithmName, RunnerType, TCRequestStatus, TCWorkerStatus


@pytest.fixture
def params() -> tuple[SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]]:
    """
    Generates the parameters required for the scheduler, pods, and VMs.

    Returns:
        tuple: A tuple containing:
            - SchedulerParameters: Configuration for the scheduler.
            - dict[str, PodParameters]: A dictionary mapping pod names to their parameters.
            - dict[str, VMParameters]: A dictionary mapping VM names to their parameters.
    """
    sched_params = SchedulerParameters(
        max_workers=3,
        call_interval=10,
        max_idle_time=20,
        algorithm=AlgorithmName.adaptive_grace_period,
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


@pytest.fixture
def adaptive_grace_period(
    params: tuple[SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]],
) -> AdaptiveGracePeriod:
    """
    Create an instance of AdaptiveGracePeriod using the provided parameters.
    """
    return AdaptiveGracePeriod(*params)


@pytest.fixture
def new_request() -> Callable[[int, TCRequestStatus], TCRequest]:
    """
    Creates a new request factory function.
    """
    def _new_request(n: int, status: TCRequestStatus = TCRequestStatus.waiting) -> TCRequest:
        request_id = uuid4()
        speed = choice(["fast", "medium", "faster", "veryfast", "ultrafast"])
        t_received = datetime.fromisoformat(f"2024-10-01T00:{n:02}:00")
        request = TCRequest(
            request_id=request_id,
            input_video=f"input_{n}.mp4",
            output_video=f"output_{n}.mp4",
            t_received=t_received,
            bitrate=n,
            speed=speed,
            status=status,
        )
        return request

    return _new_request


@pytest.fixture
def new_worker() -> Callable[[int, TCWorkerStatus], TCWorker]:
    """
    Creates a new worker factory function.
    """
    def _new_worker(n: int, status: TCWorkerStatus) -> TCWorker:
        worker_id = uuid4()
        worker = TCWorker(
            worker_id=worker_id,
            name=f"worker_{n}",
            param_key="default",
            t_created=datetime.fromisoformat(f"2024-10-01T00:{n:02}:00"),
            status=status,
        )
        return worker

    return _new_worker


def test_new_request_no_request(adaptive_grace_period: AdaptiveGracePeriod):
    """
    Test the `new_request` method of the `AdaptiveGracePeriod` class when there are no existing requests.
    """
    waiting_requests = []
    active_requests = []
    running_workers = []
    new_request_id = uuid4()
    assert adaptive_grace_period.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_new_request_no_waiting_requests(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_request: Callable[[int, TCRequestStatus], TCRequest],
):
    """
    Test the behavior of the `new_request` method in the `AdaptiveGracePeriod` class when there are no waiting requests and a new request is made.
    """
    waiting_requests = [new_request(1, TCRequestStatus.waiting)]
    active_requests = []
    running_workers = []
    new_request_id = waiting_requests[0].request_id
    assert adaptive_grace_period.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == (["default"], [])


def test_new_request_max_workers_reached(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test case for the scenario where a new request is made but the maximum number of workers is already reached.
    """
    waiting_requests = [new_request(4, TCRequestStatus.waiting)]
    active_requests = [
        new_request(n + 1, TCRequestStatus.transcoding) for n in range(3)
    ]
    running_workers = [new_worker(n + 1, TCWorkerStatus.busy) for n in range(3)]
    new_request_id = waiting_requests[0].request_id
    assert adaptive_grace_period.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_worker_available_no_waiting_requests(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test case for the `worker_available` method of the `AdaptiveGracePeriod` class when there are no waiting requests.
    """
    waiting_requests = []
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id
    assert adaptive_grace_period.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_worker_available_waiting_requests(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_request: Callable[[int], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test the scenario where a worker becomes available and there are waiting requests.
    """
    waiting_requests = [new_request(1)]
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id
    assert adaptive_grace_period.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    ) == ([], [(worker_id, waiting_requests[0].request_id)])


def test_periodic_update_no_idle_workers(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test the `periodic_update` method when there are no idle workers.
    """
    waiting_requests = []
    active_requests = []
    running_workers = [new_worker(n, TCWorkerStatus.busy) for n in range(3)]
    assert adaptive_grace_period.periodic_update(
        waiting_requests, active_requests, running_workers
    ) == ([], [], [])


def test_periodic_update_idle_workers_within_grace_period(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test the `periodic_update` method when there are idle workers within the grace period.
    """
    waiting_requests = []
    active_requests = []
    running_workers = [new_worker(n, TCWorkerStatus.idle) for n in range(3)]
    for worker in running_workers:
        worker.t_idle_start = datetime.now()
    assert adaptive_grace_period.periodic_update(
        waiting_requests, active_requests, running_workers
    ) == ([], [], [])


def test_periodic_update_idle_workers_exceeding_grace_period(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test the `periodic_update` method when there are idle workers exceeding the grace period.
    """
    waiting_requests = []
    active_requests = []
    running_workers = [new_worker(n, TCWorkerStatus.idle) for n in range(3)]
    for worker in running_workers:
        worker.t_idle_start = datetime.now().replace(year=2023)
    expected_workers_to_delete = [worker.worker_id for worker in running_workers]
    assert adaptive_grace_period.periodic_update(
        waiting_requests, active_requests, running_workers
    ) == ([], expected_workers_to_delete, [])


# def test_periodic_update_grace_period_adjustment(
#     adaptive_grace_period: AdaptiveGracePeriod,
#     new_worker: Callable[[int, TCWorkerStatus], TCWorker],
#     new_request: Callable[[int], TCRequest],
# ):
#     """
#     Test `periodic_update` with dynamic grace period adjustments.
#     """
#     waiting_requests = [new_request(1)]
#     active_requests = [new_request(2, TCRequestStatus.transcoding)]
#     running_workers = [
#         new_worker(1, TCWorkerStatus.idle),
#         new_worker(2, TCWorkerStatus.busy),
#     ]
#     # Simulate workers exceeding the initial grace period
#     for worker in running_workers:
#         worker.t_idle_start = datetime.now() - timedelta(seconds=25)
    
#     result = adaptive_grace_period.periodic_update(
#         waiting_requests, active_requests, running_workers
#     )
    
#     # Expect new workers to be created due to high request rate
#     assert result[0] == ["default"]


def test_periodic_update_grace_period_adjustment(
    adaptive_grace_period: AdaptiveGracePeriod,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    new_request: Callable[[int], TCRequest],
):
    """
    Test `periodic_update` with dynamic grace period adjustments.
    """
    # Create enough waiting requests to ensure a high request rate
    waiting_requests = [new_request(1), new_request(2), new_request(3)]  # Increased requests for higher rate
    active_requests = [new_request(4, TCRequestStatus.transcoding)]
    
    # Simulate running workers (some idle, some busy)
    running_workers = [
        new_worker(1, TCWorkerStatus.idle),
        new_worker(2, TCWorkerStatus.busy),
    ]
    
    # Simulate workers exceeding the initial grace period
    for worker in running_workers:
        worker.t_idle_start = datetime.now() - timedelta(seconds=25)
    
    # Simulate enough elapsed time for periodic update
    adaptive_grace_period._last_update_time = datetime.now() - timedelta(seconds=2)
    
    # Run the periodic update
    result = adaptive_grace_period.periodic_update(
        waiting_requests, active_requests, running_workers
    )
    
    # Debugging: Print out the internal state and result
    print("Recent request rate:", adaptive_grace_period._recent_request_rate)
    print("Worker utilization:", len(active_requests) / max(1, len(running_workers)))
    
    # Expect new workers to be created due to high request rate
    assert result[0] == ["default"], f"Expected ['default'], got {result[0]}"
    # Expect idle workers exceeding grace period to be removed
    assert result[1] == [running_workers[0].worker_id], f"Expected {running_workers[0].worker_id}, got {result[1]}"
    # No specific assignments are expected
    assert result[2] == []

