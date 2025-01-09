import pytest
from datetime import datetime
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
from dacirco.controller.minimize_workers import MinimizeWorkers
from dacirco.enums import AlgorithmName, RunnerType, TCRequestStatus, TCWorkerStatus


@pytest.fixture
def params() -> (
    tuple[SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]]
):
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
        algorithm=AlgorithmName.minimize_workers,
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
def minimize_workers(
    params: tuple[
        SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]
    ],
) -> MinimizeWorkers:
    """
    Create an instance of MinimizeWorkers using the provided parameters.

    Args:
        params (tuple): A tuple containing:
            - SchedulerParameters: Parameters for the scheduler.
            - dict[str, PodParameters]: A dictionary mapping pod names to their parameters.
            - dict[str, VMParameters]: A dictionary mapping VM names to their parameters.

    Returns:
        MinimizeWorkers: An instance of the MinimizeWorkers class.
    """
    return MinimizeWorkers(*params)


@pytest.fixture
def new_request() -> Callable[[int, TCRequestStatus], TCRequest]:
    """
    Creates a new request factory function.

    Returns:
        Callable[[int], TCRequest]: A function that generates a TCRequest object
        when called with an integer argument. The integer argument is used to
        set the bitrate and part of the timestamp for the request.

    The generated TCRequest object includes:
        - request_id: A unique identifier for the request.
        - input_video: The name of the input video file, formatted as "input_{n}.mp4".
        - output_video: The name of the output video file, formatted as "output_{n}.mp4".
        - t_received: The timestamp when the request was received, set to "2024-10-01T00:{n:02}:00".
        - bitrate: The bitrate for the request, set to the integer argument.
        - speed: The speed of the request processing, randomly chosen from
          ["fast", "medium", "faster", "veryfast", "ultrafast"].
    """

    def _new_request(
        n: int, status: TCRequestStatus = TCRequestStatus.waiting
    ) -> TCRequest:
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

    Returns:
        Callable[[int, TCWorkerStatus], TCWorker]: A function that takes an integer `n` and a `TCWorkerStatus` object,
        and returns a `TCWorker` instance with a unique worker ID, a name based on `n`, a default parameter key,
        a creation time based on `n`, and the given status.
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


def test_new_request_no_request(minimize_workers: MinimizeWorkers):
    """
    Test the `new_request` method of the `MinimizeWorkers` class when there are no existing requests.

    This test verifies that when a new request is made and there are no waiting, active, or running workers,
    the `new_request` method returns empty lists for both waiting and active requests.

    Args:
        minimize_workers (MinimizeWorkers): An instance of the `MinimizeWorkers` class.

    Asserts:
        The `new_request` method returns a tuple of two empty lists.
    """
    waiting_requests = []
    active_requests = []
    running_workers = []
    new_request_id = uuid4()
    assert minimize_workers.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_new_request_no_waiting_requests(
    minimize_workers: MinimizeWorkers,
    new_request: Callable[[int, TCRequestStatus], TCRequest],
):
    """
    Test the behavior of the `new_request` method in the `MinimizeWorkers` class when there are no waiting requests and a new request is made.

    Args:
        minimize_workers (MinimizeWorkers): An instance of the MinimizeWorkers class.
        new_request (Callable[[int], TCRequest]): A callable that generates a new request with a given ID.

    Sets up:
        - A list with one waiting request.
        - An empty list of active requests.
        - An empty list of running workers.
        - The ID of the new request.

    Asserts:
        - The `new_request` method returns a tuple with a list containing "default" and an empty list.
    """
    waiting_requests = [new_request(1, TCRequestStatus.waiting)]
    active_requests = []
    running_workers = []
    new_request_id = waiting_requests[0].request_id
    assert minimize_workers.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == (["default"], [])


def test_new_request_max_workers_reached(
    minimize_workers: MinimizeWorkers,
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test case for the scenario where a new request is made but the maximum number of workers is already reached.

    Args:
        minimize_workers (MinimizeWorkers): An instance of the MinimizeWorkers class.
        new_request (Callable[[int], TCRequest]): A callable that generates a new request given an integer ID.
        new_worker (Callable[[int, TCWorkerStatus], TCWorker]): A callable that generates a new worker given an integer ID and a worker status.

    Setup:
        - Creates a list with one waiting request.
        - Creates an empty list for active requests.
        - Creates a list of three running workers, all marked as busy.

    Asserts:
        - The `new_request` method of `minimize_workers` returns two empty lists, indicating no changes to the waiting and active requests.
    """
    waiting_requests = [new_request(4, TCRequestStatus.waiting)]
    active_requests = [new_request(n, TCRequestStatus.transcoding) for n in range(3)]
    running_workers = [new_worker(n, TCWorkerStatus.busy) for n in range(3)]
    new_request_id = waiting_requests[0].request_id
    assert minimize_workers.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_worker_available_no_waiting_requests(
    minimize_workers: MinimizeWorkers,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test case for the `worker_available` method of the `MinimizeWorkers` class when there are no waiting requests.

    Args:
        minimize_workers (MinimizeWorkers): An instance of the MinimizeWorkers class.
        new_worker (Callable[[int, TCWorkerStatus], TCWorker]): A callable that creates a new worker with a given ID and status.

    Setup:
        - Creates an empty list for waiting requests.
        - Creates an empty list for active requests.
        - Creates a new worker with ID 1 and status `idle`.
        - Adds the new worker to the list of running workers.

    Asserts:
        - The `worker_available` method returns a tuple where the first element is a list containing the worker ID and the second element is an empty list.
    """
    waiting_requests = []
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id
    assert minimize_workers.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    ) == ([worker_id], [])


def test_worker_available_waiting_requests(
    minimize_workers: MinimizeWorkers,
    new_request: Callable[[int], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test the scenario where a worker becomes available and there are waiting requests.

    Args:
        minimize_workers (MinimizeWorkers): An instance of the MinimizeWorkers class.
        new_request (Callable[[int], TCRequest]): A callable that generates a new request given an integer.

    Test Steps:
        1. Create a list of waiting requests with one new request.
        2. Create an empty list for active requests.
        3. Create an empty list for running workers.
        4. Generate a unique worker ID.
        5. Assert that when a worker becomes available, the function returns an empty list for active requests
           and a list containing a tuple of the worker ID and the request ID for the waiting request.
    """
    waiting_requests = [new_request(1)]
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id
    assert minimize_workers.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    ) == ([], [(worker_id, waiting_requests[0].request_id)])
