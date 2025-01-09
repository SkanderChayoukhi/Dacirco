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
from dacirco.controller.grace_period import GracePeriod
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
        algorithm=AlgorithmName.grace_period,
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
def grace_period(
    params: tuple[
        SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]
    ],
) -> GracePeriod:
    """
    Create an instance of GracePeriod using the provided parameters.

    Args:
        params (tuple): A tuple containing:
            - SchedulerParameters: Parameters for the scheduler.
            - dict[str, PodParameters]: A dictionary mapping pod names to their parameters.
            - dict[str, VMParameters]: A dictionary mapping VM names to their parameters.

    Returns:
        GracePeriod: An instance of the GracePeriod class.
    """
    return GracePeriod(*params)


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


def test_new_request_no_request(grace_period: GracePeriod):
    """
    Test the `new_request` method of the `GracePeriod` class when there are no existing requests.

    This test verifies that when a new request is made and there are no waiting, active, or running workers,
    the `new_request` method returns empty lists for both waiting and active requests.

    Args:
        grace_period (GracePeriod): An instance of the `GracePeriod` class.

    Asserts:
        The `new_request` method returns a tuple of two empty lists.
    """
    waiting_requests = []
    active_requests = []
    running_workers = []
    new_request_id = uuid4()
    assert grace_period.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_new_request_no_waiting_requests(
    grace_period: GracePeriod,
    new_request: Callable[[int, TCRequestStatus], TCRequest],
):
    """
    Test the behavior of the `new_request` method in the `GracePeriod` class when there are no waiting requests and a new request is made.

    Args:
        grace_period (GracePeriod): An instance of the GracePeriod class.
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
    assert grace_period.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == (["default"], [])


def test_new_request_max_workers_reached(
    grace_period: GracePeriod,
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test case for the scenario where a new request is made but the maximum number of workers is already reached.

    Args:
        grace_period (GracePeriod): An instance of the GracePeriod class.
        new_request (Callable[[int], TCRequest]): A callable that generates a new request given an integer ID.
        new_worker (Callable[[int, TCWorkerStatus], TCWorker]): A callable that generates a new worker given an integer ID and a worker status.

    Setup:
        - Creates a list with one waiting request.
        - Creates an empty list for active requests.
        - Creates a list of three running workers, all marked as busy.

    Asserts:
        - The `new_request` method of `grace_period` returns two empty lists, indicating no changes to the waiting and active requests.
    """
    waiting_requests = [new_request(4, TCRequestStatus.waiting)]
    active_requests = [
        new_request(n + 1, TCRequestStatus.transcoding) for n in range(3)
    ]
    running_workers = [new_worker(n + 1, TCWorkerStatus.busy) for n in range(3)]
    new_request_id = waiting_requests[0].request_id
    assert grace_period.new_request(
        new_request_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_worker_available_no_waiting_requests(
    grace_period: GracePeriod,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test case for the `worker_available` method of the `GracePeriod` class when there are no waiting requests.

    Args:
        grace_period (GracePeriod): An instance of the GracePeriod class.
        new_worker (Callable[[int, TCWorkerStatus], TCWorker]): A callable that creates a new worker with a given ID and status.

    Setup:
        - Creates an empty list for waiting requests.
        - Creates an empty list for active requests.
        - Creates a new worker with ID 1 and status `idle`.
        - Adds the new worker to the list of running workers.

    Asserts:
        - The `worker_available` method returns a tuple with two empty lists.
    """
    waiting_requests = []
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id
    assert grace_period.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    ) == ([], [])


def test_worker_available_waiting_requests(
    grace_period: GracePeriod,
    new_request: Callable[[int], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test the scenario where a worker becomes available and there are waiting requests.

    Args:
        grace_period (GracePeriod): An instance of the GracePeriod class.
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
    assert grace_period.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    ) == ([], [(worker_id, waiting_requests[0].request_id)])

    def test_periodic_update_no_idle_workers(
        grace_period: GracePeriod,
        new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """
        Test the `periodic_update` method when there are no idle workers.

        Args:
            grace_period (GracePeriod): An instance of the GracePeriod class.
            new_worker (Callable[[int, TCWorkerStatus], TCWorker]): A callable that creates a new worker with a given ID and status.

        Setup:
            - Creates an empty list for waiting requests.
            - Creates an empty list for active requests.
            - Creates a list of running workers, all marked as busy.

        Asserts:
            - The `periodic_update` method returns a tuple with three empty lists.
        """
        waiting_requests = []
        active_requests = []
        running_workers = [new_worker(n, TCWorkerStatus.busy) for n in range(3)]
        assert grace_period.periodic_update(
            waiting_requests, active_requests, running_workers
        ) == ([], [], [])

    def test_periodic_update_idle_workers_within_grace_period(
        grace_period: GracePeriod,
        new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """
        Test the `periodic_update` method when there are idle workers within the grace period.

        Args:
            grace_period (GracePeriod): An instance of the GracePeriod class.
            new_worker (Callable[[int, TCWorkerStatus], TCWorker]): A callable that creates a new worker with a given ID and status.

        Setup:
            - Creates an empty list for waiting requests.
            - Creates an empty list for active requests.
            - Creates a list of running workers, all marked as idle but within the grace period.

        Asserts:
            - The `periodic_update` method returns a tuple with three empty lists.
        """
        waiting_requests = []
        active_requests = []
        running_workers = [new_worker(n, TCWorkerStatus.idle) for n in range(3)]
        for worker in running_workers:
            worker.t_idle_start = datetime.now()
        assert grace_period.periodic_update(
            waiting_requests, active_requests, running_workers
        ) == ([], [], [])


def test_periodic_update_idle_workers_exceeding_grace_period(
    grace_period: GracePeriod,
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):
    """
    Test the `periodic_update` method when there are idle workers exceeding the grace period.

    Args:
        grace_period (GracePeriod): An instance of the GracePeriod class.
        new_worker (Callable[[int, TCWorkerStatus], TCWorker]): A callable that creates a new worker with a given ID and status.

    Setup:
        - Creates an empty list for waiting requests.
        - Creates an empty list for active requests.
        - Creates a list of running workers, all marked as idle and exceeding the grace period.

    Asserts:
        - The `periodic_update` method returns a tuple with an empty list, a list of worker IDs to be deleted, and an empty list.
    """
    waiting_requests = []
    active_requests = []
    running_workers = [new_worker(n, TCWorkerStatus.idle) for n in range(3)]
    for worker in running_workers:
        worker.t_idle_start = datetime.now().replace(year=2023)
    expected_workers_to_delete = [worker.worker_id for worker in running_workers]
    assert grace_period.periodic_update(
        waiting_requests, active_requests, running_workers
    ) == ([], expected_workers_to_delete, [])
