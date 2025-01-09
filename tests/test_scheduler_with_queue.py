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
from dacirco.controller.scheduler_with_queue import SchedulerWithQueue
from dacirco.enums import AlgorithmName, RunnerType, TCRequestStatus, TCWorkerStatus


@pytest.fixture
def params() -> (
    tuple[SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters], int]
):
    """
    Generates the parameters required for the scheduler, pods, and VMs.

    Returns:
        tuple: A tuple containing:
            - SchedulerParameters: Configuration for the scheduler.
            - dict[str, PodParameters]: A dictionary mapping pod names to their parameters.
            - dict[str, VMParameters]: A dictionary mapping VM names to their parameters.
            - int: the queue threshold
    """
    sched_params = SchedulerParameters(
        max_workers=3,
        call_interval=10,
        max_idle_time=20,
        algorithm=AlgorithmName.scheduler_with_queue,
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
def scheduler_with_queue(
    params: tuple[
        SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]
    ],
) -> SchedulerWithQueue:
    """
    Create an instance of SchedulerWithQueue using the provided parameters.

    Args:
        params (tuple): A tuple containing:
            - SchedulerParameters: Parameters for the scheduler.
            - dict[str, PodParameters]: A dictionary mapping pod names to their parameters.
            - dict[str, VMParameters]: A dictionary mapping VM names to their parameters.

    Returns:
        SchedulerWithQueue: An instance of the SchedulerWithQueue class.
    """
    return SchedulerWithQueue(*params)


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




""" 

new request

"""



def test_new_request_idle_workers(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """Test case: No idle workers."""
        waiting_requests = [new_request(1, TCRequestStatus.waiting)]
        active_requests = []
        running_workers = [new_worker(1, TCWorkerStatus.idle)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_queue.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 0 
      #  assert len(request_worker_pairs) == 1
      #  assert request_worker_pairs[0][1] == new_request_id




def test_new_request_no_idle_workers_waiting_requests_above_threshold_max_workers_not_reached(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        
        waiting_requests = [new_request(n+3, TCRequestStatus.waiting) for n in range(9)]
        active_requests = [new_request(1, TCRequestStatus.transcoding), new_request(2, TCRequestStatus.transcoding)]
        running_workers = [new_worker(1, TCWorkerStatus.busy), new_worker(2, TCWorkerStatus.busy)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_queue.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 1 
       # assert len(request_worker_pairs) == 1
       # assert request_worker_pairs[0][1] == new_request_id



def test_new_request_no_idle_workers_waiting_requests_above_threshold_max_workers_reached(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        
        waiting_requests = [new_request(n+4, TCRequestStatus.waiting) for n in range(9)]
        active_requests = [new_request(1, TCRequestStatus.transcoding), new_request(2, TCRequestStatus.transcoding), new_request(3, TCRequestStatus.transcoding)]
        running_workers = [new_worker(1, TCWorkerStatus.busy), new_worker(2, TCWorkerStatus.busy), new_worker(3, TCWorkerStatus.busy)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_queue.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 0 
       # assert len(request_worker_pairs) == 0


def test_new_request_no_idle_workers_waiting_requests_below_threshold(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        
        waiting_requests = [new_request(n+2, TCRequestStatus.waiting) for n in range(7)]
        active_requests = [new_request(1, TCRequestStatus.transcoding)]
        running_workers = [new_worker(1, TCWorkerStatus.busy)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_queue.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 0 
       # assert len(request_worker_pairs) == 0






""" 

worker available

"""


def test_worker_available_waiting_requests(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        
        waiting_requests = [new_request(1, TCRequestStatus.waiting)]
        active_requests = []
        worker = new_worker(1, TCWorkerStatus.idle)
        running_workers = [worker]
        worker_id = worker.worker_id

        workers_to_delete, request_worker_pairs = scheduler_with_queue.worker_available(
            worker_id, waiting_requests, active_requests, running_workers
        )

        assert len(workers_to_delete) == 0
        # assert request_worker_pairs == [(worker_id, waiting_requests[0].request_id)]



def test_worker_available_no_waiting_requests(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        
        waiting_requests = []
        active_requests = []
        worker = new_worker(1, TCWorkerStatus.idle)
        running_workers = [worker]
        worker_id = worker.worker_id

        workers_to_delete, request_worker_pairs = scheduler_with_queue.worker_available(
            worker_id, waiting_requests, active_requests, running_workers
        )

        assert len(workers_to_delete) == 1
        # assert len(request_worker_pairs) == 0





""" 

periodic update

"""




def test_periodic_update_waiting_requests_idle_workers(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = [new_request(1, TCRequestStatus.waiting)]
    active_requests = []
    running_workers = [new_worker(1, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_queue.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 0
    assert len(workers_to_delete) == 0
    # assert len(request_worker_pairs) == 1




def test_periodic_update_waiting_requests_still_above_threshold_max_workers_not_reached(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = [new_request(n+1, TCRequestStatus.waiting) for n in range(10)]
    active_requests = []
    running_workers = [new_worker(1, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_queue.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 1
    assert len(workers_to_delete) == 0
    # assert len(request_worker_pairs) == 2



def test_periodic_update_no_waiting_requests_idle_workers(
    scheduler_with_queue: SchedulerWithQueue, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = []
    active_requests = []
    running_workers = [new_worker(1, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_queue.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 0
    assert len(workers_to_delete) == 1
    # assert len(request_worker_pairs) == 0