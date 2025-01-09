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
from dacirco.controller.scheduler_with_reserve import SchedulerWithReserve
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
        algorithm=AlgorithmName.scheduler_with_reserve,
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
def scheduler_with_reserve(
    params: tuple[
        SchedulerParameters, dict[str, PodParameters], dict[str, VMParameters]
    ],
) -> SchedulerWithReserve:
    
    return SchedulerWithReserve(*params)


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



def test_new_request_no_idle_workers_max_is_more_than_one_worker_away(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """Test case: No idle workers, max is more than one worker away."""
        waiting_requests = [new_request(2, TCRequestStatus.waiting)]
        active_requests = [new_request(1, TCRequestStatus.transcoding)]
        running_workers = [new_worker(n+1, TCWorkerStatus.busy) for n in range(1)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_reserve.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 2 # One for the request, one reserve
        # assert len(request_worker_pairs) == 1
        # assert request_worker_pairs[0][1] == new_request_id


def test_new_request_no_idle_workers_max_is_one_worker_away(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """Test case: No idle workers, max is one worker away."""
        waiting_requests = [new_request(3, TCRequestStatus.waiting)]
        active_requests = [new_request(n+1, TCRequestStatus.transcoding) for n in range(2)]
        running_workers = [new_worker(n+1, TCWorkerStatus.busy) for n in range(2)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_reserve.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 1 
       # assert len(request_worker_pairs) == 1
       # assert request_worker_pairs[0][1] == new_request_id




def test_new_request_no_idle_workers_max_reached(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """Test case: No idle workers, max workers reached."""
        waiting_requests = [new_request(4, TCRequestStatus.waiting)]
        active_requests = [new_request(n+1, TCRequestStatus.transcoding) for n in range(3)]
        running_workers = [new_worker(n+1, TCWorkerStatus.busy) for n in range(3)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_reserve.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 0
       # assert len(request_worker_pairs) == 0



def test_new_request_one_idle_worker_max_not_reached(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """Test case: One idle worker, max not reached ."""
        waiting_requests = [new_request(2, TCRequestStatus.waiting)]
        active_requests = [new_request(1, TCRequestStatus.transcoding)]
        running_workers = [new_worker(1, TCWorkerStatus.busy), new_worker(2, TCWorkerStatus.busy)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_reserve.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 1
      #  assert len(request_worker_pairs) == 1
       # assert request_worker_pairs[0][1] == new_request_id


def test_new_request_one_idle_worker_max_reached(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """Test case: One idle worker, max reached ."""
        waiting_requests = [new_request(3, TCRequestStatus.waiting)]
        active_requests = [new_request(n+1, TCRequestStatus.transcoding) for n in range(2)]
        running_workers = [new_worker(n+1, TCWorkerStatus.busy) for n in range(2)] + [new_worker(3, TCWorkerStatus.idle)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_reserve.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 0
     #   assert len(request_worker_pairs) == 1
     #   assert request_worker_pairs[0][1] == new_request_id



def test_new_request_more_than_one_idle_worker(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):
        """Test case: More than one idle worker."""
        waiting_requests = [new_request(2, TCRequestStatus.waiting)]
        active_requests = [new_request(1, TCRequestStatus.transcoding)]
        running_workers = [new_worker(n+1, TCWorkerStatus.idle) for n in range(3)]
        new_request_id = waiting_requests[0].request_id

        new_workers, request_worker_pairs = scheduler_with_reserve.new_request(
            new_request_id, waiting_requests, active_requests, running_workers
        )
        assert len(new_workers) == 0
      #  assert len(request_worker_pairs) == 1
     #   assert request_worker_pairs[0][1] == new_request_id



""" 

worker available

"""



def test_worker_available_no_other_idle_worker_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = [new_request(1, TCRequestStatus.waiting)]
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id

    workers_to_delete, request_worker_pairs = scheduler_with_reserve.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    )

    assert len(workers_to_delete) == 0
  #  assert request_worker_pairs == [(worker_id, waiting_requests[0].request_id)]



def test_worker_available_no_other_idle_worker_no_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = []
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker]
    worker_id = worker.worker_id


    workers_to_delete, request_worker_pairs = scheduler_with_reserve.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    )

    assert len(workers_to_delete) == 0
  #  assert request_worker_pairs == []




def test_worker_available_other_idle_workers_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):

    waiting_requests = [new_request(1, TCRequestStatus.waiting)]
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker,new_worker(2, TCWorkerStatus.idle)]
    worker_id = worker.worker_id

    workers_to_delete, request_worker_pairs = scheduler_with_reserve.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    )

    assert len(workers_to_delete) == 0
   # assert request_worker_pairs == [(worker_id, waiting_requests[0].request_id)]


def test_worker_available_other_idle_workers_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = []
    active_requests = []
    worker = new_worker(1, TCWorkerStatus.idle)
    running_workers = [worker,new_worker(2, TCWorkerStatus.idle)]
    worker_id = worker.worker_id

    workers_to_delete, request_worker_pairs = scheduler_with_reserve.worker_available(
        worker_id, waiting_requests, active_requests, running_workers
    )

    assert len(workers_to_delete) == 1
 #   assert request_worker_pairs == []


""" 

periodic update

"""



def test_periodic_update_no_idle_workers_max_workers_not_reached_waiting_requests_max_far(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = [new_request(2, TCRequestStatus.waiting)]
    active_requests = [new_request(1, TCRequestStatus.transcoding)]
    running_workers = [new_worker(1, TCWorkerStatus.busy)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 2
    assert len(workers_to_delete) == 0
  #  assert len(request_worker_pairs) == 1





def test_periodic_update_no_idle_workers_max_workers_not_reached_waiting_requests_max_near(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = [new_request(3, TCRequestStatus.waiting)]
    active_requests = [new_request(n+1, TCRequestStatus.transcoding) for n in range(2)]
    running_workers = [new_worker(n+1, TCWorkerStatus.busy) for n in range(2)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 1
    assert len(workers_to_delete) == 0
  #  assert len(request_worker_pairs) == 1




def test_periodic_update_no_idle_workers_max_workers_not_reached_no_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],):

    waiting_requests = []
    active_requests = [new_request(n+1, TCRequestStatus.transcoding) for n in range(2)]
    running_workers = [new_worker(n+1, TCWorkerStatus.busy) for n in range(2)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 1
    assert len(workers_to_delete) == 0
 #   assert request_worker_pairs == []



def test_periodic_update_no_idle_workers_max_workers_reached(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):

    waiting_requests = []
    active_requests = [new_request(n+1, TCRequestStatus.transcoding) for n in range(3)]
    running_workers = [new_worker(n+1, TCWorkerStatus.busy) for n in range(3)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 0
    assert len(workers_to_delete) == 0
  #  assert request_worker_pairs == []







def test_periodic_update_one_idle_workers_waiting_requests_max_workers_not_reached(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):


    waiting_requests = [new_request(2, TCRequestStatus.waiting)]
    active_requests = [new_request(1, TCRequestStatus.transcoding)]
    running_workers = [new_worker(1, TCWorkerStatus.busy),new_worker(2, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 1
    assert len(workers_to_delete) == 0
  #  assert request_worker_pairs == [(running_workers[1].worker_id, waiting_requests[0].request_id)]




def test_periodic_update_one_idle_workers_waiting_requests_max_workers_reached(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = [new_request(3, TCRequestStatus.waiting)]
    active_requests = [new_request(1, TCRequestStatus.transcoding),new_request(2, TCRequestStatus.transcoding)]
    running_workers = [new_worker(1, TCWorkerStatus.busy),new_worker(2, TCWorkerStatus.busy),new_worker(3, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 0
    assert len(workers_to_delete) == 0
  #  assert request_worker_pairs == [(running_workers[2].worker_id, waiting_requests[0].request_id)]


def test_periodic_update_one_idle_workers_no_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
):

    waiting_requests = []
    active_requests = [new_request(1, TCRequestStatus.transcoding)]
    running_workers = [new_worker(1, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 0
    assert len(workers_to_delete) == 0
  #  assert request_worker_pairs == []







def test_periodic_update_many_idle_workers_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = [new_request(1, TCRequestStatus.waiting)]
    active_requests = []
    running_workers = [new_worker(1, TCWorkerStatus.idle),new_worker(2, TCWorkerStatus.idle),new_worker(3, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 0
    assert len(workers_to_delete) == 1
  #  assert request_worker_pairs == [(running_workers[0].worker_id, waiting_requests[0].request_id)]



def test_periodic_update_many_idle_workers_no_waiting_requests(
    scheduler_with_reserve: SchedulerWithReserve, 
    new_request: Callable[[int, TCRequestStatus], TCRequest],
    new_worker: Callable[[int, TCWorkerStatus], TCWorker],
    ):

    waiting_requests = []
    active_requests = []
    running_workers = [new_worker(1, TCWorkerStatus.idle),new_worker(2, TCWorkerStatus.idle),new_worker(3, TCWorkerStatus.idle)]

    new_workers, workers_to_delete, request_worker_pairs = scheduler_with_reserve.periodic_update(
        waiting_requests, active_requests, running_workers
    )

    assert len(new_workers) == 0
    assert len(workers_to_delete) == 2
  #  assert request_worker_pairs == []


