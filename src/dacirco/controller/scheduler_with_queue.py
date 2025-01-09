"""
"Scheduler with queue" scheduling algorithm.

The "Scheduler with Queue" adds new workers only when the number of waiting requests exceeds a defined threshold (`queue_threshold`).
It aims to maintain a balanced system by :
    scaling workers based on the queue size,
    removing idle workers when the queue is empty,
    and keeping only the minimal necessary resources active to handle demand efficiently.

Attributes:
    _sched_params (SchedulerParameters): Scheduler parameters.
    _pod_params (dict[str, PodParameters]): Pod parameters.
    _vm_params (dict[str, VMParameters]): VM parameters.

Methods:
    new_request:
        Assigns new requests to an idle worker if available.
        If not, adds workers when waiting_requests exceeds queue_threshold.
    worker_available:
        Assigns a waiting request to the worker, if any exist.
        Removes the worker if there are no requests.
    periodic_update:
        Periodically evaluates the state and scales workers based on queue_threshold.
"""

import logging
from uuid import UUID
# from uuid import uuid4

from dacirco.dataclasses import (
    PodParameters,
    SchedulerParameters,
    TCRequest,
    TCWorker,
    VMParameters,
)

_logger = logging.getLogger("dacirco.scheduler_with_queue")


class SchedulerWithQueue:
    def __init__(
        self,
        sched_params: SchedulerParameters,
        pod_params: dict[str, PodParameters],
        vm_params: dict[str, VMParameters],
    ) -> None:
        self._sched_params = sched_params
        self._pod_params = pod_params
        self._vm_params = vm_params
        self._queue_threshold = 9

    def new_request(
        self,
        new_request_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
        # queue_threshold: int,
    ) -> tuple[list[str], list[tuple[UUID, UUID]]]:
        """

        Assigns new requests to idle workers if available.
        If not, adds workers when waiting_requests exceeds queue_threshold.

        Args:
            new_request_id (UUID): The unique identifier for the new request.
            waiting_requests (list[TCRequest]): Requests waiting to be processed.
            active_requests (list[TCRequest]): Requests currently being processed.
            running_workers (list[TCWorker]): Workers currently running.
            queue_threshold (int): The queue size threshold for adding new workers.

        Returns:
            tuple: A list of new workers to be created and a list of request-worker assignments.
        """
        _logger.debug(f"new_request: {new_request_id}, queue size: {len(waiting_requests)}")

        new_workers: list[str] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        idle_workers = [worker for worker in running_workers if worker.status == "idle"]

        # Assign the new request to an idle worker if available
        if idle_workers:
            request_worker_pairs.append((idle_workers[0].worker_id, new_request_id))

        else:
            # Check if the queue exceeds the threshold
            if len(waiting_requests) + 1 > self._queue_threshold and len(running_workers) < self._sched_params.max_workers:
                new_workers.append("default")
              #  new_worker_id = uuid4()
              #  request_worker_pairs.append((new_worker_id, new_request_id))
            else:
                # Let the request stay in the queue if no workers are available
                _logger.debug(f"Request {new_request_id} remains in the queue.")

        return new_workers, request_worker_pairs





    def worker_available(
        self,
        worker_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
        # queue_threshold: int,
    ) -> tuple[list[UUID], list[tuple[UUID, UUID]]]:
        """

       Assigns a waiting request to the worker, if any exist.
       Removes the worker if there are no requests.

        Args:
            worker_id (UUID): The unique identifier of the available worker.
            waiting_requests (list[TCRequest]): Requests waiting to be processed.
            active_requests (list[TCRequest]): Requests currently being processed.
            running_workers (list[TCWorker]): Workers currently running.
            queue_threshold (int): The queue size threshold for adding workers.

        Returns:
            tuple: A list of workers to be deleted and a list of request-worker assignments.
        """
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []


        # Assign waiting requests to the available worker
        if waiting_requests:
            request_worker_pairs.append((worker_id,waiting_requests[0].request_id))
        else:
            # Remove the worker if there are no waiting requests
            workers_to_delete.append(worker_id)
            _logger.debug(f"Marked worker {worker_id} for removal.")

        return workers_to_delete, request_worker_pairs





    def periodic_update(
        self,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
        # queue_threshold: int,
    ) -> tuple[list[str], list[UUID], list[tuple[UUID, UUID]]]:

        """
        Periodically evaluates the state and scales workers based on queue_threshold.

        Args:
            waiting_requests (list[TCRequest]): Requests waiting to be processed.
            active_requests (list[TCRequest]): Requests currently being processed.
            running_workers (list[TCWorker]): Workers currently running.
            queue_threshold (int): The queue size threshold for adding new workers.

        Returns:
            tuple: A list of new worker IDs to create, a list of worker IDs to delete, 
                   and a list of request-worker assignments.
        """

        new_workers: list[str] = []
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        idle_workers = [worker for worker in running_workers if worker.status == "idle"]




        if waiting_requests:
            if not running_workers:
                new_workers.append("default")

            if idle_workers:
                for request, worker in zip(waiting_requests, idle_workers):
                    request_worker_pairs.append((worker.worker_id, request.request_id))

            if len(waiting_requests) - len(request_worker_pairs) + 1 > self._queue_threshold and len(running_workers) < self._sched_params.max_workers:
                    # new_worker_id = uuid4()
                new_workers.append("default")
                   #  request_worker_pairs.append((new_worker_id,waiting_requests[0]))

        else:
            if idle_workers:
                workers_to_delete.extend([worker.worker_id for worker in idle_workers])
                _logger.debug(f"Marked idle workers for removal: {[worker.worker_id for worker in idle_workers]}")




        return new_workers, workers_to_delete, request_worker_pairs
