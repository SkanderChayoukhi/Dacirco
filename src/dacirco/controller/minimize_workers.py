"""MinimizeWorkers scheduling algorithm.

The MinimizeWorkers algorithm creates new workers when there are waiting requests and the number of running workers is less than the maximum number of workers. When a worker becomes idle, the algorithm immediately removes it, unless there are waiting requests.  In that case, it assigns the oldest waiting request to the idle worker.


Attributes:
    _sched_params (SchedulerParameters): Scheduler parameters.
    _pod_params (dict[str, PodParameters]): Pod parameters.
    _vm_params (dict[str, VMParameters]): VM parameters.

Methods:
    new_request:
        Determines whether to create new workers and to which worker assign a new request.

    worker_available:
        Determines whether to stop and remove workers and which requests to assign to which worker whenever a worker becomes available.

    periodic_update:
        The controller calls this method periodically to determines whether to stop and remove workers, or to create new ones, and which requests to assign to which worker.
"""

import logging
from uuid import UUID

from dacirco.dataclasses import (
    PodParameters,
    SchedulerParameters,
    TCRequest,
    TCWorker,
    VMParameters,
)

_logger = logging.getLogger("dacirco.minimize_workers")


class MinimizeWorkers:
    """
    The MinimizeWorkers implementation of the Scheduler protocol.
    """

    def __init__(
        self,
        sched_params: SchedulerParameters,
        pod_params: dict[str, PodParameters],
        vm_params: dict[str, VMParameters],
    ) -> None:
        self._sched_params = sched_params
        self._pod_params = pod_params
        self._vm_params = vm_params

    def new_request(
        self,
        new_request_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[str], list[tuple[UUID, UUID]]]:
        """
        If the number of running workers is less than the maximum number of workers, the algorithm creates a new worker.  Otherwise, it does nothing, and the new request remains keeps waiting.


        Args:
            new_request_id (UUID): The unique identifier for the new request.
            waiting_requests (list[TCRequest]): A list of requests that are waiting to be processed (including the new request).
            active_requests (list[TCRequest]): A list of requests that are currently being processed.
            running_workers (list[TCWorker]): A list of workers that are currently running.

        Returns:
            A tuple containing:

                - A list of new workers to be added.
                - An empty list of request-worker pairs.
        """
        _logger.debug(f"new_request: waiting_requests: {waiting_requests}")
        _logger.debug(f"new_request: active_requests: {active_requests}")
        _logger.debug(f"new_request: running_workers: {running_workers}")
        new_workers: list[str] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []
        if waiting_requests and len(running_workers) < self._sched_params.max_workers:
            new_workers.append("default")
        _logger.debug(f"new_request: new workers: {new_workers}")
        _logger.debug(f"new_request: request_worker_pairs: {request_worker_pairs}")
        return new_workers, request_worker_pairs

    def worker_available(
        self,
        worker_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[UUID], list[tuple[UUID, UUID]]]:
        """The [DacircoController][dacirco.controller.controller.DaCircoController] calls this method when a worker becomes available.

        If there are no waiting requests, the algorithm removes the idle worker.  Otherwise, it assigns the oldest waiting request to the idle worker.

        Args:
            worker_id (UUID): The unique identifier of the worker that became available.
            waiting_requests (list[TCRequest]): A list of requests that are waiting to be processed.
            active_requests (list[TCRequest]): A list of requests that are currently being processed.
            running_workers (list[TCWorker]): A list of workers that are currently running.

        Returns:
            A tuple containing:

                - A list of workers to be removed (if there are no waiting requests).
                - A list of request-worker pairs (if there is at least one waiting request).

        """
        _logger.debug(f"worker_available: waiting_requests: {waiting_requests}")
        _logger.debug(f"worker_available: active_requests: {active_requests}")
        _logger.debug(f"worker_available: running_workers: {running_workers}")
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []
        if len(waiting_requests) == 0:
            workers_to_delete.append(worker_id)
        else:
            request_worker_pairs.append((worker_id, waiting_requests[0].request_id))
        _logger.debug(f"worker_available: workers_to_delete: {workers_to_delete}")
        _logger.debug(f"worker_available: request_worker_pairs: {request_worker_pairs}")
        return workers_to_delete, request_worker_pairs

    def periodic_update(
        self,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[str], list[UUID], list[tuple[UUID, UUID]]]:
        """
        MinimizeWorkers does no implement the periodic_update method.  It always returns empty lists.

        Args:
            waiting_requests (list[TCRequest]): A list of requests that are waiting to be processed.
            active_requests (list[TCRequest]): A list of requests that are currently being processed.
            running_workers (list[TCWorker]): A list of workers that are currently running.

        Returns:
            A tuple containing:

                - An empty list of new workers to be created.
                - An empty list of workers to be removed.
                - An empty list of request-worker pairs.
        """
        new_workers: list[str] = []
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []
        return new_workers, workers_to_delete, request_worker_pairs
