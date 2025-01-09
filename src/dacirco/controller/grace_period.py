"""GracePeriod scheduling algorithm.

Similar to the [`MinimizeWorkers`][dacirco.controller.minimize_workers.MinimizeWorkers] algorithm, the GracePeriod algorithm creates new workers when there are waiting requests and the number of running workers is less than the maximum number of workers. The difference is that GracePeriod does not immediately remove an idle worker when there are no waiting requests.  Rather, it waits for a predefined time (the "grace period") before destroying an idle worker.  This allows the system to keep workers alive for a certain amount of time after they become idle, in case new requests arrive.

It exploits the `periodic_update` method to check the idle time of each worker.  If a worker has been idle for more than the grace period, the algorithm removes the worker.

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
from datetime import datetime
from uuid import UUID

from dacirco.dataclasses import (
    PodParameters,
    SchedulerParameters,
    TCRequest,
    TCWorker,
    VMParameters,
)

_logger = logging.getLogger("dacirco.minimize_workers")


class GracePeriod:
    """
    The GracePeriod implementation of the Scheduler protocol.
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
        Handles a new request.

        If the number of workers is less than the maximum number of workers, the algorithm creates a new worker.  Otherwise, it does nothing (i.e., the new request will have to wait for an existing worker to become available).

        Args:
            new_request_id (UUID): The unique identifier for the new request.
            waiting_requests (list[TCRequest]): A list of requests that are waiting to be processed (including the new request).
            active_requests (list[TCRequest]): A list of requests that are currently being processed.
            running_workers (list[TCWorker]): A list of workers that are currently running.

        Returns:
            A tuple containing:
                - A list of new workers to be added.
                - A list of tuples pairing request IDs with worker IDs.
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
        """
        If there is at least one waiting request, assigns the first waiting request to the worker.  Otherwise, does nothing (i.e., the worker remains idle).

        Args:
            worker_id (UUID): The unique identifier of the worker.
            waiting_requests (list[TCRequest]): A list of requests that are waiting to be processed.
            active_requests (list[TCRequest]): A list of requests that are currently being processed.
            running_workers (list[TCWorker]): A list of workers that are currently running.

        Returns:
            A tuple containing two lists:

                - A list of worker UUIDs that should be deleted.
                - A list of tuples where each tuple contains a worker UUID and a request UUID, representing the assignment of a request to a worker.
        """
        _logger.debug(f"worker_available: waiting_requests: {waiting_requests}")
        _logger.debug(f"worker_available: active_requests: {active_requests}")
        _logger.debug(f"worker_available: running_workers: {running_workers}")
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []
        if len(waiting_requests) > 0:
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
        The controller calls this method periodically to determine whether to stop and remove workers, or to create new ones, and which requests to assign to which worker.

        This method checks the status of running workers and determines if any workers have been idle for longer than the maximum allowed idle time. If so, it marks these workers for deletion.

        Args:
            waiting_requests (list[TCRequest]): A list of requests that are waiting to be processed.
            active_requests (list[TCRequest]): A list of requests that are currently being processed.
            running_workers (list[TCWorker]): A list of workers that are currently running.

        Returns:
            A tuple containing:

                - A list of new workers to create (always empty in this algorithm).
                - A list of worker IDs to be deleted.
                - A list of tuples pairing request IDs with worker IDs (always empty in this algorithm).
        """
        new_workers: list[str] = []
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []
        for worker in running_workers:
            if worker.status == "idle" and worker.t_idle_start:
                idle_time = (datetime.now() - worker.t_idle_start).total_seconds()
                _logger.debug(
                    f"worker {worker.worker_id} has been idle for {idle_time} seconds"
                )
                if idle_time > self._sched_params.max_idle_time:
                    workers_to_delete.append(worker.worker_id)
        return new_workers, workers_to_delete, request_worker_pairs
