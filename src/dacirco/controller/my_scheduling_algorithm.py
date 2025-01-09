"""Dummy algorithm.

This algorithm is a dummy algorithm that does not perform any scheduling. You can copy this file and use it as a starting point for creating your own scheduling algorithm.

warning:
    Do Not Use This Algorithm.  It does nothing!

warning:
    Make sure to change the class name and the file name to match your new algorithm.

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


class MySchedulingAlgorithm:
    """
    Empty Scheduling algorithm.
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
        Handles a new request by determining if to create new workers and to which worker assign the new request.

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
        raise SystemExit("Do not use this algorithm!")
        return new_workers, request_worker_pairs

    def worker_available(
        self,
        worker_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[UUID], list[tuple[UUID, UUID]]]:
        """
        Determines the availability of a worker and assigns waiting requests to the worker if available.

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
        return workers_to_delete, request_worker_pairs

    def periodic_update(
        self,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[str], list[UUID], list[tuple[UUID, UUID]]]:
        """
        The controller calls this method periodically to determine whether to stop and remove workers, or to create new ones, and which requests to assign to which worker.

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
        return new_workers, workers_to_delete, request_worker_pairs
