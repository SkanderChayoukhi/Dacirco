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

_logger = logging.getLogger("dacirco.adaptive_grace_period")


class AdaptiveGracePeriod:
    """
    The AdaptiveGracePeriod implementation of the Scheduler protocol.
    Dynamically adjusts the grace period for idle workers based on workload trends.
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
        self._grace_period = sched_params.max_idle_time  # Start with max_idle_time
        self._recent_request_rate = 0  # Requests per second
        self._utilization_threshold = 0.7  # Threshold for high utilization
        self._last_update_time = datetime.now()  # Track last periodic update

    def new_request(
        self,
        new_request_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[str], list[tuple[UUID, UUID]]]:
        """
        Handles a new request. Creates a new worker if the number of running workers
        is less than the maximum allowed.
        """
        _logger.debug(f"new_request: waiting_requests: {waiting_requests}")
        _logger.debug(f"new_request: active_requests: {active_requests}")
        _logger.debug(f"new_request: running_workers: {running_workers}")
        new_workers: list[str] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        # Add a new worker if needed
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
        Assigns the first waiting request to the available worker if possible.
        Otherwise, keeps the worker idle.
        """
        _logger.debug(f"worker_available: waiting_requests: {waiting_requests}")
        _logger.debug(f"worker_available: active_requests: {active_requests}")
        _logger.debug(f"worker_available: running_workers: {running_workers}")
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        if len(waiting_requests) > 0:
            # Assign the oldest request to the available worker
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
        Periodically checks the status of running workers, adjusts the grace period,
        and decides whether to create or remove workers.
        """
        new_workers: list[str] = []
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        current_time = datetime.now()
        elapsed_time = (current_time - self._last_update_time).total_seconds()

        # Adjust grace period and monitor workload
        if elapsed_time > 1:  # Update logic every second
            self._recent_request_rate = len(waiting_requests) / elapsed_time
            worker_utilization = len(active_requests) / max(1, len(running_workers))

            # Adjust grace period based on utilization and request rate
            if worker_utilization > self._utilization_threshold or self._recent_request_rate > 1:
                self._grace_period = min(self._grace_period + 5, 2 * self._sched_params.max_idle_time)
            elif worker_utilization < self._utilization_threshold and self._recent_request_rate < 0.1:
                self._grace_period = max(self._grace_period - 5, self._sched_params.max_idle_time // 2)

            # Update the last time we ran the periodic update logic
            self._last_update_time = current_time

        # Check idle workers for removal
        for worker in running_workers:
            if worker.status == "idle" and worker.t_idle_start:
                idle_time = (current_time - worker.t_idle_start).total_seconds()
                _logger.debug(
                    f"worker {worker.worker_id} has been idle for {idle_time} seconds"
                )
                if idle_time > self._grace_period:
                    workers_to_delete.append(worker.worker_id)

        # Proactively create new workers if workload demands
        if self._recent_request_rate > 1 and len(waiting_requests) > len(running_workers) and len(running_workers) < self._sched_params.max_workers:
            new_workers.append("default")

        _logger.debug(f"periodic_update: new_workers: {new_workers}")
        _logger.debug(f"periodic_update: workers_to_delete: {workers_to_delete}")
        _logger.debug(f"periodic_update: request_worker_pairs: {request_worker_pairs}")
        return new_workers, workers_to_delete, request_worker_pairs
