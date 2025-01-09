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

_logger = logging.getLogger("dacirco.priority_based_dynamic")


class PriorityBasedDynamic:
    """
    A refined scheduling algorithm that prioritizes requests based on metadata,
    adapts worker scaling dynamically, and ensures fairness through priority aging.
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
        self._grace_period = sched_params.max_idle_time
        self._last_update_time = datetime.now()
        self._priority_aging_factor = 10  # Increment priority every 10 seconds

    def _calculate_priority(self, request: TCRequest, current_time: datetime) -> int:
        """
        Calculate priority for a given request based on metadata and aging.
        """

        # If file_size is None, treat it as 0 or any other appropriate default value

        file_size = request.file_size if request.file_size is not None else 0
        size_factor = file_size / 1_000_000  # Normalize size in MB
        speed_factor = 2 if request.speed == "ultrafast" else 1
        bitrate_factor = request.bitrate / 1_000  # Normalize bitrate in kbps
        waiting_time = (current_time - request.t_received).total_seconds()
        aging_bonus = int(waiting_time / self._priority_aging_factor)

        return int(size_factor * 0.5 + bitrate_factor * 0.3 + speed_factor * 0.2 + aging_bonus)

    def new_request(
        self,
        new_request_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[str], list[tuple[UUID, UUID]]]:
        """
        Handles a new request by assigning it to a worker or creating new workers if necessary.
        """
        _logger.debug(f"new_request: waiting_requests: {waiting_requests}")
        new_workers: list[str] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        # Sort requests by priority
        current_time = datetime.now()
        prioritized_requests = sorted(
            waiting_requests, key=lambda req: self._calculate_priority(req, current_time), reverse=True
        )

        # Check if new workers are needed
        if prioritized_requests and len(running_workers) < self._sched_params.max_workers:
            profile = "large" if prioritized_requests[0].bitrate > 5000 else "small"
            new_workers.append(profile)

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
        Assigns the highest-priority request to an available worker.
        """
        _logger.debug(f"worker_available: waiting_requests: {waiting_requests}")
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        if waiting_requests:
            # Sort requests by priority and assign the highest-priority request
            current_time = datetime.now()
            prioritized_requests = sorted(
                waiting_requests, key=lambda req: self._calculate_priority(req, current_time), reverse=True
            )
            request_worker_pairs.append((worker_id, prioritized_requests[0].request_id))

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
        Periodically checks worker status, scales dynamically, and removes idle workers.
        """
        new_workers: list[str] = []
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        current_time = datetime.now()
        elapsed_time = (current_time - self._last_update_time).total_seconds()

        if elapsed_time > 0.0001:
            # Monitor worker utilization
              worker_utilization = len(active_requests) / max(1, len(running_workers))

            # Scale workers dynamically based on workload
              if len(waiting_requests) > len(running_workers) and worker_utilization > 0.8 and len(running_workers) < self._sched_params.max_workers:
                profile = "large" if len(waiting_requests) > 5 else "small"
                new_workers.append(profile)

            # Update the last time periodic logic was run
                self._last_update_time = current_time

        # Remove idle workers exceeding grace period
        for worker in running_workers:
            if worker.status == "idle" and worker.t_idle_start:
                idle_time = (current_time - worker.t_idle_start).total_seconds()
                if idle_time > self._grace_period:
                    workers_to_delete.append(worker.worker_id)

        _logger.debug(f"periodic_update: new_workers: {new_workers}")
        _logger.debug(f"periodic_update: workers_to_delete: {workers_to_delete}")
        _logger.debug(f"periodic_update: request_worker_pairs: {request_worker_pairs}")
        return new_workers, workers_to_delete, request_worker_pairs
