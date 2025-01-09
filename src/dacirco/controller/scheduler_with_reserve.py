"""
"Scheduler with reserve" scheduling algorithm.

The "Scheduler with reserve" ensures there is always at least one worker available as a reserve.
It creates new workers when demand exceeds capacity and removes idle workers if there is
more than one idle and no waiting requests.

Attributes:
    _sched_params (SchedulerParameters): Scheduler parameters.
    _pod_params (dict[str, PodParameters]): Pod parameters.
    _vm_params (dict[str, VMParameters]): VM parameters.

Methods:
    new_request:
        Handles a new request by determining whether to create new workers and to which worker assign the new request.
        Ensures there is always at least one reserve idle worker, if max workers not reached.
    worker_available:
        Assigns waiting requests to the available worker or keeps it idle as the reserve. 
        Removes workers if there are excess idle workers and no waiting requests.
    periodic_update:
        Periodically updates the system state.
        Ensures there is always one reserve worker, assigns waiting requests, 
        and removes excess workers if necessary.
"""

import logging
from uuid import UUID
from uuid import uuid4

from dacirco.dataclasses import (
    PodParameters,
    SchedulerParameters,
    TCRequest,
    TCWorker,
    VMParameters,
)

_logger = logging.getLogger("dacirco.scheduler_with_reserve")


class SchedulerWithReserve:


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
        Handles a new request by determining whether to create new workers and to which worker assign the new request.
        Ensures there is always at least one reserve idle worker, if max workers not reached.

        Args:
            new_request_id (UUID): The unique identifier for the new request.
            waiting_requests (list[TCRequest]): A list of requests waiting to be processed.
            active_requests (list[TCRequest]): A list of requests currently being processed.
            running_workers (list[TCWorker]): A list of workers currently running.

        Returns:
            tuple: A list of new workers to be created and a list of request-worker assignments.
        """
        _logger.debug(f"new_request: waiting_requests: {waiting_requests}")
        _logger.debug(f"new_request: active_requests: {active_requests}")
        _logger.debug(f"new_request: running_workers: {running_workers}")

        new_workers: list[str] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        # Identify idle workers
        idle_workers = [worker for worker in running_workers if worker.status == "idle"]

        if len(idle_workers) == 0:
            # No idle workers: Create 2 workers, one reserve and one for the new request

            if len(running_workers) < self._sched_params.max_workers:
                worker_for_request_id = uuid4()
                new_workers.append("default")
           #     request_worker_pairs.append((worker_for_request_id, new_request_id))
            

                if len(running_workers) + len(new_workers) < self._sched_params.max_workers:
                    reserve_worker_id = uuid4()
                    new_workers.append("default")

            
        elif len(idle_workers) == 1:
            # One idle worker: Assign it to the new request and create a reserve worker if possible
        #    request_worker_pairs.append((idle_workers[0].worker_id, new_request_id))
            
            if len(running_workers) < self._sched_params.max_workers:
                reserve_worker_id = uuid4()
                new_workers.append("default")

      #   else:
            # More than one idle worker: Use the first idle worker (id = 0) for the request
         #   request_worker_pairs.append((idle_workers[0].worker_id, new_request_id))

        return new_workers, request_worker_pairs


    def worker_available(
        self,
        worker_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[UUID], list[tuple[UUID, UUID]]]:
        """

        Assigns waiting requests to the available worker or keeps it idle as the reserve. 
        Removes workers if there are excess idle workers and no waiting requests. 

        Args:
            worker_id (UUID): The unique identifier of the worker.
            waiting_requests (list[TCRequest]): A list of requests that are waiting to be processed.
            active_requests (list[TCRequest]): A list of requests that are currently being processed.
            running_workers (list[TCWorker]): A list of workers that are currently running.

        Returns:
            A tuple containing:
                - A list of worker UUIDs to be deleted.
                - A list of tuples pairing request IDs with worker IDs.
        """
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        # Identify idle workers excluding the currently available worker
        other_idle_workers = [worker for worker in running_workers if worker.status == "idle" and worker.worker_id != worker_id]

        if len(other_idle_workers) == 0:
            if len(waiting_requests) > 0:
                
               request_worker_pairs.append((worker_id, waiting_requests[0].request_id))

                # if len(running_workers) < self._sched_params.max_workers:
                #     new_worker_id = f"worker-{len(running_workers) + 1}"
                #     new_workers.append(new_worker_id)
                #     _logger.debug(f"created new reserve worker: {new_worker_id}")
 
            else:
                _logger.debug(f"keeping worker {worker_id} as reserve.")
        else:
            if len(waiting_requests) > 0:
               request_worker_pairs.append((worker_id, waiting_requests[0].request_id))
            else:
                workers_to_delete.append(worker_id)
            

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
        Periodically updates the system state.

        Ensures there is always one reserve worker, assigns waiting requests, 
        and removes excess workers if necessary.

        Args:
            waiting_requests (list[TCRequest]): A list of requests waiting to be processed.
            active_requests (list[TCRequest]): A list of requests currently being processed.
            running_workers (list[TCWorker]): A list of currently running workers.

        Returns:
            A tuple containing:
                - A list of new worker IDs to create.
                - A list of worker IDs to delete.
                - A list of tuples pairing request IDs with worker IDs.
        """
        new_workers: list[str] = []
        workers_to_delete: list[UUID] = []
        request_worker_pairs: list[tuple[UUID, UUID]] = []

        # Identify idle workers
        idle_workers = [worker for worker in running_workers if worker.status == "idle"]

        # Case 1: No idle workers
        if len(idle_workers) == 0:

            if len(running_workers) < self._sched_params.max_workers:
                new_worker_id = uuid4()
                new_workers.append("default")
                _logger.debug(f"Creating new reserve worker: {new_worker_id}")

                if waiting_requests:
                   # request_worker_pairs.append((new_worker_id, waiting_requests[0].request_id))

                    if len(running_workers) + len(new_workers) < self._sched_params.max_workers:
                        second_new_worker_id = uuid4()
                        new_workers.append("default")
                        _logger.debug(f"Creating new reserve worker: {second_new_worker_id}")



        # Case 2: One idle worker
        elif len(idle_workers) == 1:

            if waiting_requests :

                # Assign the first waiting request to the single idle worker
                request_worker_pairs.append((idle_workers[0].worker_id, waiting_requests[0].request_id))

                if len(running_workers) < self._sched_params.max_workers:
                    # Create a new reserve worker as a substitute
                    new_worker_id = uuid4()
                    new_workers.append("default")
                    _logger.debug(f"Assigned request to the only idle worker and created new reserve: {new_worker_id}")




        # Case 3: More than one idle worker
        else:
        
            # Remove excess idle workers, keeping only one as reserve

            excess_idle_workers = idle_workers[len(waiting_requests)+1:]
            workers_to_delete.extend([worker.worker_id for worker in excess_idle_workers])
            _logger.debug(f"Marked excess idle workers for deletion: {workers_to_delete}")
            if waiting_requests:
                for request, worker in zip(waiting_requests, idle_workers[:len(waiting_requests)]):
                    request_worker_pairs.append((worker.worker_id, request.request_id))


        _logger.debug(f"periodic_update: new_workers: {new_workers}")
        _logger.debug(f"periodic_update: workers_to_delete: {workers_to_delete}")
        _logger.debug(f"periodic_update: request_worker_pairs: {request_worker_pairs}")

        return new_workers, workers_to_delete, request_worker_pairs
