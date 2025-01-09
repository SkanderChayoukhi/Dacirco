"""Scheduler Protocol Interface

This module defines the `Scheduler` protocol, which outlines the methods and attributes required for implementing a scheduler in the dacirco system.

The `Scheduler` protocol is used to create and manage scheduling requests, worker availability, and periodic updates.

warning:
    All the scheduler implementations must implement this protocol.

warning:
    All the methods in this protocol are "pure functions:" their output depends only on the inputs and they must not have any side effects (e.g., they must not interact with the database directly).  The controller is responsible for interacting with the database.

note:
    You can find two sample implementations of the `Scheduler` protocol in the `dacirco.controller` module: `MinimizeWorkers` and `GracePeriod`.
"""

from typing import Protocol
from uuid import UUID

from dacirco.dataclasses import (
    PodParameters,
    SchedulerParameters,
    TCRequest,
    TCWorker,
    VMParameters,
)


class Scheduler(Protocol):
    """
    Scheduler Protocol

    Attributes:
        _sched_params (SchedulerParameters): Parameters for the scheduler.
        _pod_params (dict[str, PodParameters]): Parameters for the pods.
        _vm_params (dict[str, VMParameters]): Parameters for the virtual machines.

    Methods:
        new_request:
            Determines whether to create new workers and to which worker assign a new request.

        worker_available:
            Determines whether to stop and remove workers and which requests to assign to which worker whenever a worker becomes available.

        periodic_update:
            The controller calls this method periodically to determines whether to stop and remove workers, or to create new ones, and which requests to assign to which worker.
    """

    _sched_params: SchedulerParameters
    _pod_params: dict[str, PodParameters]
    _vm_params: dict[str, VMParameters]

    def new_request(
        self,
        new_request_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[str], list[tuple[UUID, UUID]]]:
        """
        The [DacircoController][dacirco.controller.controller.DaCircoController] calls this method when a new request is received.

        This method must decide:

        - Whether to create new workers.
        - If there any idle workers, to which on assign the new request.

        It must return a tuple containing two lists:

        - A list of strings representing the number of workers to create (the strings are the keys in th `_vm_params` or `_pod_params` dictionary).
        - A list of tuples, each containing two UUIDs representing relationships between requests and workers.

        Args:
            new_request_id (UUID): The unique identifier for the new request.
            waiting_requests (list[TCRequest]): The list of requests that are waiting to be assigned to a worker (including the new request).
            active_requests (list[TCRequest]): The list of requests that are currently being processed (i.e., a worker is transcoding them).
            running_workers (list[TCWorker]): The list of workers that are currently running (busy and idle).

        Returns:
            A tuple containing:

                - A list of strings representing the number of workers to create (the strings are the keys in th `_vm_params` or `_pod_params` dictionary).
                - A list of tuples, each containing two UUIDs representing relationships between requests and workers (the first element of the tuple is the UUID of the request to be assigned to the worker identified by the UUID in the second element of the tuple).

        note:
            Either of the lists can be empty.

        """
        ...

    def worker_available(
        self,
        worker_id: UUID,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[UUID], list[tuple[UUID, UUID]]]:
        """The [DacircoController][dacirco.controller.controller.DaCircoController] calls this method when a worker becomes available.'

        This method must decide:

        - Whether to stop and remove one or more workers.  These workers must be idle.
        - Which requests to assign to the newly available worker.

        note:
            This method can decide to do nothing (i.e., return empty lists).

        Args:
            worker_id (UUID): The unique identifier for the worker that became available.
            waiting_requests (list[TCRequest]): The list of requests that are waiting to be assigned to a worker.
            active_requests (list[TCRequest]): The list of requests that are currently being processed (i.e., a worker is transcoding them).
            running_workers (list[TCWorker]): The list of workers that are currently running (busy and idle).

        Returns:
            A tuple containing:

                - A list of the UUIDs of the worker(s) to be stopped and removed.
                - A list of tuples, each containing two UUIDs representing relationships between requests and workers (the first element of the tuple is the UUID of the request to be assigned to the worker identified by the UUID in the second element of the tuple).

        note:
            Either of the lists can be empty.

        """
        ...

    def periodic_update(
        self,
        waiting_requests: list[TCRequest],
        active_requests: list[TCRequest],
        running_workers: list[TCWorker],
    ) -> tuple[list[str], list[UUID], list[tuple[UUID, UUID]]]:
        """The [DacircoController][dacirco.controller.controller.DaCircoController] calls this method periodically (once a second by default).'

        This method must decide:

        - Whether to start new workers.
        - Whether to stop and remove one or more workers.  These workers must be idle.
        - Which requests to assign to the newly available worker.

        note:
            This method can decide to do nothing (i.e., return empty lists).

        Args:
            waiting_requests (list[TCRequest]): The list of requests that are waiting to be assigned to a worker.
            active_requests (list[TCRequest]): The list of requests that are currently being processed (i.e., a worker is transcoding them).
            running_workers (list[TCWorker]): The list of workers that are currently running (busy and idle).

        Returns:
            A tuple containing:

                - A list of strings representing the number of workers to create (the strings are the keys in th _vm_params or _pod_params dictionary).
                - A list of the UUIDs of the worker(s) to be stopped and removed.
                - A list of tuples, each containing two UUIDs representing relationships between requests and workers (the first element of the tuple is the UUID of the request to be assigned to the worker identified by the UUID in the second element of the tuple).

        note:
            Any of these lists can be empty.  They can also all be empty.
        """
        ...
