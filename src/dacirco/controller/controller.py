"""
This module defines the DaCircoController class, which is responsible for managing the lifecycle of workers and handling requests in the DaCirco system.
"""

import logging
from minio import Minio
from typing import Optional
from uuid import UUID, uuid4

from dacirco.controller.data_store.data_store import DataStore
from dacirco.controller.events import TCWorkerErrorEvent, TCWorkerEvent
from dacirco.controller.grace_period import GracePeriod
from dacirco.controller.minimize_workers import MinimizeWorkers
from dacirco.controller.my_scheduling_algorithm import MySchedulingAlgorithm
from dacirco.controller.scheduler_with_reserve import SchedulerWithReserve
from dacirco.controller.scheduler_with_queue import SchedulerWithQueue
from dacirco.controller.adaptive_grace_period import AdaptiveGracePeriod
from dacirco.controller.priority_based_dynamic import PriorityBasedDynamic
from dacirco.controller.pod_manager import PodManager
from dacirco.controller.scheduler import Scheduler
from dacirco.controller.send_task_protocol import SendTaskToWorker
from dacirco.controller.vm_manager import VMManager
from dacirco.controller.worker_manager import WorkerManager
from dacirco.dataclasses import (
    MinIoParameters,
    PodParameters,
    SchedulerParameters,
    TCRequest,
    TCTask,
    TCWorker,
    VMParameters,
)
from dacirco.enums import (
    AlgorithmName,
    RunnerType,
    TCRequestStatus,
    TCWorkerEventType,
    TCWorkerStatus,
)

_logger = logging.getLogger("dacirco")


class DaCircoController:
    """
    The DaCircoController is responsible for managing the lifecycle of workers and handling requests.

    Attributes:
        _vm_params (dict[str, VMParameters]): Parameters for VM workers.
        _pod_params (dict[str, PodParameters]): Parameters for Pod workers.
        _minio_params (MinIoParameters): Parameters for MinIO.
        _bind_ip_address (str): IP address to bind the controller.
        _ip_address_for_worker (str): IP address for workers.
        _event_log_file (str): Path to the event log file.
        _data_store (DataStore): Data store for requests and workers.
        _sched_param (SchedulerParameters): Scheduler parameters.
        _algorithm (AlgorithmName): Algorithm used by the scheduler.
        _rpc_server (SendTaskToWorker): RPC server for sending tasks to workers.
        _worker_base_name (str): Base name for workers.
        _worker_manager (WorkerManager): Manager for workers.
        _worker_counter (int): Counter for worker IDs.
        _scheduler (Scheduler): Scheduler for managing requests and workers.

    Methods:
        __init__:
            Initializes the DaCircoController with the given parameters.

        new_request:
            Handles a new request by scheduling it and creating necessary workers.

        handle_worker_available:
            Handles the event when a worker becomes available.

        register_worker:
            Registers a new worker and handles its availability.

        process_event:
            Processes events related to worker tasks.

        process_error_event:
            Processes error events related to worker tasks.

        get_requests:
            Returns a list of all request IDs.

        get_request:
            Returns the request corresponding to the given request ID.

        get_request_status:
            Returns the status of the request corresponding to the given request ID.

        get_workers:
            Returns a list of all worker IDs.

        get_worker:
            Returns the worker corresponding to the given worker ID.

        get_worker_status:
            Returns the status of the worker corresponding to the given worker ID.

        periodic_update:
            Performs periodic updates to manage workers and requests.
    """

    def __init__(
        self,
        vm_params: dict[str, VMParameters],
        minio_params: MinIoParameters,
        bind_ip_address: str,
        ip_address_for_worker: str,
        port_for_worker: int,
        event_log_file: str,
        sched_params: SchedulerParameters,
        pod_params: dict[str, PodParameters],
        data_store: DataStore,
        rpc_server: SendTaskToWorker,
    ) -> None:
        """
        Initializes the Controller with the given parameters.

        Args:
            vm_params (dict[str, VMParameters]): Dictionary containing VM parameters.
            minio_params (MinIoParameters): Parameters for MinIO configuration.
            bind_ip_address (str): IP address to bind the controller.
            ip_address_for_worker (str): IP address for the worker nodes.
            port_for_worker (int): Port for the worker nodes.
            event_log_file (str): Path to the event log file.
            sched_params (SchedulerParameters): Parameters for the scheduler.
            pod_params (dict[str, PodParameters]): Dictionary containing Pod parameters.
            data_store (DataStore): Data store instance.
            rpc_server (SendTaskToWorker): RPC server instance for sending tasks to workers.

        Returns:
            None
        """
        self._vm_params = vm_params
        self._pod_params = pod_params
        self._minio_params = minio_params
        self._bind_ip_address = bind_ip_address
        self._ip_address_for_worker = ip_address_for_worker
        self._port_for_worker = port_for_worker
        self._event_log_file = event_log_file
        self._data_store = data_store
        self._sched_param = sched_params
        self._algorithm = sched_params.algorithm
        self._rpc_server = rpc_server
        self._minio = Minio(
            f"{minio_params.server}:{minio_params.port}",
            access_key=minio_params.access_key,
            secret_key=minio_params.access_secret,
            secure=False,
        )
        self._minio_bucket_name = "videos"
        self._minio_input_path = "input-videos"
        if self._sched_param.runner_type is RunnerType.vm:
            self._worker_base_name = "tc-vm-"
            self._worker_manager: WorkerManager = VMManager(
                vm_parameters=vm_params,
                minio_parameters=minio_params,
                sched_params=sched_params,
                ip_address_for_worker=ip_address_for_worker,
                port_for_worker=port_for_worker,
            )
        else:
            self._worker_base_name = "tc-pod-"
            self._worker_manager: WorkerManager = PodManager(
                pod_parameters=pod_params,
                minio_parameters=minio_params,
                sched_params=sched_params,
                ip_address_for_worker=ip_address_for_worker,
                port_for_worker=port_for_worker,
            )
        self._worker_counter = 0
        if self._algorithm is AlgorithmName.minimize_workers:
            self._scheduler: Scheduler = MinimizeWorkers(
                sched_params=sched_params, pod_params=pod_params, vm_params=vm_params
            )
        if self._algorithm is AlgorithmName.grace_period:
            self._scheduler: Scheduler = GracePeriod(
                sched_params=sched_params, pod_params=pod_params, vm_params=vm_params
            )
        if self._algorithm is AlgorithmName.my_scheduling_algorithm:
            self._scheduler: Scheduler = MySchedulingAlgorithm(
                sched_params=sched_params, pod_params=pod_params, vm_params=vm_params
            )
        if self._algorithm is AlgorithmName.scheduler_with_reserve:
            self._scheduler: Scheduler = SchedulerWithReserve(
                sched_params=sched_params, pod_params=pod_params, vm_params=vm_params
            )
        if self._algorithm is AlgorithmName.scheduler_with_queue:
            self._scheduler: Scheduler = SchedulerWithQueue(
                sched_params=sched_params, pod_params=pod_params, vm_params=vm_params
            )
        if self._algorithm is AlgorithmName.adaptive_grace_period:
            self._scheduler: Scheduler = AdaptiveGracePeriod(
                sched_params=sched_params, pod_params=pod_params, vm_params=vm_params
            )
        if self._algorithm is AlgorithmName.priority_based_dynamic:
            self._scheduler: Scheduler = PriorityBasedDynamic(
                sched_params=sched_params, pod_params=pod_params, vm_params=vm_params
            )

    def new_request(
        self,
        request: TCRequest,
    ) -> None:
        """
        Handles a new request by adding it to the data store, scheduling workers,
        and assigning tasks to workers.

        Args:
            request (TCRequest): The request to be processed.
        """
        res = self._minio.stat_object(
            self._minio_bucket_name, f"{self._minio_input_path}/{request.input_video}"
        )
        metadata = res.metadata
        size = res.size
        duration = None
        res_x = None
        res_y = None
        if metadata:
            if "x-amz-meta-duration" in metadata.keys():
                duration = float(metadata["x-amz-meta-duration"])
            if "x-amz-meta-res-x" in metadata.keys():
                res_x = int(metadata["x-amz-meta-res-x"])
            if "x-amz-meta-res-y" in metadata.keys():
                res_y = int(metadata["x-amz-meta-res-y"])

        self._data_store.add_request(
            request, duration=duration, size=size, res_x=res_x, res_y=res_y
        )
        new_workers, request_worker_pairs = self._scheduler.new_request(
            request.request_id,
            self._data_store.get_waiting_requests(),
            self._data_store.get_active_requests(),
            self._data_store.get_running_workers(),
        )
        for key in new_workers:
            self._worker_counter += 1
            worker_id = uuid4()
            worker_name = self._worker_base_name + str(self._worker_counter)
            node = self._worker_manager.create_worker(key, worker_id, worker_name)
            self._data_store.add_worker(worker_id, worker_name, key, node)
        for worker_id, request_id in request_worker_pairs:
            task_id = self._data_store.assign_request_to_worker(request_id, worker_id)
            tc_task = TCTask(
                request_desc=request,
                worker_id=worker_id,
                task_id=task_id,
            )
            self._rpc_server.send_tc_task_to_worker(worker_id, tc_task)

    def handle_worker_available(self, worker_id: UUID) -> None:
        """
        Handles the event when a worker becomes available.

        This method performs the following steps:

        1. Determines which workers to remove and which requests to assign to available workers.
        2. Logs the workers to be removed and the request-worker pairs.
        3. Destroys and removes workers that are no longer needed.
        4. Assigns requests to available workers and sends tasks to them.

        Args:
            worker_id (UUID): The unique identifier of the worker that has become available.

        Raises:
            ValueError: If a request corresponding to a request ID is not found in the data store.
        """
        workers_to_remove, request_worker_pairs = self._scheduler.worker_available(
            worker_id,
            self._data_store.get_waiting_requests(),
            self._data_store.get_active_requests(),
            self._data_store.get_running_workers(),
        )
        _logger.debug(
            f"handle_worker_available: workers to remove: {workers_to_remove}"
        )
        _logger.debug(
            f"handle_worker_available: request worker pairs: {request_worker_pairs}"
        )
        for worker_id in workers_to_remove:
            self._worker_manager.destroy_worker(worker_id)
            self._data_store.mark_worker_stopped(worker_id)
            self._rpc_server.remove_worker_queue(worker_id)
        for worker_id, request_id in request_worker_pairs:
            request = self._data_store.get_request(request_id)
            if request is None:
                _logger.error(f"Request {request_id} not found")
                raise ValueError(f"Register worker: Request {request_id} not found")
            task_id = self._data_store.assign_request_to_worker(request_id, worker_id)
            tc_task = TCTask(
                request_desc=request,
                worker_id=worker_id,
                task_id=task_id,
            )
            self._rpc_server.send_tc_task_to_worker(worker_id, tc_task)
            self._data_store.task_download_started(task_id)

    def register_worker(self, worker_id: UUID) -> None:
        """
        Registers a worker as available.

        This method marks the worker with the given worker_id as available in the data store
        and then handles any additional logic required when a worker becomes available.

        Args:
            worker_id (UUID): The unique identifier of the worker to be registered.
        """
        self._data_store.mark_worker_available(worker_id)
        self.handle_worker_available(worker_id)

    def process_event(self, event: TCWorkerEvent) -> None:
        """
        Processes different types of events related to task workflow.

        Args:
            event (TCWorkerEvent): The event to process. It contains information about the type of event and associated task.

        Raises:
            ValueError: If the event type is invalid.

        Event Types:
            - TCWorkerEventType.file_downloaded: Logs the event and marks the task as transcoding started.
            - TCWorkerEventType.transcoding_completed: Logs the event and marks the task as upload started.
            - TCWorkerEventType.file_uploaded: Logs the event, marks the task as completed, marks the worker as available, and handles worker availability.
            - TCWorkerEventType.keepalive: No action is taken.
        """
        if event.type == TCWorkerEventType.file_downloaded:
            _logger.debug(f"Task {event.task_id} downloaded the file")
            self._data_store.task_transcoding_started(event.task_id)
        elif event.type == TCWorkerEventType.transcoding_completed:
            _logger.debug(f"Task {event.task_id} transcoding completed")
            self._data_store.task_upload_started(event.task_id)
        elif event.type == TCWorkerEventType.file_uploaded:
            _logger.debug(f"Task {event.task_id} uploaded the file")
            self._data_store.task_completed(event.task_id)
            self._data_store.mark_worker_available(event.worker_id)
            self.handle_worker_available(event.worker_id)
        elif event.type == TCWorkerEventType.keepalive:
            pass
        else:
            _logger.error(f"Invalid event type {event.type}")
            raise ValueError(f"Invalid event type {event.type}")

    def process_error_event(self, event: TCWorkerErrorEvent) -> None:
        """
        Handles the processing of an error event for a worker.

        This method logs the error event, marks the associated task as failed,
        and ensures the worker is destroyed and replaced with a new one. It
        performs the following steps:

        1. Logs the error event.
        2. Marks the task associated with the event as failed.
        3. Retrieves the worker associated with the event.
        4. If the worker is not found, logs an error and raises a ValueError.
        5. Marks the worker as available.
        6. Destroys the worker.
        7. Marks the worker as stopped.
        8. Removes the worker's queue from the RPC server.
        9. Increments the worker counter.
        10. Creates a new worker with a unique ID and name.
        11. Adds the new worker to the data store.

        Args:
            event (TCWorkerErrorEvent): The error event to process.

        Raises:
            ValueError: If the worker associated with the event is not found.
        """
        _logger.info(f"Processing error event {event}")
        self._data_store.task_failed(event.task_id)
        # Just to bo on the safe side we destroy the worker
        # and create a new one
        worker = self._data_store.get_worker(event.worker_id)
        if worker is None:
            _logger.error(f"Worker {event.worker_id} not found")
            raise ValueError(f"Worker {event.worker_id} not found")
        key = worker.param_key
        self._data_store.mark_worker_available(event.worker_id)
        self._worker_manager.destroy_worker(event.worker_id)
        self._data_store.mark_worker_stopped(event.worker_id)
        self._rpc_server.remove_worker_queue(event.worker_id)
        self._worker_counter += 1
        worker_id = uuid4()
        worker_name = self._worker_base_name + str(self._worker_counter)
        node = self._worker_manager.create_worker(key, worker_id, worker_name)
        self._data_store.add_worker(worker_id, worker_name, key, node)

    def get_requests(self) -> list[str]:
        """
        Retrieve the list of request IDs from the data store.

        Returns:
            list[str]: list of request IDs as strings.
        """
        return [str(request_id) for request_id in self._data_store.get_requests()]

    def get_request(self, request_id: UUID) -> Optional[TCRequest]:
        """
        Retrieve a request from the data store by its unique identifier.

        Args:
            request_id (UUID): The unique identifier of the request to retrieve.

        Returns:
            Optional[TCRequest]: The request object if found, otherwise None.
        """
        return self._data_store.get_request(request_id)

    def get_request_status(self, request_id: UUID) -> Optional[TCRequestStatus]:
        """
        Retrieve the status of a request by its ID.

        Args:
            request_id (UUID): The unique identifier of the request.

        Returns:
            Optional[TCRequestStatus]: The status of the request if found, otherwise None.
        """
        request = self._data_store.get_request(request_id)
        if request:
            return request.status
        return None

    def get_workers(self) -> list[str]:
        """
        Retrieve the list of worker IDs as strings.

        Returns:
            list[str]: A list containing worker IDs in string format.
        """
        return [str(worker_id) for worker_id in self._data_store.get_workers()]

    def get_worker(self, worker_id: UUID) -> Optional[TCWorker]:
        """
        Retrieve a worker by their unique identifier.

        The REST frontend uses this method to get the worker object.

        Args:
            worker_id (UUID): The unique identifier of the worker.

        Returns:
            Optional[TCWorker]: The worker object if found, otherwise None.
        """
        return self._data_store.get_worker(worker_id)

    def get_worker_status(self, worker_id: UUID) -> Optional[TCWorkerStatus]:
        """
        Retrieve the status of a worker given their unique identifier.

        The REST frontend uses this method to get the status of a worker.

        Args:
            worker_id (UUID): The unique identifier of the worker.

        Returns:
            Optional[TCWorkerStatus]: The status of the worker if found, otherwise None.
        """
        worker = self._data_store.get_worker(worker_id)
        if worker:
            return worker.status
        return None

    def periodic_update(self) -> None:
        """
        Periodically updates the state of workers and requests.

        This method performs the following operations:

        1. Calls the scheduler's `periodic_update` method to get:
           - New workers to be added.
           - Workers to be removed.
           - Pairs of workers and requests to be assigned.
        2. For each new worker:
           - Increments the worker counter.
           - Generates a unique worker ID.
           - Creates a worker with a unique name.
           - Adds the worker to the data store.
        3. For each worker to be removed:
           - Destroys the worker.
           - Marks the worker as stopped in the data store.
           - Removes the worker's queue from the RPC server.
        4. For each worker-request pair:
           - Retrieves the request from the data store.
           - Assigns the request to the worker.
           - Creates a TCTask object.
           - Sends the task to the worker via the RPC server.
           - Marks the task as started in the data store.

        Raises:
            ValueError: If a request corresponding to a worker-request pair is not found.
        """
        new_workers, workers_to_remove, request_worker_pairs = (
            self._scheduler.periodic_update(
                self._data_store.get_waiting_requests(),
                self._data_store.get_active_requests(),
                self._data_store.get_running_workers(),
            )
        )
        for key in new_workers:
            self._worker_counter += 1
            worker_id = uuid4()
            worker_name = self._worker_base_name + str(self._worker_counter)
            node = self._worker_manager.create_worker(key, worker_id, worker_name)
            self._data_store.add_worker(worker_id, worker_name, key, node)
        for worker_id in workers_to_remove:
            self._worker_manager.destroy_worker(worker_id)
            self._data_store.mark_worker_stopped(worker_id)
            self._rpc_server.remove_worker_queue(worker_id)
        for worker_id, request_id in request_worker_pairs:
            request = self._data_store.get_request(request_id)
            if request is None:
                _logger.error(f"Request {request_id} not found")
                raise ValueError(f"Register worker: Request {request_id} not found")
            task_id = self._data_store.assign_request_to_worker(request_id, worker_id)
            tc_task = TCTask(
                request_desc=request,
                worker_id=worker_id,
                task_id=task_id,
            )
            self._rpc_server.send_tc_task_to_worker(worker_id, tc_task)
            self._data_store.task_download_started(task_id)
