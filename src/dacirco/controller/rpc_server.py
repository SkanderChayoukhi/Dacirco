"""The GRPC DaCirco server.

It receives the requests from the REST front-end and it forwards them to the scheduler.

warning
    Students are not supposed to modify this code!

"""

import asyncio
import logging
import traceback
import uuid
from uuid import UUID
from collections.abc import Iterator

import grpc  # type: ignore

from dacirco.controller.controller import DaCircoController
from dacirco.controller.data_store.data_store import DataStore
from dacirco.controller.events import TCWorkerErrorEvent, TCWorkerEvent
from dacirco.dataclasses import (
    MinIoParameters,
    PodParameters,
    SchedulerParameters,
    TCRequest,
    TCTask,
    VMParameters,
)
from dacirco.enums import RunnerType, TCRequestStatus, TCWorkerStatus
from dacirco.proto import dacirco_pb2_grpc
from dacirco.proto.dacirco_pb2 import (
    GrpcEmpty,
    GrpcErrorEvent,
    GrpcEvent,
    GrpcRequestID,
    GrpcRequestIDList,
    GrpcServiceReply,
    GrpcTCRequest,
    GrpcTCRequestReply,
    GrpcTCRequestStatus,
    GrpcTCTask,
    GrpcWorkerDesc,
    GrpcWorkerFullDesc,
    GrpcWorkerID,
    GrpcWorkerIDList,
    GrpcWorkerState,
)

_logger = logging.getLogger("dacirco.grpc_server")
# Coroutines to be invoked when the event loop is shutting down.
_cleanup_coroutines = []


async def shutdown(data_store: DataStore) -> None:
    data_store.end_current_run()
    print("Shutdown complete")


class DaCircoRCPServer(dacirco_pb2_grpc.DaCircogRPCServiceServicer):
    """DaCirco RPC Server"""

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
    ) -> None:
        super().__init__()
        self._controller = DaCircoController(
            vm_params=vm_params,
            pod_params=pod_params,
            minio_params=minio_params,
            sched_params=sched_params,
            bind_ip_address=bind_ip_address,
            ip_address_for_worker=ip_address_for_worker,
            port_for_worker=port_for_worker,
            event_log_file=event_log_file,
            data_store=data_store,
            rpc_server=self,
        )
        self._call_interval = sched_params.call_interval
        self._async_queues: dict[UUID, asyncio.Queue[GrpcTCTask]] = {}
        self._vm_params = vm_params
        self._pod_params = pod_params
        self._sched_params = sched_params

    async def submit_request(
        self,
        request: GrpcTCRequest,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcTCRequestReply:
        """Submit Request endpoint

        The REST front end calls this method every time it receives a new request.

        Args:
            request (dacirco_pb2.GrpcTCRequest): The GrpcTCRequest object
            context (grpc.aio.ServicerContext): The gRPC context

        Returns:
            dacirco_pb2.GrpcTCRequestReply: The reply object (see)
        """
        _logger.info(
            "Received TC request from the REST frontend. input_video: %s bitrate: %i speed %s",
            request.input_video,
            request.bitrate,
            request.speed,
        )
        request_id = uuid.uuid4()
        tc_request = TCRequest(
            input_video=request.input_video,
            output_video=request.output_video,
            bitrate=request.bitrate,
            speed=request.speed,
            request_id=request_id,
        )
        res = GrpcTCRequestReply(
            success=True, error_message="", request_id=str(request_id)
        )
        try:
            self._controller.new_request(tc_request)
        except Exception as e:
            _logger.error("Error processing new request: %s", e)
            _logger.error(traceback.format_exc())
            res = GrpcTCRequestReply(
                success=False, error_message=str(e), request_id=str(request_id)
            )
        return res

    async def register_worker(
        self,
        request: GrpcWorkerDesc,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcServiceReply:
        """
        Registers a worker with the server.

        This method handles the registration of a worker by adding a queue to send messages to this worker.  If the worker is already registered, it logs an error and returns a failure response.

        Args:
            request (GrpcWorkerDesc): The request object containing the worker's
                name and ID.
            context (grpc.aio.ServicerContext): The gRPC context for the request.

        Returns:
            GrpcServiceReply: A response object indicating the success or failure
            of the registration process.
        """
        _logger.info(
            "TC Worker registration.  Name: %s, Id: %s",
            request.name,
            request.id,
        )
        res = GrpcServiceReply(success=True)
        worker_id = uuid.UUID(request.id)
        if worker_id in self._async_queues.keys():
            msg = f"====EE===== worker registering again. Id: {worker_id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            self._async_queues[worker_id] = asyncio.Queue[GrpcTCTask]()
            try:
                self._controller.register_worker(uuid.UUID(request.id))
            except Exception as e:
                _logger.error("Error registering worker: %s", e)
                res.success = False
                res.error_message = str(e)
        return res

    async def get_tasks(
        self,
        request: GrpcWorkerDesc,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> Iterator[GrpcTCTask]:  # type: ignore
        """
        Asynchronous generator that yields tasks from a worker's queue.

        Args:
            request (GrpcWorkerDesc): The gRPC request containing the worker's ID.
            context (grpc.aio.ServicerContext): The gRPC context (ignored).

        Yields:
            tc_task: The next task from the worker's queue.

        Logs:
            Debug information about the task being sent, including the worker ID and input video.
        """
        worker_id = request.id
        queue = self._async_queues[uuid.UUID(worker_id)]
        while True:
            tc_task = await queue.get()
            _logger.debug(
                "get_tasks: sending tc task to: %s, input_video: %s",
                worker_id,
                tc_task.input_video,
            )
            yield tc_task  # type: ignore

    def send_tc_task_to_worker(self, worker_id: UUID, tc_task_desc: TCTask) -> None:
        """
        Sends a transcoding task to a worker.

        This method enqueues a message containing the transcoding task details to the specified worker's queue.

        The [DaCircoController][dacirco.controller.controller.DaCircoController] calls this method to send a transcoding task to a worker.

        Args:
            worker_id (UUID): The unique identifier of the worker to which the task is to be sent.
            tc_task_desc (TCTask): The description of the transcoding task, including input and output video details, bitrate, speed, worker ID, and task ID.

        Returns:
            None
        """
        _logger.debug(
            "Enqueueing message for tc task to: %s, input_video: %s",
            worker_id,
            tc_task_desc.request_desc.input_video,
        )
        queue = self._async_queues[worker_id]
        queue.put_nowait(
            GrpcTCTask(
                input_video=tc_task_desc.request_desc.input_video,
                output_video=tc_task_desc.request_desc.output_video,
                bitrate=tc_task_desc.request_desc.bitrate,
                speed=tc_task_desc.request_desc.speed,
                worker_id=str(tc_task_desc.worker_id),
                task_id=str(tc_task_desc.task_id),
            )
        )

    def remove_worker_queue(self, worker_id: UUID) -> None:
        """
        Removes the worker queue associated with the given worker ID.

        Args:
            worker_id (UUID): The unique identifier of the worker whose queue is to be removed.

        Raises:
            KeyError: If the worker_id does not exist in the _async_queues.
        """
        del self._async_queues[worker_id]

    async def submit_event(
        self,
        request: GrpcEvent,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcServiceReply:
        """
        Handles the submission of an event from a gRPC request.

        Args:
            request (GrpcEvent): The gRPC event containing the event details.
            context (grpc.aio.ServicerContext): The gRPC context for the request.

        Returns:
            GrpcServiceReply: The reply indicating the success or failure of the event processing.

        Logs:
            - Debug information about the new event if the event type is not KEEPALIVE.
            - Warning if the event is from an unknown worker.
            - Error if there is an exception while processing the event.
        """
        if request.event_type is not GrpcEvent.KEEPALIVE:
            _logger.debug(
                "New Event. Type: %s, worker: %s, task: %s",
                request.event_type,
                request.worker_id,
                request.task_id,
            )
        res = GrpcServiceReply(success=True)
        worker_id = UUID(request.worker_id.replace("-", ""))
        if worker_id not in self._async_queues.keys():
            msg = f"====WW===== Ignoring event from unknown worker. Id: {request.worker_id}"
            _logger.warning(msg)
            res.success = False
            res.error_message = msg
        else:
            try:
                self._controller.process_event(TCWorkerEvent(request))
            except Exception as e:
                _logger.error("Error processing event: %s", e)
                _logger.error("Event: %s", request)
                res.success = False
                res.error_message = str(e)
        return res

    async def submit_error(
        self,
        request: GrpcErrorEvent,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcServiceReply:
        """
        Transcoding workers call this method to report an error event.

        This method logs the error details and processes the error event through the controller.  If the worker ID from the request is not recognized, it logs an error and returns a failure response.

        Args:
            request (GrpcErrorEvent): The gRPC request containing error event details.
            context (grpc.aio.ServicerContext): The gRPC context for the request.

        Returns:
            GrpcServiceReply: The response indicating the success or failure of processing the error event.
        """
        _logger.error(
            "===\u274c Error. Type: %s, worker: %s, task: %s, error_message: %s",
            request.error_type,
            request.worker_id,
            request.task_id,
            request.error_message,
        )
        res = GrpcServiceReply(success=True)
        if UUID(request.worker_id.replace("-", "")) not in self._async_queues.keys():
            msg = f"====EE===== Event from unknown worker. Id: {request.worker_id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            try:
                self._controller.process_error_event(TCWorkerErrorEvent(request))
            except Exception as e:
                _logger.error("Error processing error event: %s", e)
                _logger.error("Event: %s", request)
                res.success = False
                res.error_message = str(e)
        return res

    async def get_requests(
        self,
        request: GrpcEmpty,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcRequestIDList:
        """
        Handles the gRPC request to get a list of request IDs.

        The REST front end calls this method to get a list of all the requests that have been submitted.

        Args:
            request (GrpcEmpty): The incoming gRPC request, which is empty.
            context (grpc.aio.ServicerContext): The context of the gRPC call.

        Returns:
            GrpcRequestIDList: A list of request IDs.
        """
        return GrpcRequestIDList(request_ids=self._controller.get_requests())

    async def get_request(
        self,
        request: GrpcRequestID,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcTCRequest:
        """
        Handles the gRPC request to retrieve a specific request by its ID.

        The REST front end calls this method to get the details of a specific request.

        Args:
            request (GrpcRequestID): The gRPC request containing the request ID.
            context (grpc.aio.ServicerContext): The gRPC context for the request.

        Returns:
            GrpcTCRequest: The gRPC response containing the request details. If the request
            is not found, returns a default GrpcTCRequest with empty or zero values.
        """
        req_desc = self._controller.get_request(
            UUID(request.request_id.replace("-", ""))
        )
        if req_desc:
            res = GrpcTCRequest(
                input_video=req_desc.input_video,
                bitrate=req_desc.bitrate,
                speed=req_desc.speed,
            )
        else:
            res = GrpcTCRequest(
                input_video="",
                bitrate=0,
                speed="",
            )
        return res

    async def get_request_status(
        self,
        request: GrpcRequestID,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcTCRequestStatus:
        """
        Retrieves the status of a given request.

        The REST front end calls this method to get the status of a specific request.

        Args:
            request (GrpcRequestID): The request ID for which the status is being queried.
            context (grpc.aio.ServicerContext): The gRPC context for the request.

        Returns:
            GrpcTCRequestStatus: The status of the request, which can be one of the following:
                - INVALID: If the request status is invalid.
                - NOT_FOUND: If the request status is not found.
                - WAITING: If the request is waiting or assigned.
                - STARTED: If the request is in the process of downloading, transcoding, or uploading.
                - COMPLETED: If the request has been completed.
                - ERROR: If the request has failed.
        """
        res = GrpcTCRequestStatus.INVALID
        status = self._controller.get_request_status(
            UUID(request.request_id.replace("-", ""))
        )
        if status is None:
            res = GrpcTCRequestStatus.NOT_FOUND
        elif status is TCRequestStatus.waiting:
            res = GrpcTCRequestStatus.WAITING
        elif status is TCRequestStatus.assigned:
            res = GrpcTCRequestStatus.WAITING
        elif status is TCRequestStatus.downloading_file:
            res = GrpcTCRequestStatus.STARTED
        elif status is TCRequestStatus.transcoding:
            res = GrpcTCRequestStatus.STARTED
        elif status is TCRequestStatus.uploading_file:
            res = GrpcTCRequestStatus.STARTED
        elif status is TCRequestStatus.completed:
            res = GrpcTCRequestStatus.COMPLETED
        elif status is TCRequestStatus.failed:
            res = GrpcTCRequestStatus.ERROR
        return GrpcTCRequestStatus(request_status=res)

    async def get_workers(
        self,
        request: GrpcEmpty,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcWorkerIDList:
        """
        Retrieves a list of worker IDs.

        The REST front end calls this method to get a list of worker IDs managed by the controller.

        Args:
            request (GrpcEmpty): The gRPC request object, which is empty for this call.
            context (grpc.aio.ServicerContext): The gRPC context object, providing RPC-specific information.

        Returns:
            GrpcWorkerIDList: A list of worker IDs managed by the controller.
        """
        return GrpcWorkerIDList(worker_ids=self._controller.get_workers())

    async def get_worker(
        self,
        request: GrpcWorkerID,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcWorkerFullDesc:
        """
        Retrieves the worker details based on the provided worker ID.

        The REST front end calls this method to get the full description of a worker, including the worker name, ID, CPUs, memory, and node.

        Args:
            request (GrpcWorkerID): The gRPC request containing the worker ID.
            context (grpc.aio.ServicerContext): The gRPC context (ignored).

        Returns:
            GrpcWorkerFullDesc: The full description of the worker, including name, ID, CPUs, memory, and node.

        Raises:
            Exception: If there is an error processing the worker retrieval.
        """
        name = ""
        id = ""
        cpus = ""
        memory = ""
        node = ""

        tc_worker = None
        try:
            tc_worker = self._controller.get_worker(
                worker_id=UUID(request.worker_id.replace("-", ""))
            )
        except Exception as e:
            _logger.error("Error processing error event: %s", e)
            _logger.error("Event: %s", request)

        if tc_worker:
            if self._sched_params.runner_type is RunnerType.vm:
                if self._vm_params[tc_worker.param_key].cpus:
                    cpus = str(self._vm_params[tc_worker.param_key].cpus)
                else:
                    cpus = ""
                if self._vm_params[tc_worker.param_key].memory:
                    memory = str(self._vm_params[tc_worker.param_key].memory)
                else:
                    memory = ""
            name = tc_worker.name
            id = str(tc_worker.worker_id)
            cpus = str(cpus)
            memory = str(memory)
            if tc_worker.node:
                node = tc_worker.node
            else:
                node = ""

        res = GrpcWorkerFullDesc(name=name, id=id, cpus=cpus, memory=memory, node=node)
        return res

    async def get_worker_status(
        self,
        request: GrpcWorkerID,
        context: grpc.aio.ServicerContext,  # type: ignore
    ) -> GrpcWorkerState:
        """
        Get the status of a worker.

        The REST front end calls this method to get the status of a worker based on the worker ID.

        Args:
            request (GrpcWorkerID): The gRPC request containing the worker ID.
            context (grpc.aio.ServicerContext): The gRPC context (ignored).

        Returns:
            GrpcWorkerState: The state of the worker, which can be one of the following:
                - INVALID: The worker status is invalid.
                - BOOTING: The worker is currently booting.
                - BUSY: The worker is busy.
                - IDLE: The worker is idle.
                - STOPPED: The worker has stopped.
                - NOT_FOUND: The worker was not found.
        """
        res = GrpcWorkerState.INVALID
        status = self._controller.get_worker_status(
            worker_id=UUID(request.worker_id.replace("-", ""))
        )
        if status is TCWorkerStatus.booting:
            res = GrpcWorkerState.BOOTING
        if status is TCWorkerStatus.busy:
            res = GrpcWorkerState.BUSY
        if status is TCWorkerStatus.idle:
            res = GrpcWorkerState.IDLE
        if status is TCWorkerStatus.stopped:
            res = GrpcWorkerState.STOPPED
        if status is None:
            res = GrpcWorkerState.NOT_FOUND
        return GrpcWorkerState(worker_status=res)

    async def periodic_call(self) -> None:
        """
        Periodically calls the controller's periodic_update method.

        This coroutine first waits for 2 seconds, then enters an infinite loop
        where it calls the periodic_update method of the controller and waits
        for a specified interval before calling it again.

        Returns:
            None
        """
        await asyncio.sleep(2)
        while True:
            self._controller.periodic_update()
            await asyncio.sleep(self._call_interval)


async def serve(
    vm_parameters: dict[str, VMParameters],
    minio_parameters: MinIoParameters,
    port: int,
    bind_ip_address: str,
    ip_address_for_worker: str,
    port_for_worker: int,
    event_log_file: str,
    sched_parameters: SchedulerParameters,
    pod_parameters: dict[str, PodParameters],
    data_store: DataStore,
) -> None:
    """
    Starts the gRPC server for the DaCirco service.

    Args:
        vm_parameters (dict[str, VMParameters]): Dictionary of VM parameters.
        minio_parameters (MinIoParameters): Parameters for MinIO configuration.
        port (int): Port number on which the server will listen.
        bind_ip_address (str): IP address to bind the server.
        ip_address_for_worker (str): IP address for the worker nodes.
        event_log_file (str): Path to the event log file.
        sched_parameters (SchedulerParameters): Parameters for the scheduler.
        pod_parameters (dict[str, PodParameters]): Dictionary of Pod parameters.
        data_store (DataStore): Data store instance.

    Returns:
        None
    """
    server = grpc.aio.server()  # type: ignore
    dacirco_server = DaCircoRCPServer(
        vm_params=vm_parameters,
        minio_params=minio_parameters,
        bind_ip_address=bind_ip_address,
        ip_address_for_worker=ip_address_for_worker,
        port_for_worker=port_for_worker,
        event_log_file=event_log_file,
        sched_params=sched_parameters,
        pod_params=pod_parameters,
        data_store=data_store,
    )
    dacirco_pb2_grpc.add_DaCircogRPCServiceServicer_to_server(dacirco_server, server)
    listen_addr = "[::]:" + str(port)
    server.add_insecure_port(listen_addr)
    _logger.info("Starting server on %s", listen_addr)

    await server.start()

    # from https://github.com/grpc/grpc/pull/26622/files to avoid the need to
    # press Ctl-C twice to stop the server
    async def server_graceful_shutdown() -> None:
        logging.info("Starting graceful shutdown...")
        # Shuts down the server with 2 seconds of grace period. During the
        # grace period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await server.stop(2)

    _cleanup_coroutines.append(server_graceful_shutdown())
    coroutines = [server.wait_for_termination(), dacirco_server.periodic_call()]
    await asyncio.gather(*coroutines, return_exceptions=False)


def run_server(
    vm_params: dict[str, VMParameters],
    minio_params: MinIoParameters,
    port: int,
    bind_ip_address: str,
    ip_address_for_worker: str,
    port_for_worker: int,
    event_log_file: str,
    sched_params: SchedulerParameters,
    pod_params: dict[str, PodParameters],
    data_store: DataStore,
) -> None:
    """
    Run the RPC server with the given parameters.

    Args:
        vm_params (dict[str, VMParameters]): Dictionary of VM parameters.
        minio_params (MinIoParameters): Parameters for MinIO configuration.
        port (int): Port number to bind the server.
        bind_ip_address (str): IP address to bind the server.
        ip_address_for_worker (str): IP address for the worker nodes.
        event_log_file (str): Path to the event log file.
        sched_params (SchedulerParameters): Parameters for the scheduler.
        pod_params (dict[str, PodParameters]): Dictionary of Pod parameters.
        data_store (DataStore): Data store instance.

    Returns:
        None
    """
    loop = asyncio.get_event_loop()
    _cleanup_coroutines.append(shutdown(data_store=data_store))
    try:
        loop.run_until_complete(
            serve(
                vm_parameters=vm_params,
                minio_parameters=minio_params,
                port=port,
                bind_ip_address=bind_ip_address,
                ip_address_for_worker=ip_address_for_worker,
                port_for_worker=port_for_worker,
                event_log_file=event_log_file,
                sched_parameters=sched_params,
                pod_parameters=pod_params,
                data_store=data_store,
            )
        )
    finally:
        if _cleanup_coroutines:
            loop.run_until_complete(asyncio.gather(*_cleanup_coroutines))
        loop.close()
