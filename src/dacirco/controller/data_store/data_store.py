"""
DataStore class for managing database operations related to runs, requests, workers, and parameters.
"""

from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import sessionmaker, Session
from dacirco.controller.data_store.models import (
    DbRun,
    DbTCRequest,
    DbWorker,
    DbTcTask,
    DbSchedulerParameters,
    DbVmParameters,
    DbPodParameters,
)
from dacirco.dataclasses import (
    TCRequest,
    TCWorker,
    SchedulerParameters,
    VMParameters,
    PodParameters,
)
from dacirco.enums import AlgorithmName, RunnerType, TCRequestStatus, TCWorkerStatus


class DataStore:
    """
    DataStore class for managing database operations related to runs, requests, tasks, and workers.

    Attributes:
        session_maker (sessionmaker[Session]): A SQLAlchemy sessionmaker instance.
        _current_run_id (Optional[UUID]): The ID of the current run.

    Methods:
        current_run_id:
            Returns the ID of the current run.

        add_run:
            Adds a new run to the database and sets it as the current run.

        end_current_run:
            Ends the current run, updating the end time and aborting any incomplete tasks and requests.

        add_request:
            Adds a new request to the current run.

        get_request:
            Retrieves a request by its ID.

        get_waiting_requests:
            Retrieves all waiting requests for the current run.

        get_active_requests:
            Retrieves all active (not waiting or completed) requests for the current run.

        get_requests:
            Retrieves all request IDs for the current run.

        task_download_started:
            Marks the start of the download phase for a task.

        task_transcoding_started:
            Marks the start of the transcoding phase for a task.

        task_upload_started:
            Marks the start of the upload phase for a task.

        task_completed:
            Marks a task as completed.

        task_failed:
            Marks a task as failed.

        assign_request_to_worker:
            Assigns a request to a worker and returns the task ID.

        add_worker:
            Adds a new worker to the current run.

        get_worker:
            Retrieves a worker by its ID.

        mark_worker_available:
            Marks a worker as available.

        mark_worker_stopped:
            Marks a worker as stopped.

        get_running_workers:
            Retrieves all running (not stopped) workers for the current run.

        get_workers:
            Retrieves all worker IDs for the current run.

        add_scheduler_parameters:
            Adds scheduler parameters for a run.

        add_vm_parameters:
            Adds VM parameters for a run.

        add_pod_parameters:
            Adds pod parameters for a run.
    """

    def __init__(self, session_maker: sessionmaker[Session]) -> None:
        """
        Initializes the DataStore instance.

        Args:
            session_maker (sessionmaker[Session]): A SQLAlchemy sessionmaker instance used to create new sessions.
        """
        self.session_maker = session_maker
        self._current_run_id = None

    @property
    def current_run_id(self) -> Optional[UUID]:
        """
        Retrieve the current run ID.

        Returns:
            Optional[UUID]: The UUID of the current run if it exists, otherwise None.
        """
        return self._current_run_id

    def add_run(
        self,
        id: UUID,
        tag: str,
    ) -> None:
        """
        Adds a new run to the data store.

        Args:
            id (UUID): Unique identifier for the run.
            tag (str): Tag associated with the run.

        Returns:
            None
        """

        self._current_run_id = id
        with self.session_maker() as session:
            run = DbRun(
                id=id,
                t_start=datetime.now(),
                tag=tag,
            )
            session.add(run)
            session.commit()

    def end_current_run(self) -> None:
        """
        Ends the current run by performing the following actions:

        1. Checks if there is a current run ID. If not, raises a ValueError.
        2. Queries the database for the current run and updates its end time.
        3. Aborts all TC requests associated with the current run that are not completed.
        4. Aborts all TC tasks associated with the current run that are not completed.
        5. Stops all workers associated with the current run that are not already stopped.
        6. Commits all changes to the database.
        7. Resets the current run ID to None.

        Raises:
            ValueError: If there is no current run ID or if the run with the current run ID is not found.
        """
        if self._current_run_id is not None:
            with self.session_maker() as session:
                run = (
                    session.query(DbRun)
                    .filter(DbRun.id == self._current_run_id)
                    .first()
                )
                if run is None:
                    raise ValueError(f"Run with id {self._current_run_id} not found")
                run.t_end = datetime.now()
                tc_requests = (
                    session.query(DbTCRequest)
                    .filter(
                        DbTCRequest.run_id == self.current_run_id,
                        DbTCRequest.status != TCRequestStatus.completed,
                    )
                    .all()
                )
                for req in tc_requests:
                    req.status = TCRequestStatus.aborted
                tc_tasks = (
                    session.query(DbTcTask)
                    .filter(
                        DbTcTask.run_id == self.current_run_id,
                        DbTcTask.t_completed == None,
                    )
                    .all()
                )
                for task in tc_tasks:
                    task.t_aborted = datetime.now()
                workers = (
                    session.query(DbWorker)
                    .filter(
                        DbWorker.run_id == self.current_run_id,
                        DbWorker.status != TCWorkerStatus.stopped,
                    )
                    .all()
                )
                for worker in workers:
                    worker.status = TCWorkerStatus.stopped
                    worker.t_stopped = datetime.now()

                session.commit()
                self._current_run_id = None
        else:
            raise ValueError("No current run to end")

    ################## Request methods ##################

    def add_request(
        self,
        request: TCRequest,
        duration: Optional[float] = None,
        size: Optional[int] = None,
        res_x: Optional[int] = None,
        res_y: Optional[int] = None,
    ) -> None:
        """
        Adds a new transcoding request to the current run in the data store.

        Args:
            request (TCRequest): The transcoding request to be added.
            duration (Optional[float]): The duration of the input video.
            size (Optional[int]): The size of the input video file.
            res_x (Optional[int]): The x resolution of the input video.
            res_y (Optional[int]): The y resolution of the input video.

        Raises:
            ValueError: If there is no current run to add the request to.
        """
        if self.current_run_id is None:
            raise ValueError("No current run to add request to")
        if request.t_received is None:
            request.t_received = datetime.now()
        with self.session_maker() as session:
            tc_request = DbTCRequest(
                id=request.request_id,
                input_video=request.input_video,
                output_video=request.output_video,
                bitrate=request.bitrate,
                speed=request.speed,
                t_received=request.t_received,
                status=TCRequestStatus.waiting,
                run_id=self.current_run_id,
                duration=duration,
                file_size=size,
                resolution_x=res_x,
                resolution_y=res_y,
            )
            session.add(tc_request)
            session.commit()

    def get_request(self, request_id: UUID) -> Optional[TCRequest]:
        """
        Retrieve a TCRequest object from the database based on the given request ID.

        Args:
            request_id (UUID): The unique identifier of the request to retrieve.

        Returns:
            Optional[TCRequest]: A TCRequest object if found, otherwise None.
        """
        with self.session_maker() as session:
            tc_request = (
                session.query(DbTCRequest)
                .filter(
                    DbTCRequest.id == request_id,
                    DbTCRequest.run_id == self.current_run_id,
                )
                .first()
            )
            if tc_request is None:
                return None
            else:
                return TCRequest(
                    request_id=tc_request.id,
                    input_video=tc_request.input_video,
                    output_video=tc_request.output_video,
                    bitrate=tc_request.bitrate,
                    speed=tc_request.speed,
                    status=tc_request.status,
                    run_id=tc_request.run_id,
                    t_received=tc_request.t_received,
                    duration=tc_request.duration,
                    file_size=tc_request.file_size,
                    resolution_x=tc_request.resolution_x,
                    resolution_y=tc_request.resolution_y,
                )

    def get_waiting_requests(self) -> list[TCRequest]:
        """
        Retrieve the list of waiting TCRequests from the database.

        This method queries the database for TCRequests that are associated with the
        current run ID and have a status of 'waiting'. It returns the list of TCRequest
        objects constructed from the database records.

        Returns:
            list[TCRequest]: The list of TCRequest objects that are waiting to be processed.
        """
        with self.session_maker() as session:
            tc_requests = (
                session.query(DbTCRequest)
                .filter(
                    DbTCRequest.run_id == self.current_run_id,
                    DbTCRequest.status == TCRequestStatus.waiting,
                )
                .all()
            )
            return [
                TCRequest(
                    request_id=tc_request.id,
                    input_video=tc_request.input_video,
                    output_video=tc_request.output_video,
                    bitrate=tc_request.bitrate,
                    speed=tc_request.speed,
                    status=tc_request.status,
                    run_id=tc_request.run_id,
                    t_received=tc_request.t_received,
                    duration=tc_request.duration,
                    file_size=tc_request.file_size,
                    resolution_x=tc_request.resolution_x,
                    resolution_y=tc_request.resolution_y,
                )
                for tc_request in tc_requests
            ]

    def get_active_requests(self) -> list[TCRequest]:
        """
        Retrieve the list of active TCRequests from the database.

        This method queries the database for TCRequests that are associated with the
        current run ID and have a status other than 'waiting' or 'completed'. It then
        converts the database records into TCRequest objects and returns them as a list.

        Returns:
            list[TCRequest]: The list of active TCRequest objects.
        """
        with self.session_maker() as session:
            tc_requests = (
                session.query(DbTCRequest)
                .filter(
                    DbTCRequest.run_id == self.current_run_id,
                    DbTCRequest.status != TCRequestStatus.waiting,
                    DbTCRequest.status != TCRequestStatus.completed,
                )
                .all()
            )
            return [
                TCRequest(
                    request_id=tc_request.id,
                    input_video=tc_request.input_video,
                    output_video=tc_request.output_video,
                    bitrate=tc_request.bitrate,
                    speed=tc_request.speed,
                    status=tc_request.status,
                    run_id=tc_request.run_id,
                    t_received=tc_request.t_received,
                    duration=tc_request.duration,
                    file_size=tc_request.file_size,
                    resolution_x=tc_request.resolution_x,
                    resolution_y=tc_request.resolution_y,
                )
                for tc_request in tc_requests
            ]

    def get_requests(self) -> list[UUID]:
        """
        Retrieve the list of UUIDs for the current run's TC requests.

        This method queries the database for all TC requests associated with the
        current run ID and returns their UUIDs.

        Returns:
            list[UUID]: The list of UUIDs corresponding to the TC requests for the current run.
        """
        with self.session_maker() as session:
            tc_requests = (
                session.query(DbTCRequest)
                .filter(DbTCRequest.run_id == self.current_run_id)
                .all()
            )
            return [tc_request.id for tc_request in tc_requests]

    def task_download_started(self, task_id: UUID) -> None:
        """
        Marks the start of a download task by updating the task and request status in the database.

        Args:
            task_id (UUID): The unique identifier of the task to be updated.

        Raises:
            ValueError: If the task with the given task_id is not found.
            ValueError: If the request associated with the task is not found.

        This method performs the following steps:

        1. Retrieves the task from the database using the provided task_id.
        2. If the task is not found, raises a ValueError.
        3. Retrieves the request associated with the task.
        4. If the request is not found, raises a ValueError.
        5. Updates the task's download start time to the current datetime.
        6. Updates the request's status to indicate that the file is being downloaded.
        7. Commits the changes to the database.
        """
        with self.session_maker() as session:
            tc_task = session.query(DbTcTask).filter(DbTcTask.id == task_id).first()
            if tc_task is None:
                raise ValueError(f"Task with id {task_id} not found")
            tc_request = (
                session.query(DbTCRequest)
                .filter(DbTCRequest.id == tc_task.request_id)
                .first()
            )
            if tc_request is None:
                raise ValueError(f"Request with id {tc_task.request_id} not found")
            tc_task.t_dwnld_start = datetime.now()
            tc_request.status = TCRequestStatus.downloading_file
            session.commit()

    def task_transcoding_started(self, task_id: UUID) -> None:
        """
        Marks the transcoding task as started and updates the request status.

        This method performs the following steps:

        1. Retrieves the transcoding task from the database using the provided task_id.
        2. Raises a ValueError if the task is not found.
        3. Retrieves the associated transcoding request from the database.
        4. Raises a ValueError if the request is not found.
        5. Raises a ValueError if the request status is not 'downloading_file'.
        6. Updates the task's start time to the current datetime.
        7. Updates the request status to 'transcoding'.
        8. Commits the changes to the database.

        Args:
            task_id (UUID): The unique identifier of the transcoding task.

        Raises:
            ValueError: If the task or request is not found, or if the request status is not 'downloading_file'.
        """
        with self.session_maker() as session:
            tc_task = session.query(DbTcTask).filter(DbTcTask.id == task_id).first()
            if tc_task is None:
                raise ValueError(f"Task with id {task_id} not found")

            tc_request = (
                session.query(DbTCRequest)
                .filter(DbTCRequest.id == tc_task.request_id)
                .first()
            )
            if tc_request is None:
                raise ValueError(f"Request with id {tc_task.request_id} not found")
            if tc_request.status != TCRequestStatus.downloading_file:
                raise ValueError(
                    f"Request with id {tc_request.id} is not downloading_file"
                )
            tc_task.t_tc_start = datetime.now()
            tc_request.status = TCRequestStatus.transcoding
            session.commit()

    def task_upload_started(self, task_id: UUID) -> None:
        """
        Marks the start of a task upload process by updating the task and request statuses.

        Args:
            task_id (UUID): The unique identifier of the task.

        Raises:
            ValueError: If the task with the given task_id is not found.
            ValueError: If the request associated with the task is not found.
            ValueError: If the request status is not 'transcoding'.

        Updates:
            - Sets the task's upload start time to the current datetime.
            - Changes the request status to 'uploading_file'.
        """
        with self.session_maker() as session:
            tc_task = session.query(DbTcTask).filter(DbTcTask.id == task_id).first()
            if tc_task is None:
                raise ValueError(f"Task with id {task_id} not found")
            tc_request = (
                session.query(DbTCRequest)
                .filter(DbTCRequest.id == tc_task.request_id)
                .first()
            )
            if tc_request is None:
                raise ValueError(f"Request with id {tc_task.request_id} not found")
            if tc_request.status != TCRequestStatus.transcoding:
                raise ValueError(f"Request with id {tc_request.id} is not transcoding")
            tc_task.t_upld_start = datetime.now()
            tc_request.status = TCRequestStatus.uploading_file
            session.commit()

    def task_completed(self, task_id: UUID) -> None:
        """
        Marks a task as completed and updates the corresponding request status.

        This method performs the following steps:

        1. Retrieves the task from the database using the provided task_id.
        2. Raises a ValueError if the task is not found.
        3. Retrieves the request associated with the task.
        4. Raises a ValueError if the request is not found.
        5. Raises a ValueError if the request status is not 'uploading_file'.
        6. Sets the task's completion time to the current datetime.
        7. Updates the request status to 'completed'.
        8. Commits the changes to the database.

        Args:
            task_id (UUID): The unique identifier of the task to be marked as completed.

        Raises:
            ValueError: If the task or request is not found, or if the request status is not 'uploading_file'.
        """
        with self.session_maker() as session:
            tc_task = session.query(DbTcTask).filter(DbTcTask.id == task_id).first()
            if tc_task is None:
                raise ValueError(f"Task with id {task_id} not found")
            tc_request = (
                session.query(DbTCRequest)
                .filter(DbTCRequest.id == tc_task.request_id)
                .first()
            )
            if tc_request is None:
                raise ValueError(f"Request with id {tc_task.request_id} not found")
            if tc_request.status != TCRequestStatus.uploading_file:
                raise ValueError(
                    f"Request with id {tc_request.id} is not uploading_file"
                )
            tc_task.t_completed = datetime.now()
            tc_request.status = TCRequestStatus.completed
            session.commit()

    def task_failed(self, task_id: UUID) -> None:
        """
        Marks a task as failed and updates the corresponding request status.

        Args:
            task_id (UUID): The unique identifier of the task to be marked as failed.

        Raises:
            ValueError: If the task with the given task_id is not found.
            ValueError: If the request associated with the task is not found.
        """
        with self.session_maker() as session:
            tc_task = session.query(DbTcTask).filter(DbTcTask.id == task_id).first()
            if tc_task is None:
                raise ValueError(f"Task with id {task_id} not found")
            tc_request = (
                session.query(DbTCRequest)
                .filter(DbTCRequest.id == tc_task.request_id)
                .first()
            )
            if tc_request is None:
                raise ValueError(f"Request with id {tc_task.request_id} not found")
            tc_request.status = TCRequestStatus.failed
            tc_task.t_completed = datetime.now()
            session.commit()

    def assign_request_to_worker(self, request_id: UUID, worker_id: UUID) -> UUID:
        """
        Assigns a request to a worker and updates their statuses accordingly.

        Args:
            request_id (UUID): The unique identifier of the request to be assigned.
            worker_id (UUID): The unique identifier of the worker to whom the request will be assigned.

        Returns:
            UUID: The unique identifier of the created task.

        Raises:
            ValueError: If the request with the given request_id is not found.
            ValueError: If the worker with the given worker_id is not found.
            ValueError: If the request is not in a waiting status.
            ValueError: If the worker is not in an idle status.
        """
        with self.session_maker() as session:
            tc_request = (
                session.query(DbTCRequest)
                .filter(
                    DbTCRequest.run_id == self.current_run_id,
                    DbTCRequest.id == request_id,
                )
                .first()
            )
            if tc_request is None:
                raise ValueError(f"Request with id {request_id} not found")
            worker = session.query(DbWorker).filter(DbWorker.id == worker_id).first()
            if worker is None:
                raise ValueError(f"Worker with id {worker_id} not found")

            if tc_request.status != TCRequestStatus.waiting:
                raise ValueError(f"Request with id {request_id} is not waiting")
            if worker.status != TCWorkerStatus.idle:
                raise ValueError(f"Worker with id {worker_id} is not idle")
            worker.status = TCWorkerStatus.busy

            task = DbTcTask(
                id=uuid4(),
                worker_id=worker_id,
                request_id=request_id,
                t_assigned=datetime.now(),
                run_id=self.current_run_id,
            )

            session.add(task)
            tc_request.status = TCRequestStatus.assigned
            session.commit()
            return task.id

    ################# Worker methods #################

    def add_worker(
        self,
        id: UUID,
        name: str,
        param_key: str,
        node: str,
    ) -> None:
        """
        Adds a worker to the current run in the data store.

        Args:
            id (UUID): The unique identifier for the worker.
            name (str): The name of the worker.
            param_key (str): The parameter key associated with the worker.
            node (str): The node where the worker is located.

        Raises:
            ValueError: If there is no current run to add the worker to.

        Returns:
            None
        """
        if self.current_run_id is None:
            raise ValueError("No current run to add worker to")
        with self.session_maker() as session:
            worker = DbWorker(
                id=id,
                name=name,
                param_key=param_key,
                node=node,
                status=TCWorkerStatus.booting,
                t_created=datetime.now(),
                run_id=self.current_run_id,
            )
            session.add(worker)
            session.commit()

    def get_worker(self, worker_id: UUID) -> Optional[TCWorker]:
        """
        Retrieve a worker by its unique identifier.

        Args:
            worker_id (UUID): The unique identifier of the worker.

        Returns:
            Optional[TCWorker]: A TCWorker instance if the worker is found, otherwise None.
        """
        with self.session_maker() as session:
            worker = session.query(DbWorker).filter(DbWorker.id == worker_id).first()
            if worker is None:
                return None
            return TCWorker(
                worker_id=worker.id,
                name=worker.name,
                status=worker.status,
                param_key=worker.param_key,
                t_created=worker.t_created,
                t_available=worker.t_available,
                t_idle_start=worker.t_idle_start,
                run_id=worker.run_id,
                node=worker.node,
            )

    def mark_worker_available(self, worker_id: UUID) -> None:
        """
        Marks a worker as available by updating its status and timestamps.

        Args:
            worker_id (UUID): The unique identifier of the worker to be marked as available.

        Raises:
            ValueError: If the worker with the given ID is not found.
            ValueError: If the worker's status is neither 'booting' nor 'busy'.

        Updates:
            - Sets the worker's status to 'idle'.
            - Updates the worker's `t_available` timestamp if the status was 'booting'.
            - Updates the worker's `t_idle_start` timestamp.
        """
        with self.session_maker() as session:
            worker = session.query(DbWorker).filter(DbWorker.id == worker_id).first()
            if worker is None:
                raise ValueError(f"Worker with id {worker_id} not found")
            if (
                worker.status != TCWorkerStatus.booting
                and worker.status != TCWorkerStatus.busy
            ):
                raise ValueError(
                    f"Worker with id {worker_id} is nor booting nor busy (status: {worker.status})"
                )
            if worker.status == TCWorkerStatus.booting:
                worker.t_available = datetime.now()
            worker.t_idle_start = datetime.now()
            worker.status = TCWorkerStatus.idle
            session.commit()

    def mark_worker_stopped(self, worker_id: UUID) -> None:
        """
        Marks a worker as stopped in the database.

        Args:
            worker_id (UUID): The unique identifier of the worker to be marked as stopped.

        Raises:
            ValueError: If the worker with the given ID is not found.
            ValueError: If the worker is not in the 'idle' status.

        Returns:
            None
        """
        with self.session_maker() as session:
            worker = session.query(DbWorker).filter(DbWorker.id == worker_id).first()
            if worker is None:
                raise ValueError(f"Worker with id {worker_id} not found")
            if worker.status != TCWorkerStatus.idle:
                raise ValueError(
                    f"Worker with id {worker_id} is not idle (status: {worker.status})"
                )
            worker.status = TCWorkerStatus.stopped
            worker.t_stopped = datetime.now()
            session.commit()

    def get_running_workers(self) -> list[TCWorker]:
        """
        Retrieve the list of currently running workers.

        This method queries the database for workers associated with the current run ID
        that are not in a stopped status. It returns the list of TCWorker instances
        representing these workers.

        Returns:
            list[TCWorker]: The list of TCWorker objects representing the running workers.
        """
        with self.session_maker() as session:
            workers = (
                session.query(DbWorker)
                .filter(
                    DbWorker.run_id == self.current_run_id,
                    DbWorker.status != TCWorkerStatus.stopped,
                )
                .all()
            )
            return [
                TCWorker(
                    worker_id=worker.id,
                    name=worker.name,
                    status=worker.status,
                    param_key=worker.param_key,
                    t_created=worker.t_created,
                    t_available=worker.t_available,
                    t_idle_start=worker.t_idle_start,
                    run_id=worker.run_id,
                )
                for worker in workers
            ]

    def get_workers(self) -> list[UUID]:
        """
        Retrieve the list of worker UUIDs associated with the current run.

        Returns:
            list[UUID]: The list of UUIDs for workers that are part of the current run.
        """
        with self.session_maker() as session:
            workers = (
                session.query(DbWorker)
                .filter(DbWorker.run_id == self.current_run_id)
                .all()
            )
            return [worker.id for worker in workers]

    ####################### Parameter classes #######################

    def add_scheduler_parameters(
        self, sched_params: SchedulerParameters, run_id: UUID
    ) -> None:
        """
        Adds scheduler parameters to the database.

        Args:
            sched_params (SchedulerParameters): The scheduler parameters to be added.
            run_id (UUID): The unique identifier for the run.

        Returns:
            None
        """
        with self.session_maker() as session:
            params = DbSchedulerParameters(
                id=uuid4(),
                run_id=run_id,
                runner_type=sched_params.runner_type,
                max_workers=sched_params.max_workers,
                call_interval=sched_params.call_interval,
                max_idle_time=sched_params.max_idle_time,
                algorithm=sched_params.algorithm,
                concentrate_workers=sched_params.concentrate_workers,
                spread_workers=sched_params.spread_workers,
            )
            session.add(params)
            session.commit()

    def add_vm_parameters(
        self, vm_params: dict[str, VMParameters], run_id: UUID
    ) -> None:
        """
        Adds VM parameters to the database for a given run.

        Args:
            vm_params (dict[str, VMParameters]): A dictionary where the key is a string
                and the value is an instance of VMParameters containing the VM configuration.
            run_id (UUID): The unique identifier for the run.

        Returns:
            None
        """
        with self.session_maker() as session:
            for key, value in vm_params.items():
                params = DbVmParameters(
                    id=uuid4(),
                    run_id=run_id,
                    image=value.image,
                    flavor=value.flavor,
                    key=key,
                    keypair=value.key,
                    security_group=value.security_group,
                    network=value.network,
                )
                session.add(params)
            session.commit()

    def add_pod_parameters(
        self, pod_params: dict[str, PodParameters], run_id: UUID
    ) -> None:
        """
        Adds pod parameters to the database for a given run.

        Args:
            pod_params (dict[str, PodParameters]): A dictionary where the key is a string
                representing the pod parameter key and the value is an instance of PodParameters
                containing the pod configuration.
            run_id (UUID): The unique identifier for the run to which the pod parameters belong.

        Returns:
            None
        """
        with self.session_maker() as session:
            for key, value in pod_params.items():
                params = DbPodParameters(
                    id=uuid4(),
                    run_id=run_id,
                    image=value.image,
                    key=key,
                    cpu_request=value.cpu_request,
                    memory_request=value.memory_request,
                    cpu_limit=value.cpu_limit,
                    memory_limit=value.memory_limit,
                    node_selector=value.node_selector,
                )
                session.add(params)
            session.commit()
