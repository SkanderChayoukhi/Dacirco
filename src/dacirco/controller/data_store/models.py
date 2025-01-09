"""
This module defines SQLAlchemy ORM models for the data store.

note:
    All the classes defined in this module start with "Db" to avoid name conflicts with the classes defined in the rest of the application.  And to highlight that they are database models.

"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from dacirco.enums import AlgorithmName, RunnerType, TCRequestStatus, TCWorkerStatus


class Base(DeclarativeBase):
    pass


class DbRun(Base):
    """
    Represents a database model for a run.

    Attributes:
        id (UUID): Primary key for the run.

        max_workers (int): Maximum number of workers.
        call_interval (int): Interval between calls.
        max_idle_time (int): Maximum idle time.
        algorithm (AlgorithmName): Name of the algorithm used.
        concentrate_workers (bool): Flag to concentrate workers.
        spread_workers (bool): Flag to spread workers.
        t_start (datetime): Start time of the run.
        t_end (Optional[datetime]): End time of the run.
        tag (Optional[str]): Tag associated with the run.
    """

    __tablename__ = "run"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    t_start: Mapped[datetime]
    t_end: Mapped[Optional[datetime]]
    tag: Mapped[Optional[str]]


class DbTCRequest(Base):
    """
    DbTCRequest is a SQLAlchemy model representing a transcoding request.

    Attributes:
        id (UUID): Primary key for the transcoding request.
        input_video (str): Path to the input video file.
        output_video (str): Path to the output video file.
        bitrate (int): Bitrate for the transcoding process.
        speed (str): Speed setting for the transcoding process.
        status (TCRequestStatus): Current status of the transcoding request.
        t_received (datetime): Timestamp when the request was received.
        file_size (Optional[int]): Size of the input video file in bytes.
        duration (Optional[int]): Duration of the input video in seconds.
        resolution_x (Optional[int]): Horizontal resolution of the input video.
        resolution_y (Optional[int]): Vertical resolution of the input video.
        run_id (UUID): Foreign key referencing the associated run.
    """

    __tablename__ = "tc_request"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    input_video: Mapped[str]
    output_video: Mapped[str]
    bitrate: Mapped[int]
    speed: Mapped[str]
    status: Mapped[TCRequestStatus]
    t_received: Mapped[datetime]
    file_size: Mapped[Optional[int]]
    duration: Mapped[Optional[float]]
    # As far as I can tell, SQLAlchemy doesn't support Tuple[int, int] directly
    resolution_x: Mapped[Optional[int]]
    resolution_y: Mapped[Optional[int]]
    run_id: Mapped[UUID] = mapped_column(ForeignKey("run.id"))

    def __repr__(self) -> str:
        return f"DbTCRequest(id={self.id!r}, input_video={self.input_video!r}, output_video={self.output_video!r}, bitrate={self.bitrate!r}, speed={self.speed!r}, t_received={self.t_received!r}"


class DbWorker(Base):
    """
    DbWorker is a SQLAlchemy model representing a worker entity in the database.

    Attributes:
        id (UUID): Primary key for the worker.
        name (str): Name of the worker.
        param_key (str): The key to access the worker parameters (in the VmParameters or PodParameters dictionary).
        node (str): The node where the worker is running (if available).
        status (TCWorkerStatus): The current status of the worker.
        t_created (datetime): Timestamp when the worker was created.
        t_available (Optional[datetime]): Timestamp when the worker became available.
        t_idle_start (Optional[datetime]): Timestamp when the worker started idling.
        t_stopped (Optional[datetime]): Timestamp when the worker was stopped.
        run_id (UUID): Foreign key referencing the associated run.
    """

    __tablename__ = "worker"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    name: Mapped[str]
    param_key: Mapped[
        str
    ]  # The key to access the worker parameters (in the VmParameters or PodParameters dictionary)
    node: Mapped[Optional[str]]  # The node where the worker is running (if available)
    status: Mapped[TCWorkerStatus]
    t_created: Mapped[datetime]
    t_available: Mapped[Optional[datetime]]
    t_idle_start: Mapped[Optional[datetime]]
    t_stopped: Mapped[Optional[datetime]]
    run_id: Mapped[UUID] = mapped_column(ForeignKey("run.id"))


class DbTcTask(Base):
    """
    Represents a task in the database for a specific worker, request, and run.

    Attributes:
        id (UUID): Primary key for the task.
        worker_id (UUID): Foreign key referencing the worker.
        request_id (UUID): Foreign key referencing the request.
        run_id (UUID): Foreign key referencing the run.
        t_assigned (Optional[datetime]): Timestamp when the task was assigned.
        t_dwnld_start (Optional[datetime]): Timestamp when the download started.
        t_tc_start (Optional[datetime]): Timestamp when the task computation started.
        t_upld_start (Optional[datetime]): Timestamp when the upload started.
        t_completed (Optional[datetime]): Timestamp when the task was completed.
        t_aborted (Optional[datetime]): Timestamp when the task was aborted.
    """

    __tablename__ = "tc_task"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    worker_id: Mapped[UUID] = mapped_column(ForeignKey("worker.id"))
    request_id: Mapped[UUID] = mapped_column(ForeignKey("tc_request.id"))
    run_id: Mapped[UUID] = mapped_column(ForeignKey("run.id"))
    t_assigned: Mapped[Optional[datetime]]
    t_dwnld_start: Mapped[Optional[datetime]]
    t_tc_start: Mapped[Optional[datetime]]
    t_upld_start: Mapped[Optional[datetime]]
    t_completed: Mapped[Optional[datetime]]
    t_aborted: Mapped[Optional[datetime]]


class DbSchedulerParameters(Base):
    """
    DbSchedulerParameters is a SQLAlchemy model representing the scheduler parameters in the database.

    Attributes:
        id (UUID): Primary key for the scheduler parameters.
        runner_type (str): Type of the runner.
        max_workers (int): Maximum number of workers.
        call_interval (int): Interval between calls.
        max_idle_time (int): Maximum idle time for workers.
        algorithm (AlgorithmName): Algorithm used for scheduling.
        concentrate_workers (bool): Flag to concentrate workers.
        spread_workers (bool): Flag to spread workers.
        run_id (UUID): Foreign key referencing the run ID.
    """

    __tablename__ = "scheduler_parameters"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    runner_type: Mapped[RunnerType]
    max_workers: Mapped[int]
    call_interval: Mapped[int]
    max_idle_time: Mapped[int]
    algorithm: Mapped[AlgorithmName]
    concentrate_workers: Mapped[bool]
    spread_workers: Mapped[bool]
    run_id: Mapped[UUID] = mapped_column(ForeignKey("run.id"))


class DbVmParameters(Base):
    """
    Represents the parameters for a virtual machine (VM) in the database.

    Attributes:
        id (UUID): The primary key identifier for the VM parameters.
        image (str): The image used for the VM.
        flavor (str): The flavor/type of the VM.
        key (str): The key associated with the VM.
        keypair (str): The keypair used for the VM.
        security_group (str): The security group associated with the VM.
        network (int): The network identifier for the VM.
        run_id (UUID): The foreign key linking to the run table.
    """

    __tablename__ = "vm_parameters"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    image: Mapped[str]
    flavor: Mapped[str]
    key: Mapped[str]
    keypair: Mapped[str]
    security_group: Mapped[str]
    network: Mapped[int]
    run_id: Mapped[UUID] = mapped_column(ForeignKey("run.id"))


class DbPodParameters(Base):
    """
    DbPodParameters is a SQLAlchemy model representing the parameters for a pod.

    Attributes:
        id (UUID): The primary key for the pod parameters.
        image (str): The image used for the pod.
        run_id (UUID): The foreign key referencing the run.
        key (str): A key associated with the pod parameters.
        node_selector (str): The node selector for the pod.
        memory_request (str): The memory request for the pod.
        memory_limit (str): The memory limit for the pod.
        cpu_request (str): The CPU request for the pod.
        cpu_limit (str): The CPU limit for the pod.
    """

    __tablename__ = "pod_parameters"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    image: Mapped[str]
    run_id: Mapped[UUID] = mapped_column(ForeignKey("run.id"))
    key: Mapped[str]
    node_selector: Mapped[str]
    memory_request: Mapped[str]
    memory_limit: Mapped[str]
    cpu_request: Mapped[str]
    cpu_limit: Mapped[str]
