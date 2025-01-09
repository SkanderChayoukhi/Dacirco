"""This module contains the Dataclasses representing most entities of the DaCirco system.

Dataclasses represent either a specific entity (e.g., a transcoding request,
worker) or a set of parameters (e.g., MinIO parameters, VM parameters) that are
passed to the system components. The dataclasses are used by the controller, the
scheduler, the gRPC server, and the workers to exchange information.

Note that the dataclasses are not stored in the database. The datastore module
converts to and from the classes used by the SQLAlchemy ORM."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID

from dacirco.enums import AlgorithmName, RunnerType, TCRequestStatus, TCWorkerStatus


@dataclass
class TCRequest:
    """
    Dataclass describing a transcoding request.

    It contains all the attributes given by the user (input video, bitrate,
    speed), as well a unique identifier that can be used as a correlation
    identifier.
    """

    input_video: str = field(metadata={"description": "The name of the input video"})
    output_video: str = field(metadata={"description": "The name of the output video"})
    bitrate: int = field(metadata={"description": "The desired bitrate"})
    speed: str = field(
        metadata={"description": "How fast should the transcoding process be"}
    )
    request_id: UUID = field(
        metadata={"description": "The identifier of the corresponding client request"}
    )
    status: Optional[TCRequestStatus] = field(
        default=None, metadata={"description": "The status of the request"}
    )
    t_received: Optional[datetime] = field(
        default=None, metadata={"description": "The time when the request was received"}
    )
    run_id: Optional[UUID] = field(default=None, metadata={"description": "The run id"})
    file_size: Optional[int] = field(
        default=None, metadata={"description": "The size of the input video"}
    )
    duration: Optional[float] = field(
        default=None,
        metadata={"description": "The duration of the input video in seconds"},
    )
    resolution_x: Optional[int] = field(
        default=None,
        metadata={"description": "The horizontal resolution of the input video"},
    )
    resolution_y: Optional[int] = field(
        default=None,
        metadata={"description": "The vertical resolution of the input video"},
    )


@dataclass
class TCWorker:
    """
    Dataclass describing a transcoding worker.
    """

    worker_id: UUID = field(
        metadata={"description": "The unique identifier of the worker"}
    )
    name: str = field(metadata={"description": "The name of the worker"})
    status: TCWorkerStatus = field(metadata={"description": "The status of the worker"})
    param_key: str = field(metadata={"description": "The key to access the worker"})
    t_created: datetime = field(
        metadata={"description": "The time when the worker was created"}
    )
    t_available: Optional[datetime] = field(
        default=None,
        metadata={
            "description": "The time when the worker became available (i.e., booting completed)"
        },
    )
    t_idle_start: Optional[datetime] = field(
        default=None,
        metadata={"description": "The *last* time when the worker became idle"},
    )
    run_id: Optional[UUID] = field(default=None, metadata={"description": "The run id"})
    node: Optional[str] = field(
        default=None, metadata={"description": "The node where the worker is running"}
    )


@dataclass
class TCTask:
    """
    Dataclass describing a transcoding task.

    A task represents a given scheduling choice: it contains the
    `TCRequest`, the ID of the worker processing the request, and a unique identifier.
    """

    request_desc: TCRequest = field(
        metadata={"description": "The request corresponding to this task"}
    )
    worker_id: UUID = field(
        metadata={"description": "The id of the worker for this task"}
    )
    task_id: UUID = field(metadata={"description": "The unique task identifier"})


@dataclass
class MinIoParameters:
    """Dataclass containing all the parameters needed to connect to MinIO."""

    server: str = field(
        metadata={"description": "The name (or IP address) of the server"}
    )
    port: int = field(metadata={"description": "The port number"})
    access_key: str = field(metadata={"description": "The access key (username)"})
    access_secret: str = field(metadata={"description": "The access secret (password)"})


@dataclass
class SchedulerParameters:
    """Dataclass containing the parameters for the scheduler"""

    algorithm: AlgorithmName = field(
        metadata={"description": "The scheduling algorithm"}
    )
    max_workers: int = field(metadata={"description": "The maximum number of workers"})
    call_interval: int = field(
        metadata={
            "description": "How often (in seconds) the server calls the periodic function of the scheduler"
        }
    )
    max_idle_time: int = field(
        metadata={
            "description": "How many seconds before a worker is destroyed (grace period algorithm)"
        }
    )
    runner_type: RunnerType = field(metadata={"description": "The runner type"})
    concentrate_workers: bool = field(
        metadata={
            "description": "If true, try to place all the workers on the same node"
        }
    )
    spread_workers: bool = field(
        metadata={"description": "If true, try to place workers on different nodes"}
    )


@dataclass
class VMParameters:
    """Dataclass containing all the parameters needed to start an OpenStack VM."""

    image: str = field(metadata={"description": "The name of the image to use"})
    flavor: str = field(metadata={"description": "The name of the flavor to use"})
    key: str = field(
        metadata={"description": "The name OpenStack key to inject into the VM"}
    )
    security_group: str = field(
        metadata={"description": "The name of the security group for the VM"}
    )
    network: str = field(metadata={"description": "The name of the network to use"})
    cpus: Optional[int] = field(
        default=None, metadata={"description": "The number of vCPUs"}
    )
    memory: Optional[int] = field(
        default=None, metadata={"description": "The amount of memory"}
    )


@dataclass
class PodParameters:
    """Dataclass containing all the parameters needed to start a Kubernetes pod."""

    image: str = field(metadata={"description": "The name of the image to use"})
    cpu_request: str = field(metadata={"description": "The vCPUs request of each pod"})
    memory_request: str = field(
        metadata={"description": "The memory request for each pod"}
    )
    cpu_limit: str = field(
        metadata={"description": "The maximum number of vCPUs of each pod"}
    )
    memory_limit: str = field(
        metadata={"description": "The maximum memory of each pod"}
    )
    node_selector: str = field(
        metadata={"description": "The name of the node where pods must run"}
    )
