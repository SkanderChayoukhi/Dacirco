"""This module contains Enums that are used throughout the project.

Enums are used to define a set of named constants. Such as the runner type,, the status of a worker, the status of a request, etc."""

from enum import Enum


class RunnerType(str, Enum):
    """Enum for the runner type."""

    vm = "vm"
    pod = "pod"


class AlgorithmName(str, Enum):
    """Enum for the scheduling algorithm name."""

    minimize_workers = "minimize-workers"
    grace_period = "grace-period"
    my_scheduling_algorithm = "my-scheduling-algorithm"
    scheduler_with_reserve = "scheduler-with-reserve"
    scheduler_with_queue = "scheduler-with-queue"
    adaptive_grace_period = "adaptive-grace-period"
    priority_based_dynamic = "priority-based-dynamic"
    



class TCWorkerStatus(str, Enum):
    """Enum for the worker status."""

    booting = "booting"
    busy = "busy"
    idle = "idle"
    stopped = "stopped"


class TCRequestStatus(str, Enum):
    """Enum for the request status."""

    waiting = "waiting"
    assigned = "assigned"
    downloading_file = "downloading_file"
    transcoding = "transcoding"
    uploading_file = "uploading_file"
    completed = "completed"
    failed = "failed"
    aborted = "aborted"


class TCWorkerEventType(str, Enum):
    file_downloaded = "file_downloaded"
    transcoding_completed = "transcoding_completed"
    file_uploaded = "file_uploaded"
    keepalive = "keepalive"
    invalid = "invalid"


class TCWorkerErrorEventType(str, Enum):
    invalid = "invalid"
    transcoding_failed = "transcoding_failed"
    storage_error = "storage_error"
