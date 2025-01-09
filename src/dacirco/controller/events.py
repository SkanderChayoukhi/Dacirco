from uuid import UUID

from dacirco.enums import TCWorkerErrorEventType, TCWorkerEventType
from dacirco.proto.dacirco_pb2 import GrpcErrorEvent, GrpcEvent

"""The typing hints of the grpc objects are not easy to work with.  This is why
we have defined dedicated classes with clear type hints.
"""


class TCWorkerEvent:
    def __init__(self, grpc_event: GrpcEvent) -> None:
        self.worker_id = UUID(grpc_event.worker_id.replace("-", ""))
        if grpc_event.task_id:
            self.task_id = UUID(grpc_event.task_id.replace("-", ""))
        else:
            if grpc_event.event_type is not GrpcEvent.KEEPALIVE:
                raise ValueError("task_id is required for all events except keepalive")
        self.type = TCWorkerEventType.invalid
        if grpc_event.event_type is GrpcEvent.FILE_DOWNLOADED:
            self.type = TCWorkerEventType.file_downloaded
        elif grpc_event.event_type is GrpcEvent.TRANSCODING_COMPLETED:
            self.type = TCWorkerEventType.transcoding_completed
        elif grpc_event.event_type is GrpcEvent.FILE_UPLOADED:
            self.type = TCWorkerEventType.file_uploaded
        elif grpc_event.event_type is GrpcEvent.KEEPALIVE:
            self.type = TCWorkerEventType.keepalive


class TCWorkerErrorEvent:
    def __init__(self, grpc_error_event: GrpcErrorEvent) -> None:
        self.worker_id = UUID(grpc_error_event.worker_id.replace("-", ""))
        self.task_id = UUID(grpc_error_event.task_id.replace("-", ""))
        self.error_message = str(grpc_error_event.error_message)
        self.type = TCWorkerErrorEventType.invalid
        if grpc_error_event.error_type is GrpcErrorEvent.STORAGE_ERROR:
            self.type = TCWorkerErrorEventType.storage_error
        elif grpc_error_event.error_type is GrpcErrorEvent.TRANSCODING_FAILED:
            self.type = TCWorkerErrorEventType.transcoding_failed
