# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

from dacirco.proto import dacirco_pb2 as dacirco_dot_proto_dot_dacirco__pb2

GRPC_GENERATED_VERSION = '1.65.1'
GRPC_VERSION = grpc.__version__
EXPECTED_ERROR_RELEASE = '1.66.0'
SCHEDULED_RELEASE_DATE = 'August 6, 2024'
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    warnings.warn(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + f' but the generated code in dacirco/proto/dacirco_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
        + f' This warning will become an error in {EXPECTED_ERROR_RELEASE},'
        + f' scheduled for release on {SCHEDULED_RELEASE_DATE}.',
        RuntimeWarning
    )


class DaCircogRPCServiceStub(object):
    """*
    The gRPC interface of the DaCirco controller
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.submit_request = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/submit_request',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequest.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequestReply.FromString,
                _registered_method=True)
        self.get_requests = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/get_requests',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcEmpty.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestIDList.FromString,
                _registered_method=True)
        self.get_request = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/get_request',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestID.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequest.FromString,
                _registered_method=True)
        self.get_request_status = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/get_request_status',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestID.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequestStatus.FromString,
                _registered_method=True)
        self.get_workers = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/get_workers',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcEmpty.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerIDList.FromString,
                _registered_method=True)
        self.get_worker = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/get_worker',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerID.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerFullDesc.FromString,
                _registered_method=True)
        self.get_worker_status = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/get_worker_status',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerID.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerState.FromString,
                _registered_method=True)
        self.register_worker = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/register_worker',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerDesc.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.FromString,
                _registered_method=True)
        self.get_tasks = channel.unary_stream(
                '/dacirco_grpc_service.DaCircogRPCService/get_tasks',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerDesc.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCTask.FromString,
                _registered_method=True)
        self.submit_event = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/submit_event',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcEvent.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.FromString,
                _registered_method=True)
        self.submit_error = channel.unary_unary(
                '/dacirco_grpc_service.DaCircogRPCService/submit_error',
                request_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcErrorEvent.SerializeToString,
                response_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.FromString,
                _registered_method=True)


class DaCircogRPCServiceServicer(object):
    """*
    The gRPC interface of the DaCirco controller
    """

    def submit_request(self, request, context):
        """/ The REST frontend calls this method when it receives a new request.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_requests(self, request, context):
        """/ The REST frontend calls this method to answer a GET /jobs request.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_request(self, request, context):
        """/ The REST frontend calls this method to answer a GET /jobs/job_id request.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_request_status(self, request, context):
        """/ The REST frontend calls this method to answer a GET /jobs/job_id/state request.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_workers(self, request, context):
        """/ The REST frontend calls this method to answer a GET /workers request.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_worker(self, request, context):
        """/ The REST frontend calls this method to answer a GET /workers/worker_id request.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_worker_status(self, request, context):
        """/ The REST frontend calls this method to answer a GET /workers/worker_id/state request.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def register_worker(self, request, context):
        """/ Each transcoding worker calls this method whenever it *first* starts
        (i.e., one call only from each worker).
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_tasks(self, request, context):
        """/ Transcoding workers call this method to get their tasks.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def submit_event(self, request, context):
        """/ Transcoding workers call this method to inform the controller about a (non-error) event.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def submit_error(self, request, context):
        """/ Transcoding workers call this method to inform the controller about a (non-error) event.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DaCircogRPCServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'submit_request': grpc.unary_unary_rpc_method_handler(
                    servicer.submit_request,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequest.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequestReply.SerializeToString,
            ),
            'get_requests': grpc.unary_unary_rpc_method_handler(
                    servicer.get_requests,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcEmpty.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestIDList.SerializeToString,
            ),
            'get_request': grpc.unary_unary_rpc_method_handler(
                    servicer.get_request,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestID.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequest.SerializeToString,
            ),
            'get_request_status': grpc.unary_unary_rpc_method_handler(
                    servicer.get_request_status,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestID.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequestStatus.SerializeToString,
            ),
            'get_workers': grpc.unary_unary_rpc_method_handler(
                    servicer.get_workers,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcEmpty.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerIDList.SerializeToString,
            ),
            'get_worker': grpc.unary_unary_rpc_method_handler(
                    servicer.get_worker,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerID.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerFullDesc.SerializeToString,
            ),
            'get_worker_status': grpc.unary_unary_rpc_method_handler(
                    servicer.get_worker_status,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerID.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerState.SerializeToString,
            ),
            'register_worker': grpc.unary_unary_rpc_method_handler(
                    servicer.register_worker,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerDesc.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.SerializeToString,
            ),
            'get_tasks': grpc.unary_stream_rpc_method_handler(
                    servicer.get_tasks,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerDesc.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcTCTask.SerializeToString,
            ),
            'submit_event': grpc.unary_unary_rpc_method_handler(
                    servicer.submit_event,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcEvent.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.SerializeToString,
            ),
            'submit_error': grpc.unary_unary_rpc_method_handler(
                    servicer.submit_error,
                    request_deserializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcErrorEvent.FromString,
                    response_serializer=dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'dacirco_grpc_service.DaCircogRPCService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('dacirco_grpc_service.DaCircogRPCService', rpc_method_handlers)


 # This class is part of an EXPERIMENTAL API.
class DaCircogRPCService(object):
    """*
    The gRPC interface of the DaCirco controller
    """

    @staticmethod
    def submit_request(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/submit_request',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequest.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequestReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def get_requests(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/get_requests',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcEmpty.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestIDList.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def get_request(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/get_request',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestID.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequest.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def get_request_status(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/get_request_status',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcRequestID.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcTCRequestStatus.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def get_workers(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/get_workers',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcEmpty.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerIDList.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def get_worker(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/get_worker',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerID.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerFullDesc.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def get_worker_status(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/get_worker_status',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerID.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerState.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def register_worker(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/register_worker',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerDesc.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def get_tasks(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/get_tasks',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcWorkerDesc.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcTCTask.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def submit_event(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/submit_event',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcEvent.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def submit_error(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/dacirco_grpc_service.DaCircogRPCService/submit_error',
            dacirco_dot_proto_dot_dacirco__pb2.GrpcErrorEvent.SerializeToString,
            dacirco_dot_proto_dot_dacirco__pb2.GrpcServiceReply.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
