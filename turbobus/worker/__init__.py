from __future__ import annotations

_EXPORTS = {
    "CudaWorkerExecutor": (".cuda_executor", "CudaWorkerExecutor"),
    "WorkerDataPlaneCompletionEnvelope": (
        ".models",
        "WorkerDataPlaneCompletionEnvelope",
    ),
    "WorkerDataPlaneResourceBinder": (
        ".resources",
        "WorkerDataPlaneResourceBinder",
    ),
    "WorkerServiceEndpoint": (".endpoint", "WorkerServiceEndpoint"),
    "WorkerServiceRequestEnvelope": (".models", "WorkerServiceRequestEnvelope"),
    "WorkerServiceResponseEnvelope": (".models", "WorkerServiceResponseEnvelope"),
    "WorkerServiceSocketClient": (".socket_client", "WorkerServiceSocketClient"),
    "WorkerServiceUnixSocketTransport": (
        ".transport",
        "WorkerServiceUnixSocketTransport",
    ),
    "WorkerAsyncExecutionPool": (".lifecycle", "WorkerAsyncExecutionPool"),
    "WorkerAuthorizationError": (".lifecycle", "WorkerAuthorizationError"),
    "WorkerCleanupError": (".lifecycle", "WorkerCleanupError"),
    "WorkerMessageCodecError": (".codec", "WorkerMessageCodecError"),
    "WorkerStatusReportError": (".lifecycle", "WorkerStatusReportError"),
    "WorkerStagingSlot": (".staging_pool", "WorkerStagingSlot"),
    "WorkerStagingPool": (".staging_pool", "WorkerStagingPool"),
    "WorkerStagingPoolError": (".staging_pool", "WorkerStagingPoolError"),
    "WorkerStartupError": (".process", "WorkerStartupError"),
    "WorkerDataPlaneResources": (".resources", "WorkerDataPlaneResources"),
    "WorkerTransferAuthorizer": (".lifecycle", "WorkerTransferAuthorizer"),
    "WorkerTransferClient": (".lifecycle", "WorkerTransferClient"),
    "WorkerTransferCleanupCoordinator": (".lifecycle", "WorkerTransferCleanupCoordinator"),
    "WorkerTransferLifecycleRecord": (".lifecycle", "WorkerTransferLifecycleRecord"),
    "WorkerTransferRequest": (".models", "WorkerTransferRequest"),
    "WorkerTransferResult": (".models", "WorkerTransferResult"),
    "WorkerTransferService": (".lifecycle", "WorkerTransferService"),
    "WorkerTransferStatusReporter": (".lifecycle", "WorkerTransferStatusReporter"),
    "WorkerTransferState": (".models", "WorkerTransferState"),
    "decode_worker_request_envelope": (".codec", "decode_worker_request_envelope"),
    "decode_worker_response_envelope": (".codec", "decode_worker_response_envelope"),
    "encode_worker_request_envelope": (".codec", "encode_worker_request_envelope"),
    "encode_worker_response_envelope": (".codec", "encode_worker_response_envelope"),
    "execute_authorized_worker_lifecycle": (
        ".lifecycle",
        "execute_authorized_worker_lifecycle",
    ),
    "handle_worker_service_message": (".codec", "handle_worker_service_message"),
    "parse_worker_authorization_request_payload": (
        ".lifecycle",
        "parse_worker_authorization_request_payload",
    ),
    "build_worker_service_transport": (
        ".process",
        "build_worker_service_transport",
    ),
    "run_worker_service_process": (".process", "run_worker_service_process"),
    "worker_process_main": (".process", "main"),
    "worker_startup_evidence_from_daemon": (
        ".process",
        "worker_startup_evidence_from_daemon",
    ),
}


def __getattr__(name: str):
    try:
        module_name, symbol_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    from importlib import import_module

    value = getattr(import_module(module_name, __name__), symbol_name)
    globals()[name] = value
    return value

__all__ = [
    "CudaWorkerExecutor",
    "WorkerDataPlaneCompletionEnvelope",
    "WorkerDataPlaneResourceBinder",
    "WorkerServiceEndpoint",
    "WorkerServiceRequestEnvelope",
    "WorkerServiceResponseEnvelope",
    "WorkerServiceSocketClient",
    "WorkerStartupError",
    "WorkerServiceUnixSocketTransport",
    "WorkerAsyncExecutionPool",
    "WorkerAuthorizationError",
    "WorkerCleanupError",
    "WorkerMessageCodecError",
    "WorkerStatusReportError",
    "WorkerStagingSlot",
    "WorkerStagingPool",
    "WorkerStagingPoolError",
    "WorkerDataPlaneResources",
    "WorkerTransferAuthorizer",
    "WorkerTransferClient",
    "WorkerTransferCleanupCoordinator",
    "WorkerTransferLifecycleRecord",
    "WorkerTransferRequest",
    "WorkerTransferResult",
    "WorkerTransferService",
    "WorkerTransferStatusReporter",
    "WorkerTransferState",
    "build_worker_service_transport",
    "decode_worker_request_envelope",
    "decode_worker_response_envelope",
    "encode_worker_request_envelope",
    "encode_worker_response_envelope",
    "execute_authorized_worker_lifecycle",
    "handle_worker_service_message",
    "parse_worker_authorization_request_payload",
    "run_worker_service_process",
    "worker_process_main",
    "worker_startup_evidence_from_daemon",
]
