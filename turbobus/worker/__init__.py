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
    "WorkerServiceSocketClient": (".socket_client", "WorkerServiceSocketClient"),
    "WorkerServiceUnixSocketTransport": (
        ".transport",
        "WorkerServiceUnixSocketTransport",
    ),
    "WorkerStagingSlot": (".staging_pool", "WorkerStagingSlot"),
    "WorkerStartupError": (".process", "WorkerStartupError"),
    "WorkerTransferClient": (".lifecycle", "WorkerTransferClient"),
    "WorkerTransferRequest": (".models", "WorkerTransferRequest"),
    "WorkerTransferResult": (".models", "WorkerTransferResult"),
    "WorkerTransferService": (".lifecycle", "WorkerTransferService"),
    "WorkerTransferState": (".models", "WorkerTransferState"),
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
    "WorkerServiceSocketClient",
    "WorkerStartupError",
    "WorkerServiceUnixSocketTransport",
    "WorkerStagingSlot",
    "WorkerTransferClient",
    "WorkerTransferRequest",
    "WorkerTransferResult",
    "WorkerTransferService",
    "WorkerTransferState",
    "build_worker_service_transport",
    "run_worker_service_process",
    "worker_process_main",
    "worker_startup_evidence_from_daemon",
]
