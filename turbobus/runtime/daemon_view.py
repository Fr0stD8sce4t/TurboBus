from __future__ import annotations

from dataclasses import dataclass

from ..schema import DaemonResponse


@dataclass(frozen=True)
class RuntimeExecutionDaemonView:
    intent_daemon: object
    execution_daemon: object

    def wait_transfer_receipt(
        self,
        intent_id: str,
        timeout_seconds: float | None = None,
    ) -> DaemonResponse:
        return self.intent_daemon.wait_transfer_receipt(
            intent_id,
            timeout_seconds=timeout_seconds,
        )

    def submit_transfer_intent(self, *args, **kwargs) -> DaemonResponse:
        return self.intent_daemon.submit_transfer_intent(*args, **kwargs)

    def cleanup(self, *args, **kwargs) -> DaemonResponse:
        return self.execution_daemon.cleanup(*args, **kwargs)

    def transfer_status(self, *args, **kwargs) -> DaemonResponse:
        return self.execution_daemon.transfer_status(*args, **kwargs)

    def validate_lease(self, *args, **kwargs) -> DaemonResponse:
        return self.execution_daemon.validate_lease(*args, **kwargs)

    def runtime_telemetry(self) -> DaemonResponse:
        telemetry = getattr(self.execution_daemon, "runtime_telemetry", None)
        if not callable(telemetry):
            telemetry = getattr(self.intent_daemon, "runtime_telemetry", None)
        if not callable(telemetry):
            raise TypeError("daemon client must support runtime_telemetry")
        return telemetry()


__all__ = ["RuntimeExecutionDaemonView"]
