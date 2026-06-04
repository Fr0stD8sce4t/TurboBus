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

    def cleanup(self, *args, **kwargs) -> DaemonResponse:
        return self.execution_daemon.cleanup(*args, **kwargs)

    def transfer_status(self, *args, **kwargs) -> DaemonResponse:
        return self.execution_daemon.transfer_status(*args, **kwargs)

    def validate_lease(self, *args, **kwargs) -> DaemonResponse:
        return self.execution_daemon.validate_lease(*args, **kwargs)


__all__ = ["RuntimeExecutionDaemonView"]
