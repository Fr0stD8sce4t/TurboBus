from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..schema import (
        BufferRegistration,
        CleanupRequest,
        ExecutionTicket,
        JobIdentity,
        LeaseToken,
        PeerIdentity,
        Session,
        TransferIntent,
        TransferReservation,
        TransferStatus,
    )
    from ..scheduler import SchedulingDecision

logger = logging.getLogger(__name__)


@dataclass
class DaemonRuntimeState:
    jobs: dict[str, "JobIdentity"] = field(default_factory=dict)
    job_peer_identities: dict[str, "PeerIdentity"] = field(default_factory=dict)
    session_peer_identities: dict[str, "PeerIdentity"] = field(default_factory=dict)
    buffers: dict[str, "BufferRegistration"] = field(default_factory=dict)
    sessions: dict[str, "Session"] = field(default_factory=dict)
    reservations: dict[str, "TransferReservation"] = field(default_factory=dict)
    reservation_transfers: dict[str, str] = field(default_factory=dict)
    transfer_intents: dict[str, "TransferIntent"] = field(default_factory=dict)
    intent_transfers: dict[str, str] = field(default_factory=dict)
    transfer_queue: list[str] = field(default_factory=list)
    transfer_queue_records: dict[str, dict[str, object]] = field(default_factory=dict)
    runtime_state_version: int = 0
    transfer_plan_requests: dict[str, dict[str, object]] = field(default_factory=dict)
    transfer_plan_generations: dict[str, int] = field(default_factory=dict)
    transfer_plan_expirations: dict[str, float] = field(default_factory=dict)
    transfer_admissions: dict[str, dict[str, object]] = field(default_factory=dict)
    lease_plan_generations: dict[str, int] = field(default_factory=dict)
    transfer_plans: dict[str, dict[str, object]] = field(default_factory=dict)
    block_runtime_records: dict[str, tuple[dict[str, object], ...]] = field(
        default_factory=dict
    )
    scheduling_decisions: dict[str, "SchedulingDecision"] = field(default_factory=dict)
    execution_tickets: dict[str, "ExecutionTicket"] = field(default_factory=dict)
    transfer_tickets: dict[str, str] = field(default_factory=dict)
    transfer_completion_tickets: dict[str, "ExecutionTicket"] = field(
        default_factory=dict
    )
    lease_tokens: dict[str, "LeaseToken"] = field(default_factory=dict)
    transfer_statuses: dict[str, "TransferStatus"] = field(default_factory=dict)
    transfer_completion_sources: dict[str, str] = field(default_factory=dict)
    transfer_completion_evidence: dict[str, dict[str, object]] = field(
        default_factory=dict
    )
    transfer_buffer_snapshots: dict[str, dict[str, dict[str, object]]] = field(
        default_factory=dict
    )
    transfer_peer_identities: dict[str, "PeerIdentity"] = field(default_factory=dict)
    recent_terminal_feedback_records: dict[str, dict[str, object]] = field(
        default_factory=dict
    )
    recent_terminal_feedback_order: list[str] = field(default_factory=list)
    recent_terminal_feedback_capacity: int = 64
    transfer_receipt_archive: dict[str, dict[str, object]] = field(default_factory=dict)
    archived_intent_transfers: dict[str, str] = field(default_factory=dict)
    retired_cleanup_targets: dict[tuple[str, str], dict[str, object]] = field(
        default_factory=dict
    )
    staging_records: dict[str, dict[str, object]] = field(default_factory=dict)
    audit_records: list[dict[str, object]] = field(default_factory=list)
    connection_scoped_sessions: set[str] = field(default_factory=set)
    connection_scoped_session_connections: dict[str, str] = field(default_factory=dict)
    cleanup_events: list["CleanupRequest"] = field(default_factory=list)
    system_cleanup_events: list["CleanupRequest"] = field(default_factory=list)
    profile_cache: dict[str, dict] = field(default_factory=dict)

    def bind_to(self, daemon) -> None:
        # /*
        #  * ========================================================================
        #  * 步骤1：绑定 daemon runtime state
        #  * ========================================================================
        #  * 目标对象：TurboBusDaemon
        #  * 操作：
        #  *   1) 将集中 state 容器挂到 daemon
        #  *   2) 暴露旧私有属性作为兼容视图
        #  */
        logger.info("开始绑定 daemon runtime state...")

        # // 1.1 保留集中 state 容器
        daemon._runtime_state = self

        # // 1.2 绑定旧属性名，保证现有方法语义不变
        for public_name, value in self.__dict__.items():
            setattr(daemon, f"_{public_name}", value)
        logger.info("daemon runtime state 绑定完成")


__all__ = ["DaemonRuntimeState"]
