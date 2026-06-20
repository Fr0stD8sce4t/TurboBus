from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping

from ..runtime.validation import validate_runtime_receipt
from ..schema import TransferReceipt

logger = logging.getLogger(__name__)


def _unique_receipts_from_handles(handles: Iterable[object]) -> list[TransferReceipt]:
    from .handles import RuntimeSessionTransferHandle, _ReceiptTransferHandle

    receipts: list[TransferReceipt] = []
    seen = set()
    for handle in handles:
        if isinstance(handle, RuntimeSessionTransferHandle):
            handle = handle._handle
        receipt = handle._receipt if isinstance(handle, _ReceiptTransferHandle) else None
        if not isinstance(receipt, TransferReceipt):
            continue
        if receipt.receipt_id in seen:
            continue
        seen.add(receipt.receipt_id)
        receipts.append(receipt)
    return receipts


def _receipt_trace_from_receipts(receipts: Iterable[TransferReceipt]) -> dict[str, Any]:
    direct_bytes = 0
    relay_bytes = 0
    receipt_ids: list[str] = []
    intent_ids: list[str] = []
    decision_ids: list[str] = []
    topology_snapshot_ids: list[str] = []
    ticket_ids: list[str] = []
    receipt_states: list[str] = []
    completion_sources: list[str] = []
    transfer_ids: list[str] = []
    fallback_reasons: list[str] = []
    receipt_list = list(receipts)
    receipt_contracts: list[dict[str, Any]] = []
    runtime_buffer_bindings: list[dict[str, Any]] = []
    for receipt in receipt_list:
        receipt_ids.append(receipt.receipt_id)
        intent_ids.append(receipt.intent_id)
        decision_ids.append(receipt.decision_id)
        topology_snapshot_ids.append(receipt.topology_snapshot_id)
        ticket_ids.append(receipt.ticket_id)
        receipt_states.append(str(receipt.state.value))
        metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
        receipt_contracts.append(_receipt_contract_summary(receipt, metadata))
        runtime_buffer_bindings.extend(_runtime_buffer_bindings(receipt, metadata))
        completion_source = metadata.get("completion_source")
        if completion_source:
            completion_sources.append(str(completion_source))
        transfer_id = metadata.get("transfer_id")
        if transfer_id:
            transfer_ids.append(str(transfer_id))
        fallback_reason = metadata.get("fallback_reason")
        if fallback_reason:
            fallback_reasons.append(str(fallback_reason))
        for path in receipt.path_stats:
            path_bytes = int(path.get("bytes", 0) or 0)
            if str(path.get("kind", "")).lower() == "relay":
                relay_bytes += path_bytes
            else:
                direct_bytes += path_bytes
    return {
        "direct_bytes": direct_bytes,
        "relay_bytes": relay_bytes,
        "receipt_count": len(receipt_list),
        "intent_ids": join_unique(intent_ids),
        "receipt_ids": join_unique(receipt_ids),
        "decision_ids": join_unique(decision_ids),
        "topology_snapshot_ids": join_unique(topology_snapshot_ids),
        "ticket_ids": join_unique(ticket_ids),
        "receipt_states": join_unique(receipt_states),
        "completion_sources": join_unique(completion_sources),
        "transfer_ids": join_unique(transfer_ids),
        "fallback_reason": join_unique(fallback_reasons),
        "receipt_contracts": receipt_contracts,
        "runtime_buffer_bindings": runtime_buffer_bindings,
    }


def runtime_session_receipt_trace_from_receipts(
    receipts: Iterable[TransferReceipt],
    runtime_session,
    *,
    evidence_id: str,
    operation: str,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 姝ラ1锛氱敓鎴?RuntimeSession 缁戝畾 receipt trace
    #  * ========================================================================
    #  * 鏁版嵁婧愶細TransferReceipt 闆嗗悎涓?TurboBusRuntimeSession snapshot
    #  * 鎿嶄綔锛?    #  *   1) 鏍￠獙 receipt 灞炰簬褰撳墠 RuntimeSession
    #  *   2) 鐢熸垚 receipt trace 鍜?daemon recovery evidence
    #  *   3) 鍐欏叆 RuntimeSession transfer evidence record
    #  */
    logger.info("寮€濮嬬敓鎴?RuntimeSession 缁戝畾 receipt trace...")

    # // 1.1 鏍￠獙 RuntimeSession entrypoint 鑳芥彁渚涚敓浜ц竟鐣屽揩鐓?    _require_runtime_session_contract(runtime_session)

    # // 1.2 褰掍竴鍖?receipt 骞舵嫆缁濈┖ evidence
    receipt_list = list(receipts)
    if not receipt_list:
        raise RuntimeError("RuntimeSession receipt trace requires receipts")

    # // 1.3 鏍￠獙 receipt 鐢卞綋鍓?RuntimeSession 浜х敓
    _validate_runtime_session_receipts(receipt_list, runtime_session)

    # // 1.4 鐢熸垚 trace 骞剁粦瀹?daemon recovery evidence
    trace = _receipt_trace_from_receipts(receipt_list)
    recovery = _daemon_recovery_from_receipts(receipt_list, runtime_session)
    trace["daemon_recovery"] = recovery
    trace["daemon_recovery_count"] = len(recovery)
    trace["daemon_recovery_sources"] = join_unique(
        item.get("source") for item in recovery
    )

    # // 1.5 鍐欏叆 RuntimeSession transfer evidence record
    trace["runtime_entrypoint"] = _runtime_entrypoint_contract(
        runtime_session,
        receipts=receipt_list,
        evidence_id=str(evidence_id),
        operation=str(operation),
    )
    logger.info(
        "RuntimeSession 缁戝畾 receipt trace 鐢熸垚瀹屾垚, evidence_id: %s, receipts: %s",
        evidence_id,
        len(receipt_list),
    )
    return trace


def runtime_session_receipt_trace_from_handles(
    handles: Iterable[object],
    runtime_session,
    *,
    evidence_id: str,
    operation: str,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 姝ラ2锛氫粠 adapter handle 鐢熸垚 RuntimeSession receipt trace
    #  * ========================================================================
    #  * 鏁版嵁婧愶細adapter transfer handles 涓?TurboBusRuntimeSession
    #  * 鎿嶄綔锛?    #  *   1) 鎻愬彇鍞竴鐪熷疄 TransferReceipt
    #  *   2) 澶嶇敤 RuntimeSession 缁戝畾 trace 鍏ュ彛
    #  */
    logger.info("寮€濮嬩粠 adapter handle 鐢熸垚 RuntimeSession receipt trace...")

    # // 2.1 褰掍竴鍖?handle 闆嗗悎骞舵嫆缁濈┖杈撳叆
    handle_list = list(handles)
    if not handle_list:
        raise RuntimeError("RuntimeSession receipt trace requires handles")

    # // 2.2 鎻愬彇鍞竴 receipt 骞剁粦瀹?RuntimeSession trace
    receipts = _unique_receipts_from_handles(handle_list)
    if not receipts:
        raise RuntimeError("RuntimeSession receipt trace requires TransferReceipt")
    trace = runtime_session_receipt_trace_from_receipts(
        receipts,
        runtime_session,
        evidence_id=evidence_id,
        operation=operation,
    )
    logger.info(
        "adapter handle RuntimeSession receipt trace 鐢熸垚瀹屾垚, evidence_id: %s",
        evidence_id,
    )
    return trace


def transfer_lifecycle_evidence_from_handles(
    *,
    evidence_id: str,
    operation: str,
    transfer_context,
    item_field: str,
    item_count_field: str,
    item_names: Iterable[str],
    handles: Iterable[object],
    transfer_stats: Mapping[str, Any],
    runtime_session,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 姝ラ1锛氱敓鎴?RuntimeSession 缁戝畾 transfer 璇佹嵁
    #  * ========================================================================
    #  * 鏁版嵁婧愶細transfer handles 涓?TurboBusRuntimeSession snapshot
    #  * 鎿嶄綔锛?    #  *   1) 浠?handle 鎻愬彇鐪熷疄 TransferReceipt
    #  *   2) 鐢熸垚 RuntimeSession entrypoint 鍚堢害
    #  *   3) 鎷掔粷璋冪敤鏂硅鐩栨牳蹇冪敓浜ц竟鐣屽瓧娈?    #  */
    logger.info("寮€濮嬬敓鎴?RuntimeSession 缁戝畾 transfer 璇佹嵁...")

    # // 1.1 褰掍竴鍖?adapter item 涓?handle
    names = tuple(str(name) for name in item_names)
    handle_list = list(handles)

    # // 1.2 鎻愬彇鍞竴 TransferReceipt 璇佹嵁
    receipts = _unique_receipts_from_handles(handle_list)
    if names and not receipts:
        raise RuntimeError(
            f"{operation} completed without TransferReceipt evidence"
        )

    # // 1.3 鏍￠獙 RuntimeSession 鍏ュ彛骞剁敓鎴?receipt trace
    trace = runtime_session_receipt_trace_from_receipts(
        receipts,
        runtime_session,
        evidence_id=str(evidence_id),
        operation=str(operation),
    )
    runtime_entrypoint = trace["runtime_entrypoint"]
    extra_payload = _transfer_extra_without_contract_overrides(extra)
    evidence = {
        "evidence_id": str(evidence_id),
        "operation": str(operation),
        "job_id": transfer_context.job_id,
        "session_id": transfer_context.session_id,
        "workload_kind": str(transfer_context.workload_kind.value),
        "buffer_registration_source": "TurboBusRuntimeSession",
        "intent_source": "TransferIntent",
        "receipt_source": "TransferReceipt",
        "policy_source": "daemon_scheduler",
        "route_policy_visible_to_transfer": False,
        "physical_route_source": "daemon_scheduler",
        "daemon_recovery_source": "TurboBusRuntimeSession",
        "cpu_buffer_id": transfer_context.cpu_buffer_id,
        "gpu_buffer_id": transfer_context.gpu_buffer_id,
        item_field: names,
        item_count_field: len(names),
        "runtime_entrypoint": runtime_entrypoint,
        **trace,
        **dict(transfer_stats),
        **extra_payload,
    }
    logger.info(
        "RuntimeSession 缁戝畾 transfer 璇佹嵁鐢熸垚瀹屾垚, evidence_id: %s, receipts: %s",
        evidence_id,
        len(receipts),
    )
    return evidence


def _transfer_extra_without_contract_overrides(
    extra: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if extra is None:
        return {}
    payload = dict(extra)
    protected = {
        "buffer_registration_source",
        "intent_source",
        "receipt_source",
        "policy_source",
        "physical_route_source",
        "route_policy_visible_to_transfer",
        "daemon_recovery_source",
        "runtime_entrypoint",
        "receipt_contracts",
    }
    overridden = sorted(key for key in payload if key in protected)
    if overridden:
        raise ValueError(
            "transfer lifecycle extra must not override production boundary fields: "
            + ", ".join(overridden)
        )
    return payload


def join_unique(values: Iterable[object]) -> str:
    seen = set()
    ordered = []
    for value in values:
        if value is None:
            continue
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ",".join(ordered)


def _daemon_recovery_from_receipts(
    receipts: Iterable[TransferReceipt],
    runtime_session,
) -> list[dict[str, Any]]:
    # /*
    #  * ========================================================================
    #  * 姝ラ3锛氭瀯閫?RuntimeSession recovery 鎽樿
    #  * ========================================================================
    #  * 鏁版嵁婧愶細TransferReceipt metadata 涓?TurboBusRuntimeSession recovery API
    #  * 鎿嶄綔锛?    #  *   1) 閫氳繃 RuntimeSession 涓诲姩鎭㈠ transfer 鐘舵€?    #  *   2) 鍙繑鍥?adapter 鍙秷璐圭殑鏍囬噺鎽樿锛屼笉鍏紑 ticket/lease/queue 鏄庣粏
    #  */
    logger.info("寮€濮嬫瀯閫?RuntimeSession recovery 鎽樿...")

    # // 3.1 璇诲彇 RuntimeSession recovery 鍏ュ彛
    recover = getattr(runtime_session, "recover_transfer_state", None)
    if not callable(recover):
        logger.info("RuntimeSession recovery 鎽樿鏋勯€犲畬鎴? recovered: %s", 0)
        return []

    # // 3.2 閫愪釜 receipt 瑙﹀彂 RuntimeSession recovery
    recovered: list[dict[str, Any]] = []
    for receipt in receipts:
        metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
        transfer_id = metadata.get("transfer_id")
        if transfer_id is None:
            continue
        recovery = recover(
            intent_id=receipt.intent_id,
            transfer_id=str(transfer_id),
        )
        recovered.append(
            {
                "source": recovery.get("source"),
                "state": recovery.get("state"),
                "archived": bool(recovery.get("archived", False)),
                "receipt_recorded": isinstance(recovery.get("receipt"), Mapping),
                "route_policy_visible_to_transfer": False,
            }
        )
    logger.info(
        "RuntimeSession recovery 鎽樿鏋勯€犲畬鎴? recovered: %s",
        len(recovered),
    )
    return recovered


def _runtime_entrypoint_contract(
    runtime_session,
    *,
    receipts: Iterable[TransferReceipt],
    evidence_id: str | None = None,
    operation: str | None = None,
) -> dict[str, Any]:
    # /*
    #  * ========================================================================
    #  * 姝ラ2锛氭瀯閫?RuntimeSession 鍏ュ彛鍚堢害
    #  * ========================================================================
    #  * 鏁版嵁婧愶細TurboBusRuntimeSession.runtime_entrypoint_snapshot
    #  * 鎿嶄綔锛?    #  *   1) 璇诲彇 RuntimeSession snapshot
    #  *   2) 鏍稿 adapter receipt 涓?intent 宸茶繘鍏?entrypoint record
    #  *   3) 鍥炲啓 transfer evidence 缁戝畾蹇収
    #  */
    logger.info("寮€濮嬫瀯閫?RuntimeSession 鍏ュ彛鍚堢害...")

    # // 2.1 纭 runtime_session 鏆撮湶鐢熶骇鍏ュ彛蹇収
    _require_runtime_session_contract(runtime_session)
    receipt_list = list(receipts)
    if receipt_list:
        _validate_runtime_session_receipts(receipt_list, runtime_session)
    snapshotter = getattr(runtime_session, "runtime_entrypoint_snapshot", None)

    # // 2.2 璇诲彇褰撳墠 RuntimeSession snapshot
    snapshot = snapshotter()
    if not isinstance(snapshot, Mapping):
        raise TypeError("runtime entrypoint snapshot must be a mapping")

    # // 2.3 鎻愬彇 receipt 涓?intent 缁戝畾
    receipt_ids = [receipt.receipt_id for receipt in receipt_list]
    intent_ids = [receipt.intent_id for receipt in receipt_list]
    receipts_view = snapshot.get("receipts")
    intents_view = snapshot.get("intents")
    transfer_contexts_view = snapshot.get("transfer_contexts")
    contexts_view = transfer_contexts_view
    contract = {
        "schema": snapshot.get("schema"),
        "entrypoint": snapshot.get("entrypoint"),
        "job_id": snapshot.get("job_id"),
        "session_id": snapshot.get("session", {}).get("session_id")
        if isinstance(snapshot.get("session"), Mapping)
        else None,
        "plan_source": snapshot.get("plan_source"),
        "route_policy_visible_to_application": bool(
            snapshot.get("route_policy_visible_to_application", True)
        ),
        "route_policy_visible_to_transfer": bool(
            snapshot.get("route_policy_visible_to_transfer", True)
        ),
        "receipt_ids": receipt_ids,
        "intent_ids": intent_ids,
        "receipts_recorded": _snapshot_receipts_contain_all(receipts_view, receipt_ids),
        "intents_recorded": _snapshot_contains_all(intents_view, intent_ids),
        "transfer_context_recorded": _snapshot_transfer_context_for_receipts(
            contexts_view,
            receipt_list,
        ),
    }
    if evidence_id is not None:
        _record_runtime_transfer_evidence(
            runtime_session,
            evidence_id=str(evidence_id),
            operation=str(operation or ""),
            intent_ids=intent_ids,
            receipt_ids=receipt_ids,
        )
        refreshed = snapshotter()
        if isinstance(refreshed, Mapping):
            transfer_evidence = refreshed.get("transfer_evidence")
            evidence_view = transfer_evidence
            contract["transfer_evidence_recorded"] = _snapshot_contains_all(
                evidence_view,
                [str(evidence_id)],
            )
            contract["transfer_evidence_record"] = _snapshot_transfer_evidence_record(
                evidence_view,
                evidence_id=str(evidence_id),
            )
    logger.info(
        "RuntimeSession 鍏ュ彛鍚堢害鏋勯€犲畬鎴? receipts: %s",
        len(receipt_ids),
    )
    return contract


def _require_runtime_session_contract(runtime_session) -> None:
    snapshotter = getattr(runtime_session, "runtime_entrypoint_snapshot", None)
    if not callable(snapshotter):
        raise TypeError(
            "adapter lifecycle evidence requires TurboBusRuntimeSession "
            "entrypoint snapshots"
        )


def _validate_runtime_session_receipts(
    receipts: Iterable[TransferReceipt],
    runtime_session,
) -> None:
    # /*
    #  * ========================================================================
    #  * 姝ラ3锛氭牎楠?RuntimeSession receipt 褰掑睘
    #  * ========================================================================
    #  * 鏁版嵁婧愶細TransferReceipt 涓?TurboBusRuntimeSession 鏍囪瘑
    #  * 鎿嶄綔锛?    #  *   1) 璇诲彇 RuntimeSession job_id/session_id
    #  *   2) 瑕佹眰 receipt 鏄綋鍓?RuntimeSession 鐨勭湡瀹炴墽琛?evidence
    #  */
    logger.info("寮€濮嬫牎楠?RuntimeSession receipt 褰掑睘...")

    # // 3.1 璇诲彇 RuntimeSession 鏍囪瘑
    job_id = getattr(runtime_session, "job_id", None)
    session_id = getattr(runtime_session, "session_id", None)
    if job_id is None or session_id is None:
        raise TypeError("RuntimeSession receipt trace requires job_id and session_id")

    # // 3.2 閫愪釜鏍￠獙 receipt 褰掑睘涓庣湡瀹炴墽琛?evidence
    count = 0
    for receipt in receipts:
        if not isinstance(receipt, TransferReceipt):
            raise TypeError("RuntimeSession receipt trace requires TransferReceipt")
        validate_runtime_receipt(
            receipt,
            intent_id=receipt.intent_id,
            job_id=str(job_id),
            session_id=str(session_id),
        )
        count += 1
    logger.info("RuntimeSession receipt 褰掑睘鏍￠獙瀹屾垚, receipts: %s", count)


def _record_runtime_transfer_evidence(
    runtime_session,
    *,
    evidence_id: str,
    operation: str,
    intent_ids: Iterable[str],
    receipt_ids: Iterable[str],
) -> None:
    recorder = getattr(runtime_session, "record_transfer_lifecycle_evidence", None)
    if not callable(recorder):
        raise TypeError(
            "transfer lifecycle evidence requires TurboBusRuntimeSession "
            "transfer evidence recording"
        )
    recorder(
        evidence_id=evidence_id,
        operation=operation,
        intent_ids=tuple(str(intent_id) for intent_id in intent_ids),
        receipt_ids=tuple(str(receipt_id) for receipt_id in receipt_ids),
    )


def _snapshot_contains_all(value: object, keys: Iterable[str]) -> bool:
    if not isinstance(value, Mapping):
        return False
    return all(str(key) in value for key in keys)


def _snapshot_transfer_evidence_record(
    value: object,
    *,
    evidence_id: str,
) -> dict[str, object] | None:
    # /*
    #  * ========================================================================
    #  * ??4??? transfer evidence ??
    #  * ========================================================================
    #  * ???? RuntimeSession entrypoint snapshot ????? evidence
    #  * ????RuntimeSession transfer evidence records
    #  * ???
    #  *   1) ? evidence_id ????
    #  *   2) ??? intent/receipt ??????
    #  */
    logger.info("???? transfer evidence ??...")

    # // 4.1 ?? transfer evidence records ??
    if not isinstance(value, Mapping):
        logger.info("transfer evidence ??????, found: %s", False)
        return None

    # // 4.2 ? evidence_id ????
    record = value.get(str(evidence_id))
    if not isinstance(record, Mapping):
        logger.info("transfer evidence ??????, found: %s", False)
        return None

    # // 4.3 ??????
    summary = {
        "evidence_id": str(record.get("evidence_id", evidence_id)),
        "operation": str(record.get("operation", "")),
        "intent_ids": [
            str(intent_id)
            for intent_id in _sequence_or_empty(record.get("intent_ids"))
        ],
        "receipt_ids": [
            str(receipt_id)
            for receipt_id in _sequence_or_empty(record.get("receipt_ids"))
        ],
        "intents_recorded": bool(record.get("intents_recorded", False)),
        "receipts_recorded": bool(record.get("receipts_recorded", False)),
    }
    logger.info("transfer evidence ??????, found: %s", True)
    return summary


def _sequence_or_empty(value: object) -> tuple[object, ...]:
    # /*
    #  * ========================================================================
    #  * ??5???? evidence ????
    #  * ========================================================================
    #  * ???? RuntimeSession transfer evidence ???? tuple
    #  * ????receipt / intent / ticket id ??
    #  * ???
    #  *   1) ?????????
    #  *   2) ??????? tuple
    #  */
    logger.info("????? evidence ????...")

    # // 5.1 ?????????
    if isinstance(value, str):
        result = (value,)
        logger.info("evidence ?????????, count: %s", len(result))
        return result

    # // 5.2 ??????? tuple
    if isinstance(value, Iterable):
        result = tuple(value)
        logger.info("evidence ?????????, count: %s", len(result))
        return result

    # // 5.3 ??????? tuple
    logger.info("evidence ?????????, count: %s", 0)
    return ()


def _snapshot_receipts_contain_all(value: object, receipt_ids: Iterable[str]) -> bool:
    if not isinstance(value, Mapping):
        return False
    observed = {
        str(item.get("receipt_id"))
        for item in value.values()
        if isinstance(item, Mapping)
    }
    return all(str(receipt_id) in observed for receipt_id in receipt_ids)


def _snapshot_transfer_context_for_receipts(
    value: object,
    receipts: Iterable[TransferReceipt],
) -> bool:
    if not isinstance(value, Mapping):
        return False
    contexts = [item for item in value.values() if isinstance(item, Mapping)]
    if not contexts:
        return False
    receipt_list = list(receipts)
    if not receipt_list:
        return False
    for receipt in receipt_list:
        metadata = receipt.metadata if isinstance(receipt.metadata, Mapping) else {}
        lifetime = metadata.get("buffer_lifetime_evidence")
        if not isinstance(lifetime, Mapping):
            return False
        source_id = _runtime_buffer_id_from_lifetime(lifetime.get("source_buffer"))
        destination_id = _runtime_buffer_id_from_lifetime(
            lifetime.get("destination_buffer")
        )
        if not _contexts_include_buffer_pair(contexts, source_id, destination_id):
            return False
    return True


def _runtime_buffer_id_from_lifetime(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    buffer_id = value.get("buffer_id")
    return None if buffer_id is None else str(buffer_id)


def _contexts_include_buffer_pair(
    contexts: Iterable[Mapping[str, object]],
    source_id: str | None,
    destination_id: str | None,
) -> bool:
    if source_id is None or destination_id is None:
        return False
    for context in contexts:
        cpu_buffer_id = str(context.get("cpu_buffer_id"))
        gpu_buffer_id = str(context.get("gpu_buffer_id"))
        pair = {cpu_buffer_id, gpu_buffer_id}
        if source_id in pair and destination_id in pair:
            return True
    return False


def _receipt_contract_summary(
    receipt: TransferReceipt,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    completion_contract = metadata.get("completion_contract")
    cuda_ipc_lifecycle = metadata.get("cuda_ipc_lifecycle")
    return {
        "receipt_id": receipt.receipt_id,
        "intent_id": receipt.intent_id,
        "decision_id": receipt.decision_id,
        "topology_snapshot_id": receipt.topology_snapshot_id,
        "ticket_id": receipt.ticket_id,
        "job_id": receipt.job_id,
        "session_id": receipt.session_id,
        "state": str(receipt.state.value),
        "bytes_total": int(receipt.bytes_total),
        "bytes_completed": int(receipt.bytes_completed),
        "completion_source": metadata.get("completion_source"),
        "transfer_id": metadata.get("transfer_id"),
        "verified": bool(metadata.get("verified", False)),
        "verified_bytes": int(metadata.get("verified_bytes", 0) or 0),
        "completion_contract": (
            dict(completion_contract)
            if isinstance(completion_contract, Mapping)
            else None
        ),
        "cuda_ipc_lifecycle": (
            dict(cuda_ipc_lifecycle)
            if isinstance(cuda_ipc_lifecycle, Mapping)
            else None
        ),
    }


def _runtime_buffer_bindings(
    receipt: TransferReceipt,
    metadata: Mapping[str, Any],
) -> list[dict[str, Any]]:
    lifetime = metadata.get("buffer_lifetime_evidence")
    if not isinstance(lifetime, Mapping):
        return []
    bindings: list[dict[str, Any]] = []
    for role, key in (
        ("source", "source_buffer"),
        ("destination", "destination_buffer"),
    ):
        record = lifetime.get(key)
        if not isinstance(record, Mapping):
            continue
        registration = record.get("registration")
        registration_mapping = (
            dict(registration) if isinstance(registration, Mapping) else {}
        )
        registration_metadata = registration_mapping.get("metadata")
        bindings.append(
            {
                "role": role,
                "buffer_id": record.get("buffer_id"),
                "handle_type": registration_mapping.get("handle_type"),
                "runtime_buffer_kind": record.get("runtime_buffer_kind"),
                "runtime_session_id": record.get("runtime_session_id"),
                "runtime_owned": bool(record.get("runtime_owned", False)),
                "registration_metadata": (
                    dict(registration_metadata)
                    if isinstance(registration_metadata, Mapping)
                    else None
                ),
                "resource_evidence": (
                    dict(record["resource_evidence"])
                    if isinstance(record.get("resource_evidence"), Mapping)
                    else None
                ),
                "cuda_ipc_lifecycle": (
                    dict(record["cuda_ipc_lifecycle"])
                    if isinstance(record.get("cuda_ipc_lifecycle"), Mapping)
                    else None
                ),
            }
        )
    return bindings


__all__ = [
    "transfer_lifecycle_evidence_from_handles",
]
