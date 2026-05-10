import json
import os
import re
import tempfile
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

import demistomock as demisto
from CommonServerPython import *

from confluent_kafka import Producer


def _write_temp_file(content: Optional[str], temp_paths: list) -> Optional[str]:
    if not content:
        return None

    f = tempfile.NamedTemporaryFile(mode="w", delete=False)
    f.write(content)
    f.close()
    temp_paths.append(f.name)
    return f.name


def _cleanup_temp_files(temp_paths: list) -> None:
    for path in temp_paths:
        try:
            os.unlink(path)
        except OSError:
            pass


def _bool(value: Any) -> bool:
    return str(value).lower() == "true"


UNSAFE_TOPIC_CHAR_RE = re.compile(r"[^a-z0-9_-]+")


def sanitize_topic_segment(value: Any, fallback: str) -> str:
    cleaned = UNSAFE_TOPIC_CHAR_RE.sub("_", str(value or "").lower()).strip("_")
    return cleaned or fallback


def get_incident_field(incident: dict, field_name: str) -> Any:
    """Read an incident field, falling back to CustomFields if not at the top level."""
    if not incident:
        return None
    value = incident.get(field_name)
    if value:
        return value
    return (incident.get("CustomFields") or {}).get(field_name)


def build_topic(topic_prefix: str, incident: dict) -> str:
    account = sanitize_topic_segment(incident.get("account"), "default")
    entity = sanitize_topic_segment(get_incident_field(incident, "entity"), "unknown")
    return f"{topic_prefix.rstrip('.')}.{account}.{entity}"


CLOSE_FIELDS = ("closeReason", "closeNotes", "closingUserId", "closed")
INCIDENT_STATUS_CLOSED = 2
INCOMPLETE_PLAYBOOK_STATES = {"running", "pending"}
PENDING_REMOTE_ID_PREFIX = "xsoar-pending-"


def is_playbook_in_progress(incident: dict) -> bool:
    return bool(incident.get("playbookId")) and incident.get("runStatus") in INCOMPLETE_PLAYBOOK_STATES


def classify_event(parsed_args, delta: dict, is_first_publish: bool = False) -> str:
    closed_now = any(f in delta for f in CLOSE_FIELDS) or (
        "status" in delta and parsed_args.inc_status == INCIDENT_STATUS_CLOSED
    )
    if closed_now:
        return "incident_closed"
    if is_first_publish:
        return "incident_created"
    if delta:
        return "field_change"
    if parsed_args.entries:
        return "entry_added"
    return "incident_mirror_update"


def build_kafka_config(params: dict) -> tuple:
    brokers = params.get("brokers")
    if not brokers:
        raise DemistoException("Missing required parameter: brokers")

    use_ssl = _bool(params.get("use_ssl"))
    use_sasl = _bool(params.get("use_sasl"))
    insecure = _bool(params.get("insecure"))

    conf = {
        "bootstrap.servers": brokers,
        "client.id": "xsoar-kafka-outbound-mirror",
        "enable.idempotence": True,
        "acks": "all",
    }
    temp_paths: list = []

    if use_sasl:
        creds = params.get("credentials") or {}
        username = creds.get("identifier")
        password = creds.get("password")

        if not username or not password:
            raise DemistoException("SASL is enabled but username/password are missing.")

        conf.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": username,
                "sasl.password": password,
            }
        )

        ca_cert_path = _write_temp_file(params.get("ca_cert"), temp_paths)
        if ca_cert_path:
            conf["ssl.ca.location"] = ca_cert_path

        if insecure:
            conf["enable.ssl.certificate.verification"] = False
            conf["ssl.endpoint.identification.algorithm"] = "none"

    elif use_ssl:
        conf["security.protocol"] = "SSL"

        ca_cert_path = _write_temp_file(params.get("ca_cert"), temp_paths)
        if ca_cert_path:
            conf["ssl.ca.location"] = ca_cert_path

        client_cert_path = _write_temp_file(params.get("client_cert"), temp_paths)
        if client_cert_path:
            conf["ssl.certificate.location"] = client_cert_path

        client_key_path = _write_temp_file(params.get("client_cert_key"), temp_paths)
        if client_key_path:
            conf["ssl.key.location"] = client_key_path

        ssl_password = (params.get("ssl_key_password") or {}).get("password")
        if ssl_password:
            conf["ssl.key.password"] = ssl_password

        if insecure:
            conf["enable.ssl.certificate.verification"] = False
            conf["ssl.endpoint.identification.algorithm"] = "none"

    return conf, temp_paths


def get_producer() -> tuple:
    params = demisto.params()
    conf, temp_paths = build_kafka_config(params)
    return Producer(conf), temp_paths


def publish_to_kafka(topic: str, key: str, payload: dict) -> None:
    producer, temp_paths = get_producer()
    delivery_errors = []

    def on_delivery(err, msg):
        if err:
            delivery_errors.append(str(err))
        else:
            demisto.debug(
                f"Produced Kafka message topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
            )

    try:
        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(payload, default=str, ensure_ascii=False),
            on_delivery=on_delivery,
        )
        producer.flush(30)
    finally:
        _cleanup_temp_files(temp_paths)

    if delivery_errors:
        raise DemistoException(f"Kafka delivery failed: {delivery_errors}")


def command_test_module() -> str:
    params = demisto.params()
    topic = params.get("topic")

    if not topic:
        raise DemistoException("Missing required parameter: topic")

    producer, temp_paths = get_producer()

    try:
        # list_topics forces a real broker connection.
        producer.list_topics(timeout=10)
    finally:
        _cleanup_temp_files(temp_paths)

    return "ok"


def get_mapping_fields_command() -> GetMappingFieldsResponse:
    scheme = SchemeTypeMapping(type_name="Kafka Incident Event")

    for field_name in [
        "id",
        "name",
        "type",
        "severity",
        "status",
        "owner",
        "created",
        "modified",
        "closed",
        "closeReason",
        "closeNotes",
        "CustomFields",
        "labels",
    ]:
        scheme.add_field(name=field_name, description=f"XSOAR incident field: {field_name}")

    response = GetMappingFieldsResponse()
    response.add_scheme_type(scheme)
    return response


def update_remote_system_command(args: dict) -> str:
    """
    Called automatically by the XSOAR mirroring engine.

    XSOAR passes:
    - data: current incident data
    - delta: changed fields since last mirror
    - entries: tagged War Room entries to mirror
    - remoteId: existing remote ID, if known
    - incidentChanged: whether incident fields changed
    - status: incident status
    """
    params = demisto.params()
    topic_prefix = params.get("topic")
    include_full_incident = _bool(params.get("include_full_incident", True))
    wait_for_playbook = _bool(params.get("wait_for_playbook", True))

    if not topic_prefix:
        raise DemistoException("Missing required parameter: topic prefix")

    parsed_args = UpdateRemoteSystemArgs(args)

    incident = parsed_args.data or {}
    delta = parsed_args.delta or {}
    entries = parsed_args.entries or []

    incident_id = str(
        incident.get("id")
        or incident.get("dbotMirrorId")
        or parsed_args.remote_incident_id
        or "unknown"
    )

    is_first_publish = (
        not parsed_args.remote_incident_id
        or str(parsed_args.remote_incident_id).startswith(PENDING_REMOTE_ID_PREFIX)
    )

    # Only gate the first publish: suppress while the post-creation playbook is still
    # actively running automation. `waiting` (manual task / user input) is treated as
    # "automation done" and triggers the first publish. After the first publish, subsequent
    # mirror calls always publish regardless of playbook state.
    if wait_for_playbook and is_first_publish and is_playbook_in_progress(incident):
        demisto.debug(
            f"Suppressing Kafka publish for incident {incident_id}: "
            f"playbookId={incident.get('playbookId')} runStatus={incident.get('runStatus')}"
        )
        return parsed_args.remote_incident_id or f"{PENDING_REMOTE_ID_PREFIX}{incident_id}"

    remote_id = f"xsoar-{incident_id}"

    event_type = classify_event(parsed_args, delta, is_first_publish)

    payload = {
        "schema_version": "1.0",
        "source": "cortex-xsoar",
        "event_type": event_type,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "emitted_at_epoch_ms": int(time.time() * 1000),
        "xsoar_incident_id": incident_id,
        "mirror_remote_id": remote_id,
        "incident_changed": parsed_args.incident_changed,
        "incident_status": parsed_args.inc_status,
        "playbook_id": incident.get("playbookId"),
        "playbook_run_status": incident.get("runStatus"),
        "delta": delta,
        "entries": entries,
    }

    if include_full_incident:
        payload["incident"] = incident

    topic = build_topic(topic_prefix, incident)

    # This makes Kafka ordering sane per incident.
    publish_to_kafka(topic=topic, key=incident_id, payload=payload)

    # XSOAR expects the remote incident ID back.
    return remote_id


def main():
    try:
        command = demisto.command()
        args = demisto.args()

        demisto.debug(f"Command being called is {command}")

        if command == "test-module":
            return_results(command_test_module())

        elif command == "get-mapping-fields":
            return_results(get_mapping_fields_command())

        elif command == "update-remote-system":
            return_results(update_remote_system_command(args))

        else:
            raise NotImplementedError(f"Command {command} is not implemented.")

    except Exception as e:
        demisto.error(traceback.format_exc())
        return_error(str(e))


if __name__ in ("__main__", "__builtin__", "builtins"):
    main()
