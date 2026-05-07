import json
import tempfile
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

import demistomock as demisto
from CommonServerPython import *

from confluent_kafka import Producer


def _write_temp_file(content: Optional[str]) -> Optional[str]:
    if not content:
        return None

    f = tempfile.NamedTemporaryFile(mode="w", delete=False)
    f.write(content)
    f.close()
    return f.name


def _bool(value: Any) -> bool:
    return str(value).lower() == "true"


def build_kafka_config(params: dict) -> dict:
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

    ca_cert_path = _write_temp_file(params.get("ca_cert"))
    client_cert_path = _write_temp_file(params.get("client_cert"))
    client_key_path = _write_temp_file(params.get("client_cert_key"))

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

        if ca_cert_path:
            conf["ssl.ca.location"] = ca_cert_path

        if insecure:
            conf["enable.ssl.certificate.verification"] = False
            conf["ssl.endpoint.identification.algorithm"] = "none"

    elif use_ssl:
        conf["security.protocol"] = "SSL"

        if ca_cert_path:
            conf["ssl.ca.location"] = ca_cert_path

        if client_cert_path:
            conf["ssl.certificate.location"] = client_cert_path

        if client_key_path:
            conf["ssl.key.location"] = client_key_path

        ssl_password = (params.get("ssl_key_password") or {}).get("password")
        if ssl_password:
            conf["ssl.key.password"] = ssl_password

        if insecure:
            conf["enable.ssl.certificate.verification"] = False
            conf["ssl.endpoint.identification.algorithm"] = "none"

    return conf


def get_producer() -> Producer:
    params = demisto.params()
    conf = build_kafka_config(params)
    return Producer(conf)


def publish_to_kafka(topic: str, key: str, payload: dict) -> None:
    producer = get_producer()
    delivery_errors = []

    def on_delivery(err, msg):
        if err:
            delivery_errors.append(str(err))
        else:
            demisto.debug(
                f"Produced Kafka message topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
            )

    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(payload, default=str, ensure_ascii=False),
        on_delivery=on_delivery,
    )

    producer.flush(30)

    if delivery_errors:
        raise DemistoException(f"Kafka delivery failed: {delivery_errors}")


def command_test_module() -> str:
    params = demisto.params()
    topic = params.get("topic")

    if not topic:
        raise DemistoException("Missing required parameter: topic")

    producer = get_producer()

    # list_topics forces a real broker connection.
    producer.list_topics(timeout=10)

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
    topic = params.get("topic")
    include_full_incident = _bool(params.get("include_full_incident", True))

    if not topic:
        raise DemistoException("Missing required parameter: topic")

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

    # If this is the first outbound mirror call, XSOAR may not yet have a remote ID.
    # Kafka does not create a remote ticket, so we use a stable logical remote ID.
    remote_id = parsed_args.remote_incident_id or f"xsoar-{incident_id}"

    payload = {
        "schema_version": "1.0",
        "source": "cortex-xsoar",
        "event_type": "incident_mirror_update",
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "emitted_at_epoch_ms": int(time.time() * 1000),
        "xsoar_incident_id": incident_id,
        "mirror_remote_id": remote_id,
        "incident_changed": parsed_args.incident_changed,
        "incident_status": parsed_args.inc_status,
        "delta": delta,
        "entries": entries,
    }

    if include_full_incident:
        payload["incident"] = incident

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
