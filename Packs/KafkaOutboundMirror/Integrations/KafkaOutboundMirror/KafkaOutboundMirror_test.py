import importlib
import sys
import traceback
import types

import pytest


class DemistoException(Exception):
    pass


class FakeProducer:
    def __init__(self, conf):
        self.conf = conf


class UpdateRemoteSystemArgs:
    def __init__(self, args):
        self.data = args.get("data")
        self.delta = args.get("delta")
        self.entries = args.get("entries")
        self.remote_incident_id = args.get("remoteId")
        self.incident_changed = args.get("incidentChanged")
        self.inc_status = args.get("status")


class SchemeTypeMapping:
    def __init__(self, type_name):
        self.type_name = type_name
        self.fields = []

    def add_field(self, name, description):
        self.fields.append({"name": name, "description": description})


class GetMappingFieldsResponse:
    def __init__(self):
        self.scheme_types = []

    def add_scheme_type(self, scheme):
        self.scheme_types.append(scheme)


@pytest.fixture
def kafka_module(monkeypatch):
    demisto = types.SimpleNamespace(
        params=lambda: {},
        args=lambda: {},
        command=lambda: "",
        debug=lambda *_args, **_kwargs: None,
        error=lambda *_args, **_kwargs: None,
    )

    common = types.ModuleType("CommonServerPython")
    common.DemistoException = DemistoException
    common.UpdateRemoteSystemArgs = UpdateRemoteSystemArgs
    common.SchemeTypeMapping = SchemeTypeMapping
    common.GetMappingFieldsResponse = GetMappingFieldsResponse
    common.return_results = lambda result: result
    common.return_error = lambda result: result
    common.traceback = traceback

    confluent = types.ModuleType("confluent_kafka")
    confluent.Producer = FakeProducer

    monkeypatch.setitem(sys.modules, "demistomock", demisto)
    monkeypatch.setitem(sys.modules, "CommonServerPython", common)
    monkeypatch.setitem(sys.modules, "confluent_kafka", confluent)
    sys.modules.pop("KafkaOutboundMirror", None)

    return importlib.import_module("KafkaOutboundMirror")


def test_build_kafka_config_requires_brokers(kafka_module):
    with pytest.raises(DemistoException, match="brokers"):
        kafka_module.build_kafka_config({})


def test_build_kafka_config_sets_plain_broker_defaults(kafka_module):
    conf, temp_paths = kafka_module.build_kafka_config({"brokers": "broker1:9092,broker2:9092"})

    assert conf == {
        "bootstrap.servers": "broker1:9092,broker2:9092",
        "client.id": "xsoar-kafka-outbound-mirror",
        "enable.idempotence": True,
        "acks": "all",
    }
    assert temp_paths == []


def test_build_kafka_config_sets_sasl_ssl(kafka_module, monkeypatch):
    def fake_write(content, temp_paths):
        if not content:
            return None
        path = f"/tmp/{content}"
        temp_paths.append(path)
        return path

    monkeypatch.setattr(kafka_module, "_write_temp_file", fake_write)

    conf, temp_paths = kafka_module.build_kafka_config(
        {
            "brokers": "broker1:9092",
            "use_sasl": "true",
            "insecure": "true",
            "ca_cert": "ca.pem",
            "credentials": {"identifier": "user", "password": "pass"},
        }
    )

    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "PLAIN"
    assert conf["sasl.username"] == "user"
    assert conf["sasl.password"] == "pass"
    assert conf["ssl.ca.location"] == "/tmp/ca.pem"
    assert conf["enable.ssl.certificate.verification"] is False
    assert conf["ssl.endpoint.identification.algorithm"] == "none"
    assert temp_paths == ["/tmp/ca.pem"]


def test_update_remote_system_publishes_payload_and_returns_stable_remote_id(kafka_module, monkeypatch):
    published = {}

    monkeypatch.setattr(
        kafka_module.demisto,
        "params",
        lambda: {"topic": "xsoar.incident.events", "include_full_incident": "true"},
    )
    monkeypatch.setattr(kafka_module.time, "time", lambda: 1234.567)
    monkeypatch.setattr(
        kafka_module,
        "publish_to_kafka",
        lambda topic, key, payload: published.update({"topic": topic, "key": key, "payload": payload}),
    )

    remote_id = kafka_module.update_remote_system_command(
        {
            "data": {"id": "12345", "name": "Example Incident"},
            "delta": {"severity": 3},
            "entries": [{"id": "entry-1"}],
            "incidentChanged": True,
            "status": 1,
        }
    )

    assert remote_id == "xsoar-12345"
    assert published["topic"] == "xsoar.incident.events"
    assert published["key"] == "12345"
    assert published["payload"]["xsoar_incident_id"] == "12345"
    assert published["payload"]["mirror_remote_id"] == "xsoar-12345"
    assert published["payload"]["delta"] == {"severity": 3}
    assert published["payload"]["entries"] == [{"id": "entry-1"}]
    assert published["payload"]["incident"] == {"id": "12345", "name": "Example Incident"}
    assert published["payload"]["emitted_at_epoch_ms"] == 1234567
    # No remoteId passed in args, so this is the first mirror call.
    assert published["payload"]["event_type"] == "incident_created"


def _make_args(kafka_module, **overrides):
    base = {
        "data": {},
        "delta": {},
        "entries": [],
        "remoteId": "xsoar-12345",
        "incidentChanged": True,
        "status": 1,
    }
    base.update(overrides)
    return kafka_module.UpdateRemoteSystemArgs(base)


def test_classify_event_incident_closed_via_close_fields(kafka_module):
    args = _make_args(kafka_module, delta={"closeReason": "False Positive"})
    assert kafka_module.classify_event(args, args.delta) == "incident_closed"


def test_classify_event_incident_closed_via_status_transition(kafka_module):
    args = _make_args(kafka_module, delta={"status": 2}, status=2)
    assert kafka_module.classify_event(args, args.delta) == "incident_closed"


def test_classify_event_incident_created_when_first_publish(kafka_module):
    args = _make_args(kafka_module, remoteId=None, delta={"severity": 3})
    assert kafka_module.classify_event(args, args.delta, is_first_publish=True) == "incident_created"


def test_classify_event_close_takes_priority_over_first_publish(kafka_module):
    args = _make_args(kafka_module, remoteId=None, delta={"closeReason": "Resolved"})
    assert kafka_module.classify_event(args, args.delta, is_first_publish=True) == "incident_closed"


def test_classify_event_field_change(kafka_module):
    args = _make_args(kafka_module, delta={"severity": 3})
    assert kafka_module.classify_event(args, args.delta) == "field_change"


def test_classify_event_entry_added_when_no_delta(kafka_module):
    args = _make_args(kafka_module, delta={}, entries=[{"id": "entry-1"}])
    assert kafka_module.classify_event(args, args.delta) == "entry_added"


def test_classify_event_falls_back_when_nothing_changed(kafka_module):
    args = _make_args(kafka_module, delta={}, entries=[])
    assert kafka_module.classify_event(args, args.delta) == "incident_mirror_update"


def test_is_playbook_in_progress(kafka_module):
    assert kafka_module.is_playbook_in_progress({"playbookId": "abc", "runStatus": "running"}) is True
    assert kafka_module.is_playbook_in_progress({"playbookId": "abc", "runStatus": "pending"}) is True
    assert kafka_module.is_playbook_in_progress({"playbookId": "abc", "runStatus": "waiting"}) is False
    assert kafka_module.is_playbook_in_progress({"playbookId": "abc", "runStatus": "completed"}) is False
    assert kafka_module.is_playbook_in_progress({"playbookId": "abc", "runStatus": "failed"}) is False
    assert kafka_module.is_playbook_in_progress({"playbookId": "", "runStatus": "running"}) is False
    assert kafka_module.is_playbook_in_progress({}) is False


def _setup_publish_capture(kafka_module, monkeypatch, params):
    published = {}
    monkeypatch.setattr(kafka_module.demisto, "params", lambda: params)
    monkeypatch.setattr(
        kafka_module,
        "publish_to_kafka",
        lambda topic, key, payload: published.update({"topic": topic, "key": key, "payload": payload}),
    )
    return published


def test_update_remote_system_suppresses_publish_while_playbook_running(kafka_module, monkeypatch):
    published = _setup_publish_capture(
        kafka_module,
        monkeypatch,
        {"topic": "xsoar.incident.events", "wait_for_playbook": "true"},
    )

    remote_id = kafka_module.update_remote_system_command(
        {
            "data": {"id": "12345", "playbookId": "pb-1", "runStatus": "running"},
            "delta": {"severity": 3},
            "incidentChanged": True,
            "status": 1,
        }
    )

    assert remote_id == "xsoar-pending-12345"
    assert published == {}


def test_update_remote_system_first_real_publish_after_pending_emits_incident_created(kafka_module, monkeypatch):
    published = _setup_publish_capture(
        kafka_module,
        monkeypatch,
        {"topic": "xsoar.incident.events", "wait_for_playbook": "true", "include_full_incident": "true"},
    )

    remote_id = kafka_module.update_remote_system_command(
        {
            "data": {"id": "12345", "playbookId": "pb-1", "runStatus": "completed"},
            "delta": {"severity": 3},
            "remoteId": "xsoar-pending-12345",
            "incidentChanged": True,
            "status": 1,
        }
    )

    assert remote_id == "xsoar-12345"
    assert published["payload"]["event_type"] == "incident_created"
    assert published["payload"]["mirror_remote_id"] == "xsoar-12345"
    assert published["payload"]["playbook_id"] == "pb-1"
    assert published["payload"]["playbook_run_status"] == "completed"


def test_update_remote_system_publishes_when_no_playbook_attached(kafka_module, monkeypatch):
    published = _setup_publish_capture(
        kafka_module,
        monkeypatch,
        {"topic": "xsoar.incident.events", "wait_for_playbook": "true"},
    )

    remote_id = kafka_module.update_remote_system_command(
        {
            "data": {"id": "12345"},
            "delta": {"severity": 3},
            "incidentChanged": True,
            "status": 1,
        }
    )

    assert remote_id == "xsoar-12345"
    assert published["payload"]["event_type"] == "incident_created"


def test_update_remote_system_publishes_when_playbook_reaches_waiting(kafka_module, monkeypatch):
    published = _setup_publish_capture(
        kafka_module,
        monkeypatch,
        {"topic": "xsoar.incident.events", "wait_for_playbook": "true"},
    )

    remote_id = kafka_module.update_remote_system_command(
        {
            "data": {"id": "12345", "playbookId": "pb-1", "runStatus": "waiting"},
            "delta": {"severity": 3},
            "incidentChanged": True,
            "status": 1,
        }
    )

    assert remote_id == "xsoar-12345"
    assert published["payload"]["event_type"] == "incident_created"
    assert published["payload"]["playbook_run_status"] == "waiting"


def test_update_remote_system_no_gate_after_first_publish_even_if_playbook_running(kafka_module, monkeypatch):
    published = _setup_publish_capture(
        kafka_module,
        monkeypatch,
        {"topic": "xsoar.incident.events", "wait_for_playbook": "true"},
    )

    remote_id = kafka_module.update_remote_system_command(
        {
            "data": {"id": "12345", "playbookId": "pb-1", "runStatus": "running"},
            "delta": {"severity": 3},
            "remoteId": "xsoar-12345",
            "incidentChanged": True,
            "status": 1,
        }
    )

    assert remote_id == "xsoar-12345"
    assert published["payload"]["event_type"] == "field_change"
    assert published["payload"]["playbook_run_status"] == "running"


def test_update_remote_system_wait_for_playbook_disabled_publishes_immediately(kafka_module, monkeypatch):
    published = _setup_publish_capture(
        kafka_module,
        monkeypatch,
        {"topic": "xsoar.incident.events", "wait_for_playbook": "false"},
    )

    remote_id = kafka_module.update_remote_system_command(
        {
            "data": {"id": "12345", "playbookId": "pb-1", "runStatus": "running"},
            "delta": {"severity": 3},
            "incidentChanged": True,
            "status": 1,
        }
    )

    assert remote_id == "xsoar-12345"
    assert published["payload"]["event_type"] == "incident_created"
    assert published["payload"]["playbook_run_status"] == "running"
