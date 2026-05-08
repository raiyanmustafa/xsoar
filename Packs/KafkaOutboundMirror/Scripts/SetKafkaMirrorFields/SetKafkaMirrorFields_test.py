import importlib.util
import sys
import types
from pathlib import Path

import pytest


class DemistoException(Exception):
    pass


@pytest.fixture
def script_module(monkeypatch):
    state = {
        "args": {},
        "modules": {},
        "incident": {"id": "12345"},
        "commands": [],
        "results": None,
    }

    demisto = types.SimpleNamespace(
        args=lambda: state["args"],
        getModules=lambda: state["modules"],
        incidents=lambda: [state["incident"]],
        executeCommand=lambda command, args: state["commands"].append((command, args)),
        results=lambda result: state.update({"results": result}),
    )

    common = types.ModuleType("CommonServerPython")

    def return_error(message):
        raise DemistoException(message)

    common.return_error = return_error

    monkeypatch.setitem(sys.modules, "demistomock", demisto)
    monkeypatch.setitem(sys.modules, "CommonServerPython", common)

    module_path = Path(__file__).with_name("SetKafkaMirrorFields.py")
    spec = importlib.util.spec_from_file_location("SetKafkaMirrorFields", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.test_state = state
    return module


def _kafka_module(state="active"):
    return {"brand": "KafkaOutboundMirror", "state": state}


def test_main_auto_selects_single_active_kafka_instance(script_module):
    script_module.test_state["modules"] = {
        "KafkaOutboundMirror_instance_1": _kafka_module(),
        "OtherIntegration_instance_1": {"brand": "OtherIntegration", "state": "active"},
        "KafkaOutboundMirror_disabled": _kafka_module("disabled"),
    }

    script_module.main()

    incident = script_module.test_state["incident"]
    assert incident["dbotMirrorDirection"] == "Out"
    assert incident["dbotMirrorInstance"] == "KafkaOutboundMirror_instance_1"
    assert incident["dbotMirrorTags"] == ["xsoar-kafka"]
    assert script_module.test_state["commands"] == [
        (
            "setIncident",
            {
                "dbotMirrorDirection": "Out",
                "dbotMirrorInstance": "KafkaOutboundMirror_instance_1",
                "dbotMirrorTags": ["xsoar-kafka"],
            },
        )
    ]


def test_main_fails_when_multiple_active_kafka_instances_without_explicit_name(script_module):
    script_module.test_state["modules"] = {
        "KafkaOutboundMirror_instance_1": _kafka_module(),
        "KafkaOutboundMirror_instance_2": _kafka_module(),
    }

    with pytest.raises(DemistoException, match="Multiple active KafkaOutboundMirror integration instances found"):
        script_module.main()


def test_main_uses_explicit_instance_name_when_multiple_active_instances_exist(script_module):
    script_module.test_state["args"] = {"instance_name": "KafkaOutboundMirror_instance_2"}
    script_module.test_state["modules"] = {
        "KafkaOutboundMirror_instance_1": _kafka_module(),
        "KafkaOutboundMirror_instance_2": _kafka_module(),
    }

    script_module.main()

    incident = script_module.test_state["incident"]
    assert incident["dbotMirrorDirection"] == "Out"
    assert incident["dbotMirrorInstance"] == "KafkaOutboundMirror_instance_2"
    assert incident["dbotMirrorTags"] == ["xsoar-kafka"]
    assert script_module.test_state["commands"] == [
        (
            "setIncident",
            {
                "dbotMirrorDirection": "Out",
                "dbotMirrorInstance": "KafkaOutboundMirror_instance_2",
                "dbotMirrorTags": ["xsoar-kafka"],
            },
        )
    ]


def test_find_kafka_instance_ignores_inactive_and_non_kafka_modules(script_module):
    script_module.test_state["modules"] = {
        "OtherIntegration_instance_1": {"brand": "OtherIntegration", "state": "active"},
        "KafkaOutboundMirror_disabled": _kafka_module("disabled"),
        "KafkaOutboundMirror_instance_1": _kafka_module(),
    }

    assert script_module.find_kafka_instance() == "KafkaOutboundMirror_instance_1"
