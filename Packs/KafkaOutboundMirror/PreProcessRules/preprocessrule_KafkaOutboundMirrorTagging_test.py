import json
from pathlib import Path

import yaml


def test_preprocess_rule_runs_set_kafka_mirror_fields_script():
    rule_path = Path(__file__).with_name("preprocessrule-KafkaOutboundMirrorTagging.json")
    rule = json.loads(rule_path.read_text())

    assert rule["action"] == "script"
    assert rule["scriptID"] == "SetKafkaMirrorFields"
    assert rule["scriptName"] == "SetKafkaMirrorFields"


def test_set_kafka_mirror_fields_is_available_to_preprocess_rules():
    script_path = Path(__file__).parents[1] / "Scripts" / "SetKafkaMirrorFields" / "SetKafkaMirrorFields.yml"
    script = yaml.safe_load(script_path.read_text())

    assert "preProcessing" in script["tags"]
    assert "pre-processing" in script["tags"]
