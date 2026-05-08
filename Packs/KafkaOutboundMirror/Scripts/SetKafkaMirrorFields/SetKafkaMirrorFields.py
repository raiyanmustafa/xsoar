import demistomock as demisto
from CommonServerPython import *

KAFKA_BRAND = "KafkaOutboundMirror"


def find_kafka_instance():
    modules = demisto.getModules() or {}
    for name, mod in modules.items():
        if mod.get("brand") == KAFKA_BRAND and mod.get("state") == "active":
            return name
    return None


def main():
    args = demisto.args()
    instance_name = args.get("instance_name") or find_kafka_instance()
    tag = args.get("tag") or "xsoar-kafka"

    if not instance_name:
        return_error(
            f"No active {KAFKA_BRAND} integration instance found. "
            "Create one in Settings > Integrations, or pass instance_name explicitly."
        )

    incident = demisto.incidents()[0]
    incident["dbotMirrorDirection"] = "Out"
    incident["dbotMirrorInstance"] = instance_name
    incident["dbotMirrorTags"] = [tag]

    demisto.executeCommand("setIncident", {"dbotMirrorDirection": "Out"})
    demisto.executeCommand("setIncident", {"dbotMirrorInstance": instance_name})
    demisto.executeCommand("setIncident", {"dbotMirrorTags": [tag]})

    demisto.results(incident)


if __name__ in ("__main__", "__builtin__", "builtins"):
    main()
