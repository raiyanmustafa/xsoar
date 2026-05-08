import demistomock as demisto
from CommonServerPython import *

KAFKA_BRAND = "KafkaOutboundMirror"


def find_kafka_instances():
    modules = demisto.getModules() or {}
    instances = []
    for name, mod in modules.items():
        if mod.get("brand") == KAFKA_BRAND and mod.get("state") == "active":
            instances.append(name)
    return instances


def find_kafka_instance():
    instances = find_kafka_instances()

    if len(instances) == 1:
        return instances[0]

    if len(instances) > 1:
        return_error(
            f"Multiple active {KAFKA_BRAND} integration instances found: {', '.join(sorted(instances))}. "
            "Pass instance_name explicitly in the pre-processing rule arguments."
        )

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

    demisto.executeCommand(
        "setIncident",
        {
            "dbotMirrorDirection": "Out",
            "dbotMirrorInstance": instance_name,
            "dbotMirrorTags": [tag],
        },
    )

    demisto.results(incident)


if __name__ in ("__main__", "__builtin__", "builtins"):
    main()
