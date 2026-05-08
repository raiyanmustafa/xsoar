# Kafka Outbound Mirror

Outbound-only Cortex XSOAR mirroring pack. Meaningful incident updates are published to a Kafka topic from XSOAR's outbound mirror cycle so downstream systems can react in near real time without polling XSOAR.

---

## Use case

A downstream system (analytics warehouse, external case tracker, external SIEM, BI dashboard, etc.) needs to know about XSOAR incidents and how they evolve over their lifetime. The two ways to get that data are:

1. **Pull** — periodically poll the XSOAR API or query the database directly.
2. **Push** — XSOAR sends relevant updates through the outbound mirror cycle.

Polling is operationally bad: it adds load, scales poorly, lags reality, and couples the consumer to XSOAR's API surface. Reading the database directly is worse.

This pack implements the push pattern. XSOAR pushes Kafka events from outbound mirror invocations. Downstream subscribes to one topic and is free of XSOAR.

The contract is simple: **each outbound mirror invocation with relevant incident changes → one Kafka event containing the cumulative delta**. XSOAR may batch multiple edits into one mirror invocation, and the first publish may be delayed until the incident playbook reaches a settled state.

---

## Solution overview

A pack containing three pieces of content that work together:

| Component | Type | Purpose |
| --- | --- | --- |
| `KafkaOutboundMirror` | Integration | Implements XSOAR's outbound mirroring contract. Receives `update-remote-system` calls from the mirror engine and publishes a Kafka event. |
| `SetKafkaMirrorFields` | Pre-processing script | Sets `dbotMirrorDirection`, `dbotMirrorInstance`, `dbotMirrorTags` on every new incident so the mirror engine knows to invoke the integration. Auto-discovers the active integration instance when exactly one exists. |
| `Kafka Outbound Mirror - Tag every incident` | Pre-processing rule | Wires the script to every new incident. |

XSOAR's data model treats these as three distinct entity types, but they ship and version together as one pack.

The default pre-processing rule is intended for deployments with one active `KafkaOutboundMirror` instance. If multiple active Kafka mirror instances exist, pass `instance_name` explicitly in the pre-processing rule arguments or create separate rules with conditions that route incidents to the intended instance.

---

## Architecture

```
+---------------------+         +-----------------+
|  Incident created   |         | Pre-processing  |
|  (manual or fetch)  |-------->|     rule fires  |
+---------------------+         +-----------------+
                                          |
                                          v
                                +-------------------+
                                | SetKafkaMirrorFields
                                | sets dbotMirror*  |
                                +-------------------+
                                          |
                                          v
                                +-------------------+
                                | XSOAR mirror      |
                                | engine (1-min     |
                                | cycle, batches    |
                                | field deltas)     |
                                +-------------------+
                                          |
                                          v
                                +-------------------+
                                | KafkaOutboundMirror
                                | update-remote-system
                                +-------------------+
                                          |
                                          v
                                +-------------------+
                                |    Kafka topic    |
                                | xsoar.incident.events
                                +-------------------+
                                          |
                                          v
                                  downstream consumer
```

Mirror direction is `Outgoing` only. The integration never reads back from Kafka and does not implement `fetch-incidents`, `get-remote-data`, or `get-modified-remote-data`. XSOAR is the source of truth; Kafka is a one-way fanout.

---

## Event lifecycle

A typical incident produces this sequence of Kafka events over its life:

| Stage | XSOAR state | Event published | Notes |
| --- | --- | --- | --- |
| Incident created, post-creation playbook running | `runStatus = running` / `pending` | **none** (suppressed) | Integration returns `xsoar-pending-{id}` sentinel. Avoids publishing a half-built incident. |
| Playbook reaches a manual task | `runStatus = waiting` | `incident_created` | Automation has done what it can; this is the first "settled" snapshot. |
| Playbook completes without manual task | `runStatus = completed` | `incident_created` | Same idea, alternate path. |
| Analyst edits one or more fields | any | `field_change` | Delta carries the fields changed in that mirror cycle; multiple edits can be batched into one event. |
| Analyst adds a tagged war-room note | any | `entry_added` | Only entries tagged with `xsoar-kafka` are mirrored. |
| Incident closed | close fields in delta or status → 2 | `incident_closed` | Highest-priority classification. |

Once an incident has been published once, the playbook gate no longer applies — every subsequent change publishes regardless of `runStatus`.

The gate can be disabled per instance via the `Wait for incident playbook to complete before publishing` toggle if a deployment uses indefinitely-paused playbooks.

---

## Event schema

```json
{
  "schema_version": "1.0",
  "source": "cortex-xsoar",
  "event_type": "incident_created | field_change | entry_added | incident_closed | incident_mirror_update",
  "emitted_at": "2026-05-07T12:50:21Z",
  "emitted_at_epoch_ms": 1715086221000,
  "xsoar_incident_id": "9281",
  "mirror_remote_id": "xsoar-9281",
  "incident_changed": true,
  "incident_status": 1,
  "playbook_id": "playbook0",
  "playbook_run_status": "waiting",
  "delta":   { "severity": 3 },
  "entries": [],
  "incident": { "id": "9281", "name": "...", "...": "full snapshot if include_full_incident=true" }
}
```

- **Partition key:** `xsoar_incident_id`. Per-incident ordering is preserved across all event types.
- **Topic:** single topic, classified via `event_type`. Standard CDC pattern (think Debezium / Outbox).
- **Idempotence:** the producer uses `enable.idempotence=true` and `acks=all`.

---

## Design decisions

| Decision | Why |
| --- | --- |
| Outbound only (`isremotesyncout: true`, no `isfetch`) | The integration only emits. Setting `isfetch: true` would schedule a `fetch-incidents` call the integration does not implement, generating error noise. |
| Suppress publish while `runStatus in {running, pending}` for the first publish only | Avoids publishing a half-built incident before the post-creation playbook settles. After first publish, gate is released so subsequent edits flow normally. |
| `waiting` does **not** suppress | Manual-task pauses are interpreted as "automation finished, awaiting human" — that is the natural moment to publish the first snapshot. |
| Classify event types in payload, not topic | One topic preserves per-incident ordering. Consumers filter on `event_type`. |
| Include full incident snapshot by default | Lets the consumer be stateless. Toggle off if payload size is a concern. |
| Auto-discover integration instance in the pre-processing script | One pack handles any single-instance deployment without hardcoding. Multi-instance deployments must pass `instance_name` explicitly to avoid ambiguous routing. |
| No application-level debouncing / field filtering | XSOAR's mirror engine already batches changes within a 1-minute cycle. A single `update-remote-system` call carries the cumulative delta, so a 30-second flurry of edits becomes one Kafka event. |

---

## Configuration

Set on the integration instance.

| Field | Required | Default | Notes |
| --- | --- | --- | --- |
| Brokers | Yes | — | CSV of broker hostports. |
| Topic | Yes | `xsoar.incident.events` | Single topic for all event types. |
| Use TLS / Use SASL PLAIN over SSL | No | off | Pick one or neither. |
| CA cert / Client cert / Client cert key / Key password | No | — | Provided as PEM bodies in the UI; written to ephemeral temp files at runtime and cleaned up after each call. |
| SASL credentials | No | — | Username/password pair for SASL_SSL/PLAIN. |
| Mirror Direction | No | `Outgoing` | The integration only supports outbound. |
| Outgoing Entry Tags | No | `xsoar-kafka` | Only tagged war-room entries are mirrored out. |
| Include full incident snapshot | No | `true` | If false, payload only carries delta + entries. |
| Wait for incident playbook to complete before publishing | No | `true` | Implements the playbook gate described above. |

---

## Deployment

The pack is the unit of deployment. The integration, the pre-processing script, and the pre-processing rule that wires them together all install in one step.

### 1. Validate and upload the pack

```bash
./sdk validate -i Packs/KafkaOutboundMirror
./sdk upload -i Packs/KafkaOutboundMirror -z
```

The SDK uploads the pack through the XSOAR pack-upload API as a custom/dev pack. That API skips signature verification for custom packs, which is required because this repository does not produce a Cortex XSOAR-signed Marketplace pack.

If you need a file artifact for review or handoff, build one separately:

```bash
./sdk prepare-content -i Packs/KafkaOutboundMirror -o /tmp/kafka-pack-build
```

`prepare-content` writes `/tmp/kafka-pack-build.zip`. The archive contains the unified integration, the unified script, the pre-processing rule, and the pack metadata, but some XSOAR UI import paths only accept signed Marketplace packs and will reject this unsigned ZIP. Use SDK/API upload for installation.

This requires the API key configured in `.env` to have permissions to upload custom/dev packs. If the upload fails with authorization errors, use an admin-scoped key or ask an XSOAR admin to run the command.

### 2. Create the integration instance

`Settings → Integrations → Instances → Add instance → Kafka Outbound Mirror`. Set brokers, topic, TLS/SASL, click **Test**. The pre-processing rule is already enabled and references the `SetKafkaMirrorFields` script by ID, so as long as exactly one active `KafkaOutboundMirror` instance exists, no further wiring is needed. For multi-instance deployments, edit the rule and set `instance_name` explicitly per the [Solution overview](#solution-overview).

That's it — create a test incident to verify (see [Verification](#verification)).

### Developer iteration

For fast feedback while editing the integration code or the pre-processing script — *after* the pack has been installed once via step 2 — upload them item-by-item without rebuilding the ZIP:

```bash
./sdk upload -i Packs/KafkaOutboundMirror/Integrations/KafkaOutboundMirror
./sdk upload -i Packs/KafkaOutboundMirror/Scripts/SetKafkaMirrorFields
```

Item-level upload is only for updating the integration or script after the pack has already been installed. Pre-processing rules are not reliably item-uploadable through the SDK, so the first install must go through pack upload. Afterwards the rule lives on the server and code-only changes can iterate item-level.

For incidents that come from a fetching integration's classifier/mapper, you can alternately add the three `dbotMirror*` fields directly to that mapper. The pre-processing rule shipped in the pack is preferred because it covers manually created incidents too.

### Upload troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| UI import says the file is not a signed Cortex XSOAR Content Pack | The ZIP is an unsigned custom/dev pack, not a Marketplace-signed pack. | Use `./sdk upload -i Packs/KafkaOutboundMirror -z`, which uploads through the custom/dev pack API with signature verification skipped. |
| SDK upload shows `FAILED UPLOADS ... Unauthorized` | The API key reached the pack-upload endpoint but does not have permission to upload custom/dev packs. | Use an admin-scoped API key or ask an XSOAR admin to run the SDK upload. `--skip_validation` does not fix authorization failures. |
| Item-level upload works but the pre-processing rule is missing | Integration/script item upload does not install `PreProcessRules`. | Install the pack with `./sdk upload -i Packs/KafkaOutboundMirror -z` first, then use item-level upload only for later code iterations. |

---

## Verification

Create a test incident. In a Kafka shell:

```bash
kafka-console-consumer --bootstrap-server <broker>:9092 \
  --topic xsoar.incident.events --from-beginning
```

Expected sequence for a typical incident:

1. Incident created, playbook running → no message yet.
2. Playbook reaches manual task or completes → first message: `event_type: "incident_created"`.
3. Edit `severity` → `event_type: "field_change"`, `delta: {"severity": ...}`. Multiple edits in the same mirror cycle may appear in one cumulative delta.
4. Add a war-room note tagged `xsoar-kafka` → `event_type: "entry_added"`.
5. Close the incident → `event_type: "incident_closed"`.

If nothing fires:

```text
!getIncidents id=<incident_id>
```

Check `dbotMirrorDirection` (should be `Out`), `dbotMirrorInstance` (your instance name), `dbotMirrorTags` (should include `xsoar-kafka`). If any are empty the pre-processing rule did not run on that incident.

To force a mirror cycle for debugging:

```text
!triggerDebugMirroringRun incidentId=<incident_id>
```

Other useful hidden commands: `!getMirrorStatistics`, `!getSyncMirrorRecords`.

---

## Tests

```bash
.venv/bin/python -m pytest Packs/KafkaOutboundMirror/Scripts/SetKafkaMirrorFields/SetKafkaMirrorFields_test.py
.venv/bin/python -m pytest Packs/KafkaOutboundMirror/Integrations/KafkaOutboundMirror/KafkaOutboundMirror_test.py
./sdk validate -i Packs/KafkaOutboundMirror
./sdk prepare-content -i Packs/KafkaOutboundMirror -o /tmp/kafka-pack-build
```

Covers Kafka config building (plain / SASL / SSL), the event classifier, the playbook gate, the sentinel-based "first real publish" detection, payload shape, pre-processing script instance selection, pack validation, and pack ZIP generation.
