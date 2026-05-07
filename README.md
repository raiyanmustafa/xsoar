# XSOAR Custom Content Workspace

This workspace contains custom Cortex XSOAR content and a local Demisto SDK environment.

## SDK

Use the wrapper from the repository root:

```bash
./sdk --version
./sdk validate -i Packs/KafkaOutboundMirror/Integrations/KafkaOutboundMirror
./sdk upload -i Packs/KafkaOutboundMirror/Integrations/KafkaOutboundMirror
```

The wrapper uses `.venv/bin/demisto-sdk` and sets `DEMISTO_SDK_IGNORE_CONTENT_WARNING=true` because this is a small custom-content workspace, not the full public Content repository.

For upload/run commands, create a local `.env` from `.env.example` and fill in your Cortex XSOAR or XSIAM API settings.
