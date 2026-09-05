# Production release evidence

To make a production-readiness claim for a real deployment, generate:

```bash
cne stability --config /path/to/production.toml --days 20 --enforce \
  > release-evidence/vX.Y.Z/stability-20d.json
cne sources slo --config /path/to/production.toml \
  --window-days 30 --minimum-observations 10 --enforce \
  > release-evidence/vX.Y.Z/source-slo-30d.json
python scripts/validate_release_evidence.py release-evidence/vX.Y.Z
```

Both reports must be produced within seven days of the release, pass their
native gates, and contain no failing critical source or open source incident.
Never copy reports from a fixture or temporary lake into this directory. These
reports are optional operational evidence and are not a package publication
gate.
