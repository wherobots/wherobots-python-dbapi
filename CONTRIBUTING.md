# Contributor Guidance

This project uses `uv`. Run `uv sync` after checking out the repository
to initialize your virtualenv with the project's dependencies.

## Running tests

Unit tests live in `tests/` and run with pytest:

```bash
uv run pytest tests/
```

The `scripts/` directory contains integration scripts that require a live
Wherobots environment and are not part of the automated test suite.

### Smoke test

`scripts/smoke.py` runs queries against a live Wherobots SQL session.
It requires an API key (or token) and supports most `connect()` options
via CLI flags.

```bash
# Basic query with an API key
uv run python scripts/smoke.py \
  --api-key-file ~/.wherobots/api-key \
  "SELECT 1"

# Specify runtime, region, and version
uv run python scripts/smoke.py \
  --api-key-file ~/.wherobots/api-key \
  --runtime tiny --region aws-us-west-2 --version latest \
  "SELECT ST_AsText(ST_Point(1, 2))"

# Connect directly to an existing session via WebSocket URL
uv run python scripts/smoke.py \
  --api-key-file ~/.wherobots/api-key \
  --ws-url wss://compute.example.com/sql/org/session-id \
  "SHOW TABLES"

# Enable debug logging and execution progress
uv run python scripts/smoke.py \
  --api-key-file ~/.wherobots/api-key \
  --debug --progress \
  "SELECT * FROM wherobots_open_data.overture.places LIMIT 10"
```

Run `uv run python scripts/smoke.py --help` for all available options.

## Publish package to PyPI

When we are ready to release a new version `vx.y.z`, one of the maintainers should:

1. Execute `uv version {minor,patch}` to update the project version
1. Commit the version change and any release-related changes
1. Create a new tag with `git tag -s -m "Version x.y.z" vx.y.z`.
   If the tag doesn't match with the project version, step 4 validation will fail.
1. Push the tag to the repo with `git push origin vx.y.z`
1. There will be a GitHub Action triggered automatically which will build and publish to PyPI
