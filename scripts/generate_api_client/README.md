Generate OpenAPI client

This folder contains helpers to generate the Python client for the internal OpenAPI spec.

Cross-platform with Python (recommended inside a venv where openapi-python-client is installed):

    python scripts/generate_openapi.py --overwrite

Notes:
- Install the generator with pip: pip install openapi-python-client
- The scripts call the public raw YAML at: https://raw.githubusercontent.com/aura-historia/internal-api/master/swagger.yaml
