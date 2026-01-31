"""Cross-platform wrapper to run openapi-python-client generate.

Usage:
    python scripts/generate_api_client/script.py [--url URL] [--overwrite]

This attempts to find `openapi-python-client` on PATH; if not present it tries to run
`python -m openapi_python_client` as a fallback (the module exposes an entry point).
"""

import argparse
import shutil
import subprocess
import sys

DEFAULT_URL = (
    "https://raw.githubusercontent.com/aura-historia/internal-api/master/swagger.yaml"
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)

    cmd = ["openapi-python-client", "generate", "--url", args.url]
    if args.overwrite:
        cmd.append("--overwrite")

    # Prefer CLI binary
    if shutil.which(cmd[0]):
        print("Running: ", " ".join(cmd))
        return subprocess.call(cmd)

    # Fallback to module
    module_cmd = [
        sys.executable,
        "-m",
        "openapi_python_client",
        "generate",
        "--url",
        args.url,
    ]
    if args.overwrite:
        module_cmd.append("--overwrite")

    print(
        "openapi-python-client not found on PATH, trying python -m openapi_python_client ..."
    )
    print("Running: ", " ".join(module_cmd))
    return subprocess.call(module_cmd)


if __name__ == "__main__":
    raise SystemExit(main())
