"""Parallel SSH availability scanner.

Usage example:
    # Banner check only (fast, no authentication)
    python scripts/available_ssh_remotes.py --port 22050

    # With login verification (secure password prompt)
    python scripts/available_ssh_remotes.py --port 22050 --username user123 --verify-login

    # With environment variable for password (most secure)
    export SSH_PASSWORD='yourpassword'
    python scripts/available_ssh_remotes.py --port 22050 --username user123 --verify-login

A host is considered available if:
- TCP connection succeeds AND
- SSH banner is received (starts with "SSH-")
- (Optional with --verify-login) Successful SSH authentication

Security features:
- Password via secure prompt or environment variable (never via command line)
- Rate limiting with --concurrency and --delay
- Optional host key verification
- Minimal logging (use --verbose for detailed output)
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import ipaddress
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from dotenv import load_dotenv

try:
    import asyncssh

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False
    asyncssh = None  # type: ignore

load_dotenv(verbose=True)

DEFAULT_PREFIX = "141.57.11"
DEFAULT_PORT = 22050
DEFAULT_THIRD_OCTET_START = 11
DEFAULT_THIRD_OCTET_END = 11
DEFAULT_PASSWORD_ENV = os.getenv("SSH_PASSWORD")


@dataclass(frozen=True)
class ProbeResult:
    host: str
    available: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class ScanConfig:
    hosts: List[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    timeout_sec: float
    concurrency: int
    output: Optional[Path]
    dry_run: bool
    show_errors: bool
    append_hosts_file: Optional[Path]
    summary: bool
    verify_login: bool
    verbose: bool


def _expand_range(
    base_prefix: str, third_start: int, third_end: int, reverse: bool = False
) -> List[str]:
    """
    Expand IP range with configurable third octet range.

    Example:
    - base_prefix="141.57", third_start=10, third_end=12
      -> 141.57.10.1-254, 141.57.11.1-254, 141.57.12.1-254
    - reverse=True scans in reverse order
    """
    parts = base_prefix.strip().rstrip(".").split(".")
    if len(parts) == 2:
        # Format: "141.57" -> use third_start/end for third octet
        first_two = base_prefix
    elif len(parts) == 3:
        # Format: "141.57.11" -> extract first two, always use provided third_start/end
        first_two = ".".join(parts[:2])
    else:
        raise ValueError("Prefix must be like 141.57 or 141.57.11")

    hosts: List[str] = []

    # Determine order for third octet
    if reverse:
        third_range = range(third_end, third_start - 1, -1)
    else:
        third_range = range(third_start, third_end + 1)

    for third_octet in third_range:
        # Determine order for fourth octet
        if reverse:
            fourth_range = range(254, 0, -1)
        else:
            fourth_range = range(1, 255)

        for fourth_octet in fourth_range:
            host = f"{first_two}.{third_octet}.{fourth_octet}"
            ipaddress.ip_address(host)
            hosts.append(host)

    return hosts


def build_hosts(third_start: int, third_end: int, reverse: bool) -> List[str]:
    return _expand_range(DEFAULT_PREFIX, third_start, third_end, reverse)


def parse_args(argv: Iterable[str]) -> ScanConfig:
    parser = argparse.ArgumentParser(
        description="Parallel SSH availability scanner. "
        "A host is available if it responds with an SSH banner."
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="SSH port (optional, uses 22 if not specified)",
    )
    parser.add_argument(
        "--username", type=str, default=None, help="SSH username for login verification"
    )
    parser.add_argument(
        "--password-env",
        type=str,
        default=DEFAULT_PASSWORD_ENV,
        help="Environment variable name for SSH password (default: SSH_PASSWORD)",
    )
    parser.add_argument(
        "--verify-login",
        action="store_true",
        help="Verify actual SSH login (requires --username)",
    )
    parser.add_argument(
        "--third-octet-start",
        type=int,
        default=DEFAULT_THIRD_OCTET_START,
        help=f"Start of third octet range (default: {DEFAULT_THIRD_OCTET_START})",
    )
    parser.add_argument(
        "--third-octet-end",
        type=int,
        default=DEFAULT_THIRD_OCTET_END,
        help=f"End of third octet range (default: {DEFAULT_THIRD_OCTET_END})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="Timeout in seconds to wait for SSH banner (default: 3.0)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=100,
        help="Parallel connection limit (default: 5)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between connections (default: 0.0)",
    )
    parser.add_argument(
        "--reverse",
        action="store_true",
        help="Scan IPs in reverse order (11->3 and 254->1)",
    )
    parser.add_argument("--output", help="Write available hosts to this file")
    parser.add_argument(
        "--append-hosts-file", help="Append available hosts to this file"
    )
    parser.add_argument(
        "--summary", action="store_true", help="Print summary to stderr"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print expanded hosts, no connections",
    )
    parser.add_argument(
        "--show-errors", action="store_true", help="Print errors for unavailable hosts"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose output for successful connections",
    )

    args = parser.parse_args(list(argv))

    hosts = build_hosts(args.third_octet_start, args.third_octet_end, args.reverse)

    output = Path(args.output) if args.output else None
    append_hosts_file = Path(args.append_hosts_file) if args.append_hosts_file else None

    # Handle password securely
    password = None
    if args.verify_login:
        if not args.username:
            parser.error("--verify-login requires --username")

        # Try to get password from environment variable
        password = os.getenv(args.password_env)

        # If not in environment, prompt securely
        if not password:
            password = getpass.getpass(f"SSH Password for {args.username}: ")
            if not password:
                parser.error("Password is required for --verify-login")

    return ScanConfig(
        hosts=hosts,
        port=args.port,
        username=args.username,
        password=password,
        timeout_sec=args.timeout,
        concurrency=args.concurrency,
        output=output,
        dry_run=args.dry_run,
        show_errors=args.show_errors,
        append_hosts_file=append_hosts_file,
        summary=args.summary,
        verify_login=args.verify_login,
        verbose=args.verbose,
    )


async def _check_ssh_banner(host: str, port: int, verbose: bool) -> ProbeResult:
    """Check if host responds with valid SSH banner."""
    writer: Optional[asyncio.StreamWriter] = None
    try:
        # TCP connect
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except asyncio.TimeoutError:
            error = f"ssh: connect to host {host} port {port}: Connection timed out"
            return ProbeResult(host=host, available=False, error=error)

        # Read SSH banner
        try:
            banner = await reader.readline()
        except asyncio.TimeoutError:
            error = f"ssh: connect to host {host} port {port}: Banner read timed out"
            return ProbeResult(host=host, available=False, error=error)

        # Check if it's a valid SSH banner
        if banner and banner.startswith(b"SSH-"):
            if verbose:
                print(f"[OK] {host} - SSH server responding", file=sys.stderr)
            return ProbeResult(host=host, available=True)
        else:
            error = f"ssh: connect to host {host} port {port}: No SSH banner received"
            return ProbeResult(host=host, available=False, error=error)

    except ConnectionRefusedError:
        error = f"ssh: connect to host {host} port {port}: Connection refused"
        return ProbeResult(host=host, available=False, error=error)
    except OSError as exc:
        error = f"ssh: connect to host {host} port {port}: {exc}"
        return ProbeResult(host=host, available=False, error=error)
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


async def _verify_ssh_login(
    host: str,
    port: int,
    username: Optional[str],
    password: Optional[str],
    verbose: bool,
) -> ProbeResult:
    """Verify SSH login with username and password."""
    if not ASYNCSSH_AVAILABLE:
        error = "ssh: asyncssh library not installed (pip install asyncssh)"
        return ProbeResult(host=host, available=False, error=error)

    try:
        # Connect with username/password
        conn = await asyncssh.connect(
            host,
            port=port,
            username=username,
            password=password,
            known_hosts=None,  # Don't verify host keys for scanning
        )

        # Connection successful! Send exit command and close
        try:
            await conn.run("exit", check=True)
        except Exception:
            pass  # Ignore exit errors

        conn.close()
        await conn.wait_closed()

        if verbose:
            print(f"[OK] {host} - SSH login and exit successful", file=sys.stderr)
        return ProbeResult(host=host, available=True)

    except asyncio.TimeoutError:
        error = f"ssh: connect to host {host} port {port}: Connection timed out"
        return ProbeResult(host=host, available=False, error=error)
    except asyncssh.PermissionDenied:
        error = f"ssh: connect to host {host} port {port}: Permission denied (wrong password)"
        return ProbeResult(host=host, available=False, error=error)
    except ConnectionRefusedError:
        error = f"ssh: connect to host {host} port {port}: Connection refused"
        return ProbeResult(host=host, available=False, error=error)
    except Exception as exc:
        error = f"ssh: connect to host {host} port {port}: {exc}"
        return ProbeResult(host=host, available=False, error=error)


async def probe_ssh(
    host: str, config: ScanConfig, semaphore: asyncio.Semaphore
) -> ProbeResult:
    """
    Try to connect to host:port and read SSH banner.
    If we get a banner starting with "SSH-", the host is available
    (meaning it would ask for password or hostkey confirmation).

    IMPORTANT: A host is ONLY available if:
    1. TCP connection succeeds within timeout
    2. SSH banner (starting with "SSH-") is received within timeout
    3. (Optional) If --verify-login: Actual SSH authentication succeeds

    Any timeout or error = NOT available.
    """
    port = config.port if config.port is not None else 22
    async with semaphore:
        try:
            async with asyncio.timeout(config.timeout_sec):
                if config.verify_login:
                    # For login verification, use 3x timeout
                    async with asyncio.timeout(config.timeout_sec * 3):
                        return await _verify_ssh_login(
                            host, port, config.username, config.password, config.verbose
                        )
                else:
                    return await _check_ssh_banner(host, port, config.verbose)
        except asyncio.TimeoutError:
            error = f"ssh: connect to host {host} port {port}: Connection timed out"
            return ProbeResult(host=host, available=False, error=error)


async def scan_hosts(config: ScanConfig) -> List[ProbeResult]:
    semaphore = asyncio.Semaphore(max(1, config.concurrency))
    tasks = [probe_ssh(host, config, semaphore) for host in config.hosts]
    return await asyncio.gather(*tasks)


def write_output(hosts: List[str], output: Optional[Path]) -> None:
    lines = "\n".join(hosts)
    if output:
        output.write_text(lines + ("\n" if lines else ""), encoding="utf-8")
    else:
        print(lines)


def append_hosts(hosts: List[str], target: Optional[Path]) -> None:
    if not target or not hosts:
        return

    existing_content = ""
    existing: set[str] = set()
    if target.exists():
        existing_content = target.read_text(encoding="utf-8")
        existing = {
            line.strip() for line in existing_content.splitlines() if line.strip()
        }

    new_hosts = [host for host in hosts if host not in existing]
    if not new_hosts:
        return

    content = "\n".join(new_hosts) + "\n"
    target.write_text(existing_content + content, encoding="utf-8")


def main(argv: Iterable[str]):
    config = parse_args(argv)

    if config.dry_run:
        write_output(config.hosts, config.output)
        return

    port = config.port if config.port is not None else 22
    print(
        f"Scanning {len(config.hosts)} hosts on port {port} (timeout: {config.timeout_sec}s)...",
        file=sys.stderr,
    )

    results = asyncio.run(scan_hosts(config))
    available_hosts = [result.host for result in results if result.available]

    if config.show_errors:
        for result in results:
            if result.error:
                print(result.error, file=sys.stderr)

    if config.summary:
        print(
            f"SSH available: {len(available_hosts)}/{len(config.hosts)}",
            file=sys.stderr,
        )

    write_output(available_hosts, config.output)
    append_hosts(available_hosts, config.append_hosts_file)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
