"""Outbound URL validation for provider and MCP integrations."""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit


class SSRFProtectionError(ValueError):
    """Raised when an outbound URL targets a private or unsafe network."""


def _unsafe_ip(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return any(
        (
            address.is_private,
            address.is_loopback,
            address.is_link_local,
            address.is_reserved,
            address.is_multicast,
            address.is_unspecified,
        )
    )


def validate_outbound_url(
    url: str,
    *,
    allow_http: bool = True,
    trusted_hosts: set[str] | None = None,
) -> str:
    """Reject local/private destinations before an outbound request."""

    value = str(url or "").strip()
    parsed = urlsplit(value)
    schemes = {"https"} | ({"http"} if allow_http else set())
    if parsed.scheme.lower() not in schemes or not parsed.hostname:
        raise SSRFProtectionError("outbound URL must use HTTP(S) with a hostname")
    hostname = parsed.hostname.rstrip(".").lower()
    if hostname == "localhost" or hostname.endswith(".localhost"):
        raise SSRFProtectionError("outbound URL targets localhost")
    if hostname in {str(item).rstrip(".").lower() for item in (trusted_hosts or set())}:
        return value
    try:
        if _unsafe_ip(hostname):
            raise SSRFProtectionError("outbound URL targets a private or reserved address")
    except ValueError:
        try:
            addresses = {
                item[4][0]
                for item in socket.getaddrinfo(hostname, parsed.port or (443 if parsed.scheme == "https" else 80))
            }
        except OSError:
            addresses = set()
        if any(_unsafe_ip(address) for address in addresses):
            raise SSRFProtectionError("outbound URL resolves to a private or reserved address")
    return value
