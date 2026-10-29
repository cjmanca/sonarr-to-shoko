"""One credential-free identity for metadata transport, recovery and status."""

from __future__ import annotations

import hashlib

import httpx


def metadata_source_key_for_url(url: str | httpx.URL) -> str:
    """Preserve proxy path identity while normalizing like the HTTP client.

    HTTPX canonicalizes scheme, host, default port and dot segments. Credentials,
    query and fragment never participate; only trailing path slashes normalize.
    """
    try:
        base = httpx.URL(url).copy_with(username=None, password=None, query=None, fragment=None)
    except httpx.InvalidURL:
        raise ValueError("invalid metadata source URL") from None
    canonical = str(base).rstrip("/") + "/"
    return hashlib.sha256(("shoko-metadata-source:v1\0" + canonical).encode()).hexdigest()
