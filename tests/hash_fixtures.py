"""Deterministic, structurally valid Shoko hash fixtures (F13).

Shoko 5.3.3 reports the file identity Shoko and AniDB match on as
``ed2k://|file|<name>|<size>|<md5hex>|h=<crc32hex>|/``, and
:func:`shoko_linker.shoko.has_available_hashes` admits a link candidate only when that
identity is complete and valid. Placeholder strings such as ``ed2k-12`` are *malformed*
under that rule, so fixtures build real-shaped values instead. Every digest here is a
deterministic fake derived from the file id; no real media hash is represented.
"""

from __future__ import annotations

from typing import Any, Final

ED2K_SCHEME: Final[str] = "ed2k://"


def hex8(seed: int) -> str:
    return f"{seed & 0xFFFFFFFF:08x}"


def hex32(seed: int) -> str:
    return hex8(seed) * 4


def hex40(seed: int) -> str:
    return hex8(seed) * 5


def ed2k_uri(name: str, size: int, seed: int) -> str:
    """A canonical ED2K identity URI: non-empty name, positive size, 32-hex MD5, hex hashlist."""
    return f"{ED2K_SCHEME}|file|{name}|{size}|{hex32(seed)}|h={hex8(seed)}|/"


def full_hashes(file_id: int, size: int, name: str | None = None) -> dict[str, Any]:
    """Valid wire ``Hashes`` object with every field populated."""
    file_name = f"file-{file_id}.mkv" if name is None else name
    return {
        "ED2K": ed2k_uri(file_name, size, file_id),
        "SHA1": hex40(file_id),
        "CRC32": file_id & 0xFFFFFFFF,
        "MD5": hex32(file_id),
    }


def ed2k_hashes(file_id: int, size: int, name: str | None = None) -> dict[str, Any]:
    """Valid wire ``Hashes`` object: complete ED2K, numeric CRC32, SHA1/MD5 unreported."""
    file_name = f"file-{file_id}.mkv" if name is None else name
    return {
        "ED2K": ed2k_uri(file_name, size, file_id),
        "SHA1": None,
        "CRC32": file_id & 0xFFFFFFFF,
        "MD5": None,
    }
