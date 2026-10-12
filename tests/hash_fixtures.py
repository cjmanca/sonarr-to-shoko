"""Deterministic, Shoko-faithful hash fixtures (F13/F16).

Shoko 5.3.3 reports file hashes on the wire as **raw hexadecimal strings** in a
PascalCase ``Hashes`` object — never ``ed2k://`` URIs. ``HashesDict.ED2K`` is the
AniDB base hash (the MD5 of the file content): 32 hex chars, stored uppercase by
``HashFileJob`` (``vlocal.Hash = hashes.ED2K.ToUpperInvariant()``). ``SHA1``,
``MD5`` and ``CRC32`` are nullable strings of 40, 32 and 8 hex chars respectively,
and are reported as ``null`` when Shoko has not computed them.
:func:`shoko_linker.shoko.has_available_hashes` admits a link candidate only when
ED2K is a complete 32-hex value and every *populated* optional field is a plausible
hex string. ``ed2k://`` URIs and placeholder strings such as ``ed2k-12`` are
therefore *malformed* under that rule, so fixtures build real-shaped values instead.
Every digest here is a deterministic fake derived from the file id; no real media
hash is represented.
"""

from __future__ import annotations

from typing import Any


def hex8(seed: int) -> str:
    """8-hex CRC32 string, uppercase (Shoko stores ``ToUpperInvariant``)."""
    return f"{seed & 0xFFFFFFFF:08X}"


def hex32(seed: int) -> str:
    """32-hex string used for ED2K (AniDB base hash) and MD5, uppercase."""
    return hex8(seed) * 4


def hex40(seed: int) -> str:
    """40-hex SHA1 string, uppercase."""
    return hex8(seed) * 5


def full_hashes(file_id: int, size: int, name: str | None = None) -> dict[str, Any]:
    """Valid wire ``Hashes`` object with every field populated as raw hex strings.

    ``size`` and ``name`` are accepted for call-site compatibility; Shoko's hash
    fields are content digests independent of the file name/size reported elsewhere.
    """
    return {
        "ED2K": hex32(file_id),
        "SHA1": hex40(file_id),
        "CRC32": hex8(file_id),
        "MD5": hex32(file_id),
    }


def ed2k_hashes(file_id: int, size: int, name: str | None = None) -> dict[str, Any]:
    """Valid wire ``Hashes`` object: a complete 32-hex ED2K with SHA1/CRC32/MD5 unreported.

    This is the realistic "fully hashed, optional hashes not computed" shape:
    ``{"ED2K": "<32 hex>", "SHA1": null, "CRC32": null, "MD5": null}``.
    """
    return {
        "ED2K": hex32(file_id),
        "SHA1": None,
        "CRC32": None,
        "MD5": None,
    }
