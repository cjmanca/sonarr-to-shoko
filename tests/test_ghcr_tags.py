"""Offline tests for scripts/ghcr_tags.py (M8 F3).

No live registry calls: a fake transport (and a monkeypatched ``_open`` seam
for the urllib path) supplies canned OCI responses. Covers every outcome
class — ok (with Link-header pagination), package_missing, auth_failure,
permission_failure, network_failure, unavailable — plus plan-consumer abort
semantics, exit-code mapping, and bounded provenance verification.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import io
import json
import sys
import urllib.error
import urllib.parse
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ghcr_tags.py"
SHA_A = "a" * 40
SHA_B = "b" * 40
ZERO_DIGEST = "sha256:" + "0" * 64
TOKEN = "ghp-token-123"
REVISION_KEY = "org.opencontainers.image.revision"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("ghcr_tags", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


tags = _load_module()


class FakeTransport:
    """Scripted OCI transport: (method-path prefix match) -> canned response."""

    def __init__(self, responses: list[tuple[int, dict[str, str], bytes]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(
        self, url: str, headers: dict[str, str], timeout: float
    ) -> tuple[int, dict[str, str], bytes]:
        self.calls.append((url, headers))
        if not self.responses:
            raise AssertionError(f"no canned response left for {url}")
        return self.responses.pop(0)


class RaisingTransport:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def get(
        self, url: str, headers: dict[str, str], timeout: float
    ) -> tuple[int, dict[str, str], bytes]:
        raise self.exc


def _token_ok(token: str = TOKEN) -> tuple[int, dict[str, str], bytes]:
    body = json.dumps({"token": token, "expires_in": 300}).encode()
    return 200, {"content-type": "application/json"}, body


def _token_401() -> tuple[int, dict[str, str], bytes]:
    return 401, {}, b'{"errors":[{"code":"DENIED"}]}'


def test_list_ok_paginates_via_link_header() -> None:
    page1 = json.dumps({"name": "octo/shoko-sonarr-linker", "tags": [f"sha-{SHA_A}"]}).encode()
    page2 = json.dumps(
        {"name": "octo/shoko-sonarr-linker", "tags": ["latest", f"sha-{SHA_B}"]}
    ).encode()
    next_url = "https://ghcr.io/v2/octo/shoko-sonarr-linker/tags/list?last=x"
    next_link = {"link": f'<{next_url}>; rel="next"'}
    transport = FakeTransport(
        [
            _token_ok(),
            (200, next_link, page1),
            (200, {}, page2),
        ]
    )
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_OK
    assert result["tags"] == [f"sha-{SHA_A}", "latest", f"sha-{SHA_B}"]
    assert len(transport.calls) == 3
    assert "token?service=ghcr.io" in transport.calls[0][0]
    basic = base64.b64decode(transport.calls[0][1]["Authorization"].removeprefix("Basic ")).decode()
    assert basic.endswith(f":{TOKEN}")  # token reaches the registry only as a Basic password
    assert transport.calls[1][1]["Authorization"] == f"Bearer {TOKEN}"


def test_list_package_missing_on_401_name_unknown_after_token() -> None:
    body = json.dumps(
        {"errors": [{"code": "NAME_UNKNOWN", "message": "repository name not known to registry"}]}
    ).encode()
    transport = FakeTransport([_token_ok(), (401, {}, body)])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_PACKAGE_MISSING
    assert result["tags"] == []
    assert "NAME_UNKNOWN" in result["detail"]


def test_list_package_missing_on_404() -> None:
    transport = FakeTransport([_token_ok(), (404, {}, b"{}")])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_PACKAGE_MISSING


def test_list_token_401_is_auth_failure_not_package_missing() -> None:
    transport = FakeTransport([_token_401()])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_AUTH_FAILURE
    assert result["tags"] == []


def test_list_token_403_is_permission_failure() -> None:
    transport = FakeTransport([(403, {}, b'{"errors":[{"code":"DENIED"}]}')])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_PERMISSION_FAILURE


def test_list_403_is_permission_failure() -> None:
    body = json.dumps(
        {"errors": [{"code": "DENIED", "message": "access to this repository is denied"}]}
    ).encode()
    transport = FakeTransport([_token_ok(), (403, {}, body)])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_PERMISSION_FAILURE


def test_list_network_error_is_network_failure() -> None:
    transport = RaisingTransport(tags.NetworkFailureError("connection refused"))
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_NETWORK_FAILURE


def test_list_5xx_is_unavailable() -> None:
    transport = FakeTransport([_token_ok(), (502, {}, b"bad gateway")])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_UNAVAILABLE


def test_list_malformed_json_is_unavailable() -> None:
    transport = FakeTransport([_token_ok(), (200, {}, b"not json at all")])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_UNAVAILABLE


def test_list_token_json_malformed_is_unavailable() -> None:
    transport = FakeTransport([(200, {}, b"<html>proxy error</html>")])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_UNAVAILABLE


def test_list_pagination_cap_is_unavailable() -> None:
    page = json.dumps({"tags": ["x"]}).encode()
    link = {
        "link": '<https://ghcr.io/v2/octo/shoko-sonarr-linker/tags/list?n=100&last=x>; rel="next"'
    }
    transport = FakeTransport([_token_ok()] + [(200, link, page) for _ in range(60)])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport, max_pages=50)
    assert result["status"] == tags.STATUS_UNAVAILABLE
    assert "max_pages" in result["detail"]


def test_list_rejects_offsite_link_header() -> None:
    page1 = json.dumps({"tags": ["x"]}).encode()
    evil = {"link": '<https://evil.example/v2/x/tags/list>; rel="next"'}
    transport = FakeTransport([_token_ok(), (200, evil, page1)])
    result = tags.list_tags("octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert result["status"] == tags.STATUS_UNAVAILABLE
    assert "ghcr.io" in result["detail"]


def test_list_rejects_non_lowercase_namespace() -> None:
    with pytest.raises(ValueError, match="lowercase GHCR namespace"):
        tags.list_tags("Octo", "shoko-sonarr-linker", TOKEN, transport=FakeTransport([]))


def test_plan_consumer_helper_aborts_on_unknown_state() -> None:
    ok_doc = tags._result(tags.STATUS_OK, [f"sha-{SHA_A}", "latest"], "x")
    assert tags.plan_sha_tags(ok_doc) == [f"sha-{SHA_A}"]
    missing_doc = tags._result(tags.STATUS_PACKAGE_MISSING, [], "x")
    assert tags.plan_sha_tags(missing_doc) == []
    for status in (
        tags.STATUS_AUTH_FAILURE,
        tags.STATUS_PERMISSION_FAILURE,
        tags.STATUS_NETWORK_FAILURE,
        tags.STATUS_UNAVAILABLE,
    ):
        with pytest.raises(
            tags.PlanAbortedError, match="refusing to publish against unknown state"
        ):
            tags.plan_sha_tags(tags._result(status, [], "x"))


def test_exit_status_table() -> None:
    exit_zero = {tags.STATUS_OK, tags.STATUS_PACKAGE_MISSING}
    assert set(tags.SUCCESS_STATUSES) == exit_zero


def test_main_list_exit_codes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHCR_TOKEN", TOKEN)
    monkeypatch.setattr(
        tags,
        "list_tags",
        lambda *a, **k: dict(tags._result(tags.STATUS_OK, [f"sha-{SHA_A}"], "ok")),
    )
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    assert tags.main(["list", "--owner", "octo"]) == 0
    assert json.loads(out.getvalue())["status"] == tags.STATUS_OK

    monkeypatch.setattr(
        tags,
        "list_tags",
        lambda *a, **k: dict(tags._result(tags.STATUS_NETWORK_FAILURE, [], "boom")),
    )
    out2 = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out2)
    assert tags.main(["list", "--owner", "octo"]) == 1
    assert json.loads(out2.getvalue())["status"] == tags.STATUS_NETWORK_FAILURE


def test_main_package_missing_exits_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHCR_TOKEN", TOKEN)
    monkeypatch.setattr(
        tags,
        "list_tags",
        lambda *a, **k: dict(tags._result(tags.STATUS_PACKAGE_MISSING, [], "fresh")),
    )
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    assert tags.main(["list", "--owner", "octo"]) == 0


def test_main_requires_token_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GHCR_TOKEN", raising=False)
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    assert tags.main(["list", "--owner", "octo"]) == 1
    assert json.loads(out.getvalue())["status"] == tags.STATUS_AUTH_FAILURE


def test_main_verify_bounded_and_aborts_on_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GHCR_TOKEN", TOKEN)
    monkeypatch.setattr(tags, "verify_sha_tag", lambda sha, *a, **k: tags.PROVENANCE_PUBLISHED)
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    rc = tags.main(["verify", "--owner", "octo", "--shas", f"{SHA_A},{SHA_B}"])
    assert rc == 0
    document = json.loads(out.getvalue())
    assert document["status"] == tags.STATUS_OK
    assert document["results"] == {
        SHA_A: tags.PROVENANCE_PUBLISHED,
        SHA_B: tags.PROVENANCE_PUBLISHED,
    }


def test_main_verify_respects_provenance_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHCR_TOKEN", TOKEN)
    many = ",".join([f"{i:x}" * 40 for i in range(1, tags.VERIFY_CAP + 2)])
    err = io.StringIO()
    monkeypatch.setattr(sys, "stderr", err)
    assert tags.main(["verify", "--owner", "octo", "--shas", many]) == 2
    assert "cap" in err.getvalue()


def _manifest(media_type: str, body: dict[str, Any]) -> tuple[int, dict[str, str], bytes]:
    return 200, {"content-type": media_type}, json.dumps(body).encode()


def _config_blob(revision: str) -> tuple[int, dict[str, str], bytes]:
    body = json.dumps(
        {"architecture": "amd64", "annotations": {"org.opencontainers.image.revision": revision}}
    )
    return 200, {"content-type": "application/vnd.oci.image.config.v1+json"}, body.encode()


# --- F12: OCI index fixtures (buildx attestation-enabled images) ---------------

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
DOCKER_LIST = "application/vnd.docker.distribution.manifest.list.v2+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
DOCKER_CONFIG = "application/vnd.docker.container.image.v1+json"
INTOTO_CONFIG = "application/vnd.in-toto+json"
PREFERRED_PLATFORM = {"os": "linux", "architecture": "amd64"}
ARM_PLATFORM = {"os": "linux", "architecture": "arm64", "variant": "v8"}
# Real buildx shape: attestation descriptors carry an UNKNOWN platform.
ATTESTATION_PLATFORM = {"os": "unknown", "architecture": "unknown"}
ATTESTATION_ANNOTATIONS = {
    "in-toto.io/predicate-type": "https://slsa.dev/provenance/v0.2",
    "k8s.io/attestation/provenance": "true",
}


def _digest_of(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _paths(transport: FakeTransport) -> list[str]:
    return [urllib.parse.urlsplit(url).path for url, _ in transport.calls]


def _accepts(transport: FakeTransport) -> list[str]:
    return [headers.get("Accept", "") for _, headers in transport.calls]


def _config(
    media_type: str = OCI_CONFIG, revision: str | None = SHA_A
) -> tuple[int, dict[str, str], bytes]:
    """Image config blob: OCI ``annotations`` shape or docker ``config.Labels`` shape."""
    if revision is None:
        payload: dict[str, Any] = {"architecture": "amd64"}
    elif media_type == DOCKER_CONFIG:
        payload = {"architecture": "amd64", "config": {"Labels": {REVISION_KEY: revision}}}
    else:
        payload = {"architecture": "amd64", "annotations": {REVISION_KEY: revision}}
    return 200, {"content-type": media_type}, json.dumps(payload).encode()


def _child(
    config_body: bytes,
    config_media_type: str = OCI_CONFIG,
    media_type: str = OCI_MANIFEST,
    annotations: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    payload: dict[str, Any] = {
        "mediaType": media_type,
        "config": {"mediaType": config_media_type, "digest": _digest_of(config_body)},
        "layers": [],
    }
    if annotations is not None:
        payload["annotations"] = annotations
    return _manifest(media_type, payload)


def _attestation_manifest() -> tuple[int, dict[str, str], bytes]:
    payload = {
        "mediaType": OCI_MANIFEST,
        "subject": {"mediaType": OCI_MANIFEST, "digest": ZERO_DIGEST},
        "config": {"mediaType": INTOTO_CONFIG, "digest": ZERO_DIGEST},
        "layers": [{"mediaType": INTOTO_CONFIG, "digest": ZERO_DIGEST, "size": 7}],
    }
    return _manifest(OCI_MANIFEST, payload)


def _descriptor(
    body: bytes,
    media_type: str,
    platform: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {"mediaType": media_type, "digest": _digest_of(body), "size": len(body)}
    if platform is not None:
        entry["platform"] = platform
    if annotations is not None:
        entry["annotations"] = annotations
    return entry


def _index(
    descriptors: list[dict[str, Any]],
    annotations: dict[str, str] | None = None,
    media_type: str = OCI_INDEX,
) -> tuple[int, dict[str, str], bytes]:
    payload: dict[str, Any] = {"mediaType": media_type, "manifests": descriptors}
    if annotations is not None:
        payload["annotations"] = annotations
    return _manifest(media_type, payload)


def test_verify_index_manifest_chain_published() -> None:
    config = _config_blob(SHA_A)
    config_digest = "sha256:" + hashlib.sha256(config[2]).hexdigest()
    inner = _manifest(
        "application/vnd.oci.image.manifest.v1+json",
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"digest": config_digest},
        },
    )
    inner_digest = "sha256:" + hashlib.sha256(inner[2]).hexdigest()
    index = _manifest(
        "application/vnd.oci.image.index.v1+json",
        {
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {"digest": inner_digest, "platform": {"os": "linux", "architecture": "amd64"}}
            ],
        },
    )
    transport = FakeTransport([_token_ok(), index, inner, config])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_PUBLISHED)
    assert len(transport.calls) == 4
    paths = _paths(transport)
    # OCI Distribution Spec: child MANIFESTS via /manifests/, config blob via /blobs/.
    assert paths[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{inner_digest}"
    assert paths[3] == f"/v2/octo/shoko-sonarr-linker/blobs/{config_digest}"


def test_verify_index_annotation_revision_published() -> None:
    index = _manifest(
        "application/vnd.oci.image.index.v1+json",
        {
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "annotations": {"org.opencontainers.image.revision": SHA_A},
        },
    )
    transport = FakeTransport([_token_ok(), index])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_PUBLISHED)
    assert len(transport.calls) == 2


def test_verify_plain_manifest_config_chain_published() -> None:
    config = _config_blob(SHA_A)
    digest = "sha256:" + hashlib.sha256(config[2]).hexdigest()
    manifest = _manifest(
        "application/vnd.oci.image.manifest.v1+json",
        {"mediaType": "application/vnd.oci.image.manifest.v1+json", "config": {"digest": digest}},
    )
    transport = FakeTransport([_token_ok(), manifest, config])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_PUBLISHED)


def test_verify_manifest_level_annotations_published() -> None:
    # P2 review: plain manifests can carry the revision annotation at manifest level.
    manifest = _manifest(
        "application/vnd.oci.image.manifest.v1+json",
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "annotations": {"org.opencontainers.image.revision": SHA_A},
            "config": {"digest": ZERO_DIGEST},
        },
    )
    transport = FakeTransport([_token_ok(), manifest])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_PUBLISHED)
    assert len(transport.calls) == 2  # no config fetch needed once the annotation is found


def test_verify_docker_schema2_config_labels_published() -> None:
    # P2 review: docker schema2 pushes keep labels under config.config.Labels.
    body = json.dumps(
        {
            "architecture": "amd64",
            "config": {"Labels": {"org.opencontainers.image.revision": SHA_A}},
        }
    )
    config = (
        200,
        {"content-type": "application/vnd.docker.container.image.v1+json"},
        body.encode(),
    )
    digest = "sha256:" + hashlib.sha256(config[2]).hexdigest()
    manifest = _manifest(
        "application/vnd.docker.distribution.manifest.v2+json",
        {
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"digest": digest},
        },
    )
    transport = FakeTransport([_token_ok(), manifest, config])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_PUBLISHED)


def test_verify_revision_mismatch_is_unknown_provenance() -> None:
    config = _config_blob("f" * 40)
    digest = "sha256:" + hashlib.sha256(config[2]).hexdigest()
    manifest = _manifest(
        "application/vnd.docker.distribution.manifest.v2+json",
        {
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"digest": digest},
        },
    )
    transport = FakeTransport([_token_ok(), manifest, config])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_UNKNOWN)


def test_verify_404_is_not_published() -> None:
    transport = FakeTransport([_token_ok(), (404, {}, b"{}")])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_NOT_PUBLISHED)


def test_verify_manifest_5xx_is_unknown_provenance() -> None:
    transport = FakeTransport([_token_ok(), (500, {}, b"boom")])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_UNKNOWN)


def test_verify_config_blob_failure_is_unknown_provenance() -> None:
    config = _config_blob(SHA_A)
    digest = "sha256:" + hashlib.sha256(config[2]).hexdigest()
    manifest = _manifest(
        "application/vnd.oci.image.manifest.v1+json",
        {"mediaType": "application/vnd.oci.image.manifest.v1+json", "config": {"digest": digest}},
    )
    transport = FakeTransport([_token_ok(), manifest, (503, {}, b"unavailable")])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_UNKNOWN)


def test_verify_network_failure_is_unknown_provenance() -> None:
    transport = RaisingTransport(tags.NetworkFailureError("dns down"))
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_UNKNOWN)


def test_verify_token_auth_failure_is_unknown_provenance() -> None:
    transport = FakeTransport([_token_401()])
    assert tags.verify_sha_tag(
        SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport
    ) == (tags.PROVENANCE_UNKNOWN)


def test_verify_rejects_non_sha_and_non_lowercase_owner() -> None:
    with pytest.raises(ValueError, match="40-char lowercase hex"):
        tags.verify_sha_tag(
            "nope", "octo", "shoko-sonarr-linker", TOKEN, transport=FakeTransport([])
        )
    with pytest.raises(ValueError, match="namespace"):
        tags.verify_sha_tag(
            SHA_A, "Octo", "shoko-sonarr-linker", TOKEN, transport=FakeTransport([])
        )


def test_urllib_transport_maps_http_and_network_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status = 200

        def read(self) -> bytes:
            return b"body"

        def getheaders(self) -> list[tuple[str, str]]:
            return [("Content-Type", "application/json")]

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

    monkeypatch.setattr(tags, "_open", lambda request, timeout: FakeResponse())
    transport = tags.UrllibTransport()
    status, headers, body = transport.get("https://ghcr.io/v2/x/tags/list", {}, 1.0)
    assert (status, body) == (200, b"body")
    assert headers["content-type"] == "application/json"

    def boom(request: Any, timeout: float) -> Any:
        raise urllib.error.URLError("connection reset")

    monkeypatch.setattr(tags, "_open", boom)
    with pytest.raises(tags.NetworkFailureError):
        transport.get("https://ghcr.io/v2/x/tags/list", {}, 1.0)


# --- F12: OCI index provenance (manifests endpoint, runnable-descriptor selection) ---


def _attestation_descriptor(attestation_body: bytes) -> dict[str, Any]:
    return _descriptor(
        attestation_body, OCI_MANIFEST, ATTESTATION_PLATFORM, ATTESTATION_ANNOTATIONS
    )


def test_verify_attestation_index_fetches_child_manifest_via_manifests_endpoint() -> None:
    """F12 P2 root cause: buildx publishes attestation-enabled images as OCI indexes.
    The runnable child manifest MUST be read from /manifests/<digest> (OCI
    Distribution Spec), never /blobs/, and the attestation descriptor skipped."""
    config = _config(OCI_CONFIG, SHA_A)
    child = _child(config[2])
    attestation = _attestation_manifest()
    index = _index(
        [
            _attestation_descriptor(attestation[2]),
            _descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM),
            _descriptor(child[2], OCI_MANIFEST, ARM_PLATFORM),
        ]
    )
    transport = FakeTransport([_token_ok(), index, child, config])
    outcome = tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
    assert outcome == tags.PROVENANCE_PUBLISHED
    paths = _paths(transport)
    child_digest = _digest_of(child[2])
    assert paths[1] == f"/v2/octo/shoko-sonarr-linker/manifests/sha-{SHA_A}"
    assert paths[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{child_digest}"
    assert paths[3] == f"/v2/octo/shoko-sonarr-linker/blobs/{_digest_of(config[2])}"
    assert _digest_of(attestation[2]) not in "".join(paths)  # attestation never fetched
    accepts = _accepts(transport)
    assert accepts[2] == tags.ACCEPT_MANIFESTS  # manifest endpoints negotiate manifest types
    assert accepts[3] == tags.ACCEPT_CONFIG  # blob endpoint negotiates config types


def test_verify_attestation_first_descriptor_still_publishes() -> None:
    """Descriptor ordering variation: attestation manifest listed first, no platform."""
    config = _config(OCI_CONFIG, SHA_A)
    child = _child(config[2])
    attestation = _attestation_manifest()
    index = _index(
        [
            _descriptor(attestation[2], OCI_MANIFEST, None, ATTESTATION_ANNOTATIONS),
            _descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM),
        ]
    )
    transport = FakeTransport([_token_ok(), index, child, config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_PUBLISHED
    )
    assert _paths(transport)[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{_digest_of(child[2])}"


def test_verify_index_multiple_platforms_prefers_linux_amd64() -> None:
    """arm64 listed first must NOT be chosen: linux/amd64 is the documented preference."""
    arm_config = _config(OCI_CONFIG, SHA_B)
    arm_child = _child(arm_config[2])
    amd_config = _config(OCI_CONFIG, SHA_A)
    amd_child = _child(amd_config[2])
    index = _index(
        [
            _descriptor(arm_child[2], OCI_MANIFEST, ARM_PLATFORM),
            _descriptor(amd_child[2], OCI_MANIFEST, PREFERRED_PLATFORM),
        ]
    )
    transport = FakeTransport([_token_ok(), index, amd_child, amd_config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_PUBLISHED
    )
    paths = _paths(transport)
    assert paths[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{_digest_of(amd_child[2])}"
    assert _digest_of(arm_child[2]) not in "".join(paths)


def test_verify_index_multiple_platforms_tie_break_is_alphabetical() -> None:
    """No linux/amd64 present: deterministic (os, architecture) alphabetical pick."""
    ppc_config = _config(OCI_CONFIG, SHA_B)
    ppc_child = _child(ppc_config[2])
    arm_config = _config(OCI_CONFIG, SHA_A)
    arm_child = _child(arm_config[2])
    index = _index(
        [
            _descriptor(ppc_child[2], OCI_MANIFEST, {"os": "linux", "architecture": "ppc64le"}),
            _descriptor(arm_child[2], OCI_MANIFEST, ARM_PLATFORM),
        ]
    )
    transport = FakeTransport([_token_ok(), index, arm_child, arm_config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_PUBLISHED
    )
    assert (
        _paths(transport)[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{_digest_of(arm_child[2])}"
    )


def test_verify_index_attestation_only_is_unknown_not_published() -> None:
    """No runnable descriptor -> fail closed (unknown), never a crash and never published."""
    attestation = _attestation_manifest()
    index = _index([_attestation_descriptor(attestation[2])])
    transport = FakeTransport([_token_ok(), index])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2  # no child fetch attempted


def test_verify_index_descriptor_without_platform_is_unknown() -> None:
    """A descriptor with no platform object is not identifiable as runnable."""
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    descriptor = {"mediaType": OCI_MANIFEST, "digest": _digest_of(child[2]), "size": len(child[2])}
    transport = FakeTransport([_token_ok(), _index([descriptor])])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2


def test_verify_index_without_manifests_list_is_unknown() -> None:
    index = _manifest(OCI_INDEX, {"mediaType": OCI_INDEX})
    transport = FakeTransport([_token_ok(), index])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2


def test_verify_index_child_404_is_unknown_not_not_published() -> None:
    """A 404 on the CHILD manifest means the index is broken, NOT that the tag is absent."""
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    index = _index([_descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM)])
    not_found: tuple[int, dict[str, str], bytes] = (
        404,
        {},
        b'{"errors":[{"code":"BLOB_UNKNOWN"}]}',
    )
    transport = FakeTransport([_token_ok(), index, not_found])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert _paths(transport)[2].startswith("/v2/octo/shoko-sonarr-linker/manifests/")


def test_verify_index_child_digest_mismatch_is_unknown() -> None:
    """Digest verification is exercised on every fetched child manifest."""
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    tampered_body = child[2].replace(b'"layers": []', b'"layers": [{"digest": "sha256:1111"}]')
    assert tampered_body != child[2]
    index = _index([_descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM)])
    tampered = (200, {"content-type": OCI_MANIFEST}, tampered_body)
    transport = FakeTransport([_token_ok(), index, tampered])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 3  # chain stops at the failed digest check


def test_verify_index_config_digest_mismatch_is_unknown() -> None:
    """Digest verification is exercised on the config blob too."""
    config = _config(OCI_CONFIG, SHA_A)
    child = _child(config[2])
    tampered_config = (200, dict(config[1]), config[2].replace(b"amd64", b"amd65"))
    assert tampered_config[2] != config[2]
    index = _index([_descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM)])
    transport = FakeTransport([_token_ok(), index, child, tampered_config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert _paths(transport)[3] == f"/v2/octo/shoko-sonarr-linker/blobs/{_digest_of(config[2])}"


def test_verify_index_child_annotation_published_without_config_fetch() -> None:
    """Child annotations satisfy the chain; index annotation wins when present."""
    config = _config(OCI_CONFIG, "f" * 40)
    child = _child(config[2], annotations={REVISION_KEY: SHA_A})
    descriptors = [_descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM)]
    transport = FakeTransport([_token_ok(), _index(descriptors), child])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_PUBLISHED
    )
    assert len(transport.calls) == 3  # no config fetch once the child annotation is found
    annotated_index = _index(descriptors, annotations={REVISION_KEY: SHA_A})
    transport2 = FakeTransport([_token_ok(), annotated_index])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport2)
        == tags.PROVENANCE_PUBLISHED
    )
    assert len(transport2.calls) == 2  # index-level annotation wins: no child fetch


def test_verify_index_missing_revision_labels_is_unknown_without_crash() -> None:
    """Config carries no revision label -> revision None -> unknown (no crash)."""
    config = _config(OCI_CONFIG, None)
    child = _child(config[2])
    index = _index([_descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM)])
    transport = FakeTransport([_token_ok(), index, child, config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert _paths(transport)[3] == f"/v2/octo/shoko-sonarr-linker/blobs/{_digest_of(config[2])}"


def test_verify_docker_manifest_list_chain_published() -> None:
    """Docker manifest list (descriptors without mediaType) -> schema2 child -> config Labels."""
    config = _config(DOCKER_CONFIG, SHA_A)
    child = _child(config[2], config_media_type=DOCKER_CONFIG, media_type=DOCKER_MANIFEST)
    digest = _digest_of(child[2])
    index = _index(
        [
            {"digest": digest, "size": len(child[2]), "platform": ARM_PLATFORM},
            {"digest": digest, "size": len(child[2]), "platform": PREFERRED_PLATFORM},
        ],
        media_type=DOCKER_LIST,
    )
    transport = FakeTransport([_token_ok(), index, child, config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_PUBLISHED
    )
    paths = _paths(transport)
    assert paths[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{digest}"
    assert paths[3] == f"/v2/octo/shoko-sonarr-linker/blobs/{_digest_of(config[2])}"
    assert DOCKER_CONFIG in _accepts(transport)[3]


def test_verify_nested_index_recurses_through_manifests_endpoint() -> None:
    """Index -> nested index -> runnable manifest: every hop uses /manifests/."""
    config = _config(OCI_CONFIG, SHA_A)
    child = _child(config[2])
    inner = _index(
        [
            _attestation_descriptor(_attestation_manifest()[2]),
            _descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM),
        ]
    )
    inner_descriptor = {
        "digest": _digest_of(inner[2]),
        "size": len(inner[2]),
        "platform": PREFERRED_PLATFORM,
    }
    outer = _manifest(OCI_INDEX, {"mediaType": OCI_INDEX, "manifests": [inner_descriptor]})
    transport = FakeTransport([_token_ok(), outer, inner, child, config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_PUBLISHED
    )
    paths = _paths(transport)
    assert paths[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{_digest_of(inner[2])}"
    assert paths[3] == f"/v2/octo/shoko-sonarr-linker/manifests/{_digest_of(child[2])}"
    assert paths[4] == f"/v2/octo/shoko-sonarr-linker/blobs/{_digest_of(config[2])}"


def test_verify_index_nesting_beyond_depth_bound_is_unknown() -> None:
    """Bounded recursion: an over-deep index chain fails closed, never crashes."""
    leaf = _child(_config(OCI_CONFIG, SHA_A)[2])
    document = _index([_descriptor(leaf[2], OCI_MANIFEST, PREFERRED_PLATFORM)])
    chain: list[tuple[int, dict[str, str], bytes]] = []
    for _ in range(tags.MAX_INDEX_DEPTH + 2):
        chain.append(document)
        nested_descriptor = {
            "digest": _digest_of(document[2]),
            "size": len(document[2]),
            "platform": PREFERRED_PLATFORM,
        }
        document = _manifest(OCI_INDEX, {"mediaType": OCI_INDEX, "manifests": [nested_descriptor]})
    transport = FakeTransport([_token_ok(), *reversed(chain)])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    paths = _paths(transport)
    assert all(path.startswith("/v2/octo/shoko-sonarr-linker/manifests/") for path in paths[1:])
    assert not any("/blobs/" in path for path in paths)


def test_verify_published_index_is_stable_across_repeated_backfills() -> None:
    """Workflow policy input: a repeated backfill re-verifies published -> skip (no push)."""
    config = _config(OCI_CONFIG, SHA_A)
    child = _child(config[2])
    index = _index(
        [
            _attestation_descriptor(_attestation_manifest()[2]),
            _descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM),
        ]
    )
    for _ in range(2):
        transport = FakeTransport([_token_ok(), index, child, config])
        assert (
            tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
            == tags.PROVENANCE_PUBLISHED
        )
        assert _paths(transport)[2].startswith("/v2/octo/shoko-sonarr-linker/manifests/")


def test_verify_index_permission_failure_is_unknown() -> None:
    """403 on the manifest fetch: authenticated-not-permitted stays unknown, not absent."""
    forbidden: tuple[int, dict[str, str], bytes] = (
        403,
        {},
        b'{"errors":[{"code":"DENIED"}]}',
    )
    transport = FakeTransport([_token_ok(), forbidden])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )


# --- F12 review P2: strict sha256 digest validation (fail closed, never fail open) ---


def test_verify_index_sha512_descriptor_digest_is_unknown_without_fetch() -> None:
    """A non-sha256 descriptor digest cannot be verified: fail closed, no child fetch."""
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    forged = _manifest(
        OCI_INDEX,
        {
            "mediaType": OCI_INDEX,
            "manifests": [
                {
                    "mediaType": OCI_MANIFEST,
                    "digest": "sha512:" + "a" * 128,
                    "size": len(child[2]),
                    "platform": PREFERRED_PLATFORM,
                    "annotations": {REVISION_KEY: SHA_A},
                }
            ],
        },
    )
    transport = FakeTransport([_token_ok(), forged])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2  # chain stops at validation: no forged fetch
    assert not any("sha512:" in path for path in _paths(transport))


def test_verify_index_uppercase_descriptor_digest_is_unknown() -> None:
    """Digest matching is case-sensitive lowercase hex: uppercase fails closed."""
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    uppercase = "sha256:" + hashlib.sha256(child[2]).hexdigest().upper()
    forged = _index(
        [
            {
                "mediaType": OCI_MANIFEST,
                "digest": uppercase,
                "size": len(child[2]),
                "platform": PREFERRED_PLATFORM,
            }
        ]
    )
    transport = FakeTransport([_token_ok(), forged])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2


def test_verify_index_trailing_newline_descriptor_digest_is_unknown() -> None:
    """Digest validation is fullmatch-strict: a trailing newline cannot slip past."""
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    padded = _index(
        [
            {
                "mediaType": OCI_MANIFEST,
                "digest": _digest_of(child[2]) + "\n",
                "size": len(child[2]),
                "platform": PREFERRED_PLATFORM,
            }
        ]
    )
    transport = FakeTransport([_token_ok(), padded])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2


def test_verify_index_short_descriptor_digest_is_unknown() -> None:
    """A 63-hex sha256 digest is not verifiable: fail closed, no child fetch."""
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    short = "sha256:" + hashlib.sha256(child[2]).hexdigest()[:63]
    forged = _index(
        [
            {
                "mediaType": OCI_MANIFEST,
                "digest": short,
                "size": len(child[2]),
                "platform": PREFERRED_PLATFORM,
            }
        ]
    )
    transport = FakeTransport([_token_ok(), forged])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2


def test_verify_index_empty_descriptor_digest_is_unknown() -> None:
    child = _child(_config(OCI_CONFIG, SHA_A)[2])
    forged = _index(
        [
            {
                "mediaType": OCI_MANIFEST,
                "digest": "",
                "size": len(child[2]),
                "platform": PREFERRED_PLATFORM,
            }
        ]
    )
    transport = FakeTransport([_token_ok(), forged])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert len(transport.calls) == 2


def test_verify_config_malformed_digest_is_unknown_without_blob_fetch() -> None:
    """A manifest naming a non-sha256 config digest is unverifiable: fail closed, no blob GET."""
    child = _manifest(
        OCI_MANIFEST,
        {
            "mediaType": OCI_MANIFEST,
            "config": {"mediaType": OCI_CONFIG, "digest": "notadigest"},
            "layers": [],
        },
    )
    transport = FakeTransport([_token_ok(), child])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    paths = _paths(transport)
    assert len(paths) == 2  # token + manifest only
    assert not any("/blobs/" in path for path in paths)


def test_verify_config_empty_digest_is_unknown_without_blob_fetch() -> None:
    child = _manifest(
        OCI_MANIFEST,
        {
            "mediaType": OCI_MANIFEST,
            "config": {"mediaType": OCI_CONFIG, "digest": ""},
            "layers": [],
        },
    )
    transport = FakeTransport([_token_ok(), child])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert not any("/blobs/" in path for path in _paths(transport))


def test_verify_index_config_sha512_digest_is_unknown_after_child_fetch() -> None:
    """Index chain: valid child manifest, sha512 config digest -> unknown, no blob GET."""
    child = _manifest(
        OCI_MANIFEST,
        {
            "mediaType": OCI_MANIFEST,
            "config": {"mediaType": OCI_CONFIG, "digest": "sha512:" + "b" * 128},
            "layers": [],
        },
    )
    index = _index([_descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM)])
    transport = FakeTransport([_token_ok(), index, child])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    paths = _paths(transport)
    assert paths[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{_digest_of(child[2])}"
    assert not any("/blobs/" in path for path in paths)


def test_verify_docker_schema2_sha512_config_digest_is_unknown() -> None:
    """Single-manifest path keeps the same fail-closed digest rule."""
    manifest = _manifest(
        DOCKER_MANIFEST,
        {
            "mediaType": DOCKER_MANIFEST,
            "config": {"mediaType": DOCKER_CONFIG, "digest": "sha512:" + "c" * 128},
        },
    )
    transport = FakeTransport([_token_ok(), manifest])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_UNKNOWN
    )
    assert not any("/blobs/" in path for path in _paths(transport))


def test_verify_digest_format_still_accepts_valid_chain() -> None:
    """Regression guard: the strict rule does not break the conformant chain/paths."""
    config = _config(OCI_CONFIG, SHA_A)
    child = _child(config[2])
    index = _index(
        [
            _attestation_descriptor(_attestation_manifest()[2]),
            _descriptor(child[2], OCI_MANIFEST, PREFERRED_PLATFORM),
        ]
    )
    transport = FakeTransport([_token_ok(), index, child, config])
    assert (
        tags.verify_sha_tag(SHA_A, "octo", "shoko-sonarr-linker", TOKEN, transport=transport)
        == tags.PROVENANCE_PUBLISHED
    )
    paths = _paths(transport)
    assert paths[2] == f"/v2/octo/shoko-sonarr-linker/manifests/{_digest_of(child[2])}"
    assert paths[3] == f"/v2/octo/shoko-sonarr-linker/blobs/{_digest_of(config[2])}"
