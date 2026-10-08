"""Drain and restart a launchd Worker, optionally updating its code."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
from pathlib import Path
from urllib.parse import quote, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .config import WorkerConfig
from .http import _USER_AGENT, ApiError, HttpClient


_REVISION = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_PACKAGE_BYTES = 128 * 1024 * 1024
_CONTENT_RANGE = re.compile(r"^bytes ([0-9]+)-([0-9]+)/([0-9]+)$")


class _RetryablePackageError(RuntimeError):
    """A complete request was unusable but a fresh request may recover."""


def _origin(value: str) -> tuple[str, str, int]:
    parsed = urlparse(value)
    port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    return parsed.scheme.lower(), (parsed.hostname or "").lower(), port


class _PackageRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and _origin(newurl) != _origin(req.full_url):
            for header in (
                "Authorization", "CF-Access-Client-Id", "CF-Access-Client-Secret",
            ):
                redirected.remove_header(header)
        return redirected


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _git(repo: Path, *args: str, capture: bool = False) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], check=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
    )
    return (result.stdout or "").strip()


def _git_z(repo: Path, *args: str) -> set[str]:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return {
        item.decode("utf-8")
        for item in result.stdout.split(b"\0") if item
    }




def _installation_path(value: str | Path, label: str) -> Path:
    """Keep cwd-relative semantics, allowing lexical .. but never symlink traversal."""
    raw = Path(value)
    if not raw.is_absolute():
        raw = Path.cwd() / raw
    # Check the original traversal as well: normalizing link/.. first would
    # hide a symlink and could change the directory the caller actually meant.
    prefix = Path(raw.anchor)
    for part in raw.parts[1:]:
        prefix = prefix / part
        if prefix.is_symlink():
            raise RuntimeError(f"{label} must not use symlinks")
    return Path(os.path.abspath(raw))


def _program_config_paths(repo: Path) -> set[str]:
    """Exempt only the default and actual configured files in program trees."""
    repo = _installation_path(repo, "installation root")
    default = repo / "client/worker.json"
    config = _installation_path(os.environ.get("H3WORKER_CONFIG", str(default)), "Worker config")
    paths = {"client/worker.json"}
    if config.is_relative_to(repo):
        relative = config.relative_to(repo)
        if len(relative.parts) >= 2 and relative.parts[0] in ("client", "shared"):
            paths.add(relative.as_posix())
    elif any(parent.name in ("client", "shared") for parent in config.parents):
        raise RuntimeError("Worker config belongs to another installation root")
    for relative in paths:
        path = _installation_path(repo / relative, "Worker config")
        if path.exists() and not path.is_file():
            raise RuntimeError("Worker config must be a regular file")
    return paths

def _code_digest(repo: Path) -> str:
    """Fingerprint program files, excluding preserved config and Python caches."""
    repo = _installation_path(repo, "installation root")
    digest = hashlib.sha256()
    configs = _program_config_paths(repo)
    for tree in ("client", "shared", "requirements.txt"):
        base = repo / tree
        paths = sorted(base.rglob("*")) if base.is_dir() else [base]
        for path in [base, *paths] if base.is_dir() else paths:
            if path.is_symlink():
                raise RuntimeError(f"symlink in Worker program tree: {path}")
            relative = path.relative_to(repo)
            if "__pycache__" in relative.parts or path.suffix == ".pyc" \
                    or relative.as_posix() in configs:
                continue
            if path.is_file():
                digest.update(str(relative).encode() + b"\0")
                digest.update(path.read_bytes())
                digest.update(b"\0" + str(path.stat().st_mode & 0o111).encode())
    return digest.hexdigest()


def _installation_metadata(repo: Path, data_dir: str) -> Path:
    repo = _installation_path(repo, "installation root")
    data = _installation_path(data_dir, "installation data directory")
    if any(data.is_relative_to(repo / tree) for tree in ("client", "shared")):
        raise RuntimeError("installation metadata must be outside program trees")
    return data / "installation.json"


def classify_installation(repo: Path, data_dir: str) -> str:
    """Return git, package or mixed; invalid identity evidence fails closed."""
    repo = _installation_path(repo, "installation root")
    if (repo / "client").is_symlink() or (repo / "shared").is_symlink():
        raise RuntimeError("Worker program roots must not be symlinks")
    digest = _code_digest(repo)
    metadata = _installation_metadata(repo, data_dir)
    pending = metadata.with_name("installation-update-pending.json")
    if pending.exists() or pending.is_symlink():
        raise RuntimeError("incomplete Git update; local operator verification required")
    identity = None
    if metadata.is_symlink():
        raise RuntimeError("installation metadata must not be a symlink")
    if metadata.exists():
        try:
            identity = json.loads(metadata.read_text())
            if not isinstance(identity, dict) or set(identity) != {
                "schema", "source", "revision", "root", "code_sha256",
            } or type(identity["schema"]) is not int or identity["schema"] != 1 \
                    or identity["source"] not in ("git", "package") \
                    or not isinstance(identity["revision"], str) \
                    or not _REVISION.fullmatch(identity["revision"]) \
                    or identity["root"] != str(repo) \
                    or identity["code_sha256"] != digest:
                raise ValueError("invalid or stale identity")
        except (ValueError, TypeError, OSError) as exc:
            raise RuntimeError("invalid, stale or cross-root installation metadata") from exc
    git_dir = repo / ".git"
    if git_dir.is_symlink():
        raise RuntimeError("symlink Git metadata is unsafe")
    if not git_dir.exists():
        if identity and identity["source"] == "git":
            raise RuntimeError("Git installation metadata has no checkout")
        return "package"
    try:
        if Path(_git(repo, "rev-parse", "--show-toplevel", capture=True)).resolve() != repo:
            raise RuntimeError("Git root does not match Worker root")
        head = _git(repo, "rev-parse", "HEAD", capture=True)
        dirty = _git(repo, "status", "--porcelain", "--untracked-files=no", capture=True)
        if identity and identity["source"] == "package":
            # Unrelated tracked edits must not be hidden by package identity.
            changed = _git_z(repo, "diff", "--name-only", "-z", "HEAD", "--")
            if _git(repo, "diff", "--cached", "--name-only", capture=True) or any(p != "requirements.txt" and not p.startswith(("client/", "shared/")) for p in changed):
                return "mixed"
            return "package"
        if identity and identity["revision"] != head:
            raise RuntimeError("installation revision does not match Git HEAD")
        extras = _git_z(repo, "ls-files", "--others", "--exclude-standard", "-z", "--", "client", "shared")
        configs = _program_config_paths(repo)
        extras = {p for p in extras if p not in configs
                  and "__pycache__" not in Path(p).parts and not p.endswith(".pyc")}
        return "mixed" if dirty or extras else "git"
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("invalid Git installation metadata") from exc


def _write_installation_record(path: Path, identity: dict) -> None:
    """Atomically persist non-secret identity, restoring it on write failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise RuntimeError("installation metadata must not be a symlink")
    previous = path.read_bytes() if path.exists() else None
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    fd, name = tempfile.mkstemp(prefix=".installation-", dir=path.parent)
    published = False
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(identity, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(name, path)
        published = True
        os.fsync(directory)
    except Exception:
        if published:
            if previous is None:
                path.unlink()
            else:
                with open(name, "wb") as output:
                    output.write(previous)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(name, path)
            os.fsync(directory)
        raise
    finally:
        os.close(directory)
        Path(name).unlink(missing_ok=True)


def write_installation_metadata(repo: Path, data_dir: str, source: str, revision: str) -> None:
    if source not in ("git", "package") or not _REVISION.fullmatch(revision):
        raise ValueError("installation identity requires an exact revision and source")
    path = _installation_metadata(repo, data_dir)
    _write_installation_record(path, dict(
        schema=1, source=source, revision=revision,
        root=str(repo.resolve()), code_sha256=_code_digest(repo),
    ))


def mark_git_update_pending(repo: Path, data_dir: str, revision: str) -> None:
    """Keep a failed legacy overlay conversion from becoming implicit Git mode."""
    if not _REVISION.fullmatch(revision):
        raise ValueError("Git update requires an exact revision")
    path = _installation_metadata(repo, data_dir).with_name("installation-update-pending.json")
    _write_installation_record(path, dict(schema=1, source="git", revision=revision,
                                          root=str(repo.resolve())))


def finish_git_update(repo: Path, data_dir: str, revision: str) -> None:
    if _git(repo, "rev-parse", "HEAD", capture=True) != revision \
            or _git(repo, "status", "--porcelain", "--untracked-files=no", capture=True):
        raise RuntimeError("updated Git checkout is not clean at the exact target")
    write_installation_metadata(repo, data_dir, "git", revision)
    _installation_metadata(repo, data_dir).with_name("installation-update-pending.json").unlink(missing_ok=True)

def installation_update_sources(repo: Path, data_dir: str) -> list[str]:
    try:
        mode = classify_installation(repo, data_dir)
    except (RuntimeError, OSError):
        return []
    # A metadata-less overlay is indistinguishable from user edits. Only the
    # node-local exact-target Git verification may recover an ambiguous tree.
    return [] if mode == "mixed" else [mode]


def validate_lifecycle(repo: Path, config: WorkerConfig) -> str:
    repo = _installation_path(repo, "installation root")
    expected = _installation_path(os.environ.get("H3WORKER_LIFECYCLE_ROOT", str(repo)), "lifecycle root")
    if expected != repo:
        raise RuntimeError("lifecycle root does not match Worker runtime root")
    config_path = _installation_path(os.environ.get("H3WORKER_CONFIG", str(repo / "client/worker.json")), "Worker config")
    if config_path.name == "worker.json" and config_path.parent.name == "client" \
            and config_path.parent != repo / "client":
        raise RuntimeError("Worker config belongs to another installation root")
    return classify_installation(repo, config.data_dir)

def _verify_package_overlay(repo: Path, revision: str) -> tuple[set[str], list[str]]:
    """Prove every overlay change is exactly reconcilable without modifying it."""
    repo = _installation_path(repo, "installation root")
    configs = _program_config_paths(repo)
    target = _git_z(
        repo, "ls-tree", "-r", "--name-only", "-z", revision, "--",
        "client", "shared", "requirements.txt",
    )
    if _git(repo, "diff", "--cached", "--name-only", capture=True):
        raise RuntimeError("worker checkout has staged changes; refusing automatic update")
    tracked = _git_z(repo, "diff", "--name-only", "-z", "HEAD", "--")
    untracked = _git_z(
        repo, "ls-files", "--others", "--exclude-standard", "-z", "--",
        "client", "shared", "requirements.txt",
    )
    for relative in sorted(tracked):
        if relative != "requirements.txt" \
                and not relative.startswith(("client/", "shared/")):
            raise RuntimeError(
                f"tracked change is not package-owned: {relative}"
            )
        path = repo / relative
        if relative not in target:
            if path.exists() or path.is_symlink():
                raise RuntimeError(
                    f"tracked package path differs from target: {relative}"
                )
            continue
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(
                f"tracked package path is not a regular file: {relative}"
            )
        local_blob = _git(repo, "hash-object", "--", relative, capture=True)
        target_blob = _git(
            repo, "rev-parse", "--verify", f"{revision}:{relative}",
            capture=True,
        )
        if local_blob != target_blob:
            raise RuntimeError(
                f"tracked package path differs from target: {relative}"
            )
    extras = {p for p in untracked - target
              if p not in configs and "__pycache__" not in Path(p).parts
              and not p.endswith(".pyc")}
    if extras:
        raise RuntimeError("conflicting untracked package paths: " + ", ".join(sorted(extras)))
    collisions = sorted(untracked & target)
    for relative in collisions:
        if relative != "requirements.txt" \
                and not relative.startswith(("client/", "shared/")):
            raise RuntimeError(
                f"untracked target path is not package-owned: {relative}"
            )
        path = repo / relative
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(
                f"untracked package path is not a regular file: {relative}"
            )
        local_blob = _git(repo, "hash-object", "--", relative, capture=True)
        target_blob = _git(
            repo, "rev-parse", "--verify", f"{revision}:{relative}",
            capture=True,
        )
        if local_blob != target_blob:
            raise RuntimeError(
                f"untracked package path differs from target: {relative}"
            )
    return tracked, collisions


def _reconcile_verified_package_overlay(repo: Path, revision: str,
                                        data_dir: str | None = None) -> None:
    """Re-verify at the gated apply boundary before restoring exact overlays."""
    repo = _installation_path(repo, "installation root")
    tracked, collisions = _verify_package_overlay(repo, revision)
    # Merge follows even for a clean checkout. Intent precedes the first
    # reconciliation/merge mutation, never read-only preparation or draining.
    if data_dir is not None:
        mark_git_update_pending(repo, data_dir, revision)
    for relative in sorted(tracked):
        _git(repo, "checkout-index", "-f", "--", relative)
    for relative in collisions:
        (repo / relative).unlink()


def _prepare_update(repo: Path, revision: str, data_dir: str | None = None) -> None:
    """Fetch and verify target/overlay read-only; never publish pending or edit code."""
    repo = _installation_path(repo, "installation root")
    if not _REVISION.fullmatch(revision):
        raise ValueError("update revision must be a 40-character lowercase SHA")
    _git(repo, "fetch", "--quiet", "origin", "main")
    resolved = _git(repo, "rev-parse", "--verify", f"{revision}^{{commit}}", capture=True)
    if resolved != revision:
        raise RuntimeError("update revision did not resolve exactly")
    remote_tip = _git(repo, "rev-parse", "--verify", "origin/main", capture=True)
    subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor",
         revision, remote_tip], check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor",
         "HEAD", revision], check=True,
    )
    _verify_package_overlay(repo, revision)


def _apply_update(repo: Path, revision: str, data_dir: str | None = None) -> None:
    """Advance the checkout, install locked dependencies and run smoke tests."""
    repo = _installation_path(repo, "installation root")
    _reconcile_verified_package_overlay(repo, revision, data_dir)
    _git(repo, "merge", "--ff-only", revision)
    python = repo / ".venv" / "bin" / "python"
    if not python.is_file():
        raise RuntimeError(f"worker virtualenv missing: {python}")
    subprocess.run(
        [str(python), "-m", "pip", "install", "--disable-pip-version-check",
         "-r", str(repo / "requirements.txt")], check=True,
    )
    tts_requirements = repo / "client" / "requirements-tts.txt"
    tts_python = repo / "var" / "tts-venv" / "bin" / "python"
    if tts_requirements.is_file() and tts_python.is_file():
        subprocess.run(
            [str(tts_python), "-m", "pip", "install", "--disable-pip-version-check",
             "-r", str(tts_requirements)], check=True,
        )
    qwen_requirements = repo / "client" / "qwen3-requirements.txt"
    qwen_python = repo / "var" / "tts-qwen3-venv" / "bin" / "python"
    if qwen_requirements.is_file() and qwen_python.is_file():
        subprocess.run(
            [str(qwen_python), "-m", "pip", "install", "--disable-pip-version-check",
             "-r", str(qwen_requirements)], check=True,
        )
    for test in (
        "config_test.py", "runner_test.py", "tts_test.py", "enable_qwen3_test.py",
    ):
        subprocess.run([str(python), str(repo / "tests" / test)], check=True)


def _download_package(url: str, destination: Path, expected_sha256: str,
                      headers: dict[str, str] | None = None, *,
                      timeout: float = 60.0, max_retries: int = 4) -> None:
    """Download and verify a package, resuming transiently interrupted reads.

    ``urllib``'s timeout is an inactivity timeout for each socket operation,
    not an end-to-end deadline.  A proxy can therefore deliver response
    headers and then stall.  Keep the verified partial file and resume with a
    byte range instead of treating one connection as the only update attempt.
    """
    if not _SHA256.fullmatch(expected_sha256):
        raise ValueError("package_sha256 must be 64 lowercase hex characters")
    if timeout <= 0:
        raise ValueError("package download timeout must be positive")
    if max_retries < 0:
        raise ValueError("package download max_retries must not be negative")

    destination.unlink(missing_ok=True)
    opener = build_opener(_PackageRedirectHandler)
    validator: str | None = None
    last_error: Exception | None = None

    def downloaded_matches() -> bool:
        if not destination.is_file():
            return False
        with destination.open("rb") as downloaded:
            return hashlib.file_digest(downloaded, "sha256").hexdigest() \
                == expected_sha256

    for attempt in range(max_retries + 1):
        offset = destination.stat().st_size if destination.exists() else 0
        request_headers = dict(headers or {})
        request_headers.setdefault("Accept", "application/gzip")
        request_headers.setdefault("Accept-Encoding", "identity")
        if offset:
            request_headers["Range"] = f"bytes={offset}-"
            if validator:
                request_headers["If-Range"] = validator
        request = Request(url, headers=request_headers)
        try:
            with opener.open(request, timeout=timeout) as response:
                status = getattr(response, "status", response.getcode())
                append = offset > 0 and status == 206
                if offset and not append and status != 200:
                    raise RuntimeError(
                        f"update package resume returned HTTP {status}"
                    )
                if append:
                    value = response.headers.get("Content-Range", "")
                    match = _CONTENT_RANGE.fullmatch(value)
                    if match is None or int(match.group(1)) != offset:
                        raise RuntimeError(
                            "update package resume returned an invalid Content-Range"
                        )
                    total = int(match.group(3))
                    end = int(match.group(2))
                    if end < offset or end >= total:
                        raise RuntimeError(
                            "update package resume returned an invalid Content-Range"
                        )
                    if total > _MAX_PACKAGE_BYTES:
                        raise RuntimeError("update package exceeds 128 MiB")
                else:
                    # A server may ignore Range or an If-Range validator after
                    # the object changes.  Restart rather than append a 200.
                    offset = 0
                    total = None
                current_validator = response.headers.get("ETag")
                if validator and append and current_validator \
                        and current_validator != validator:
                    raise RuntimeError("update package ETag changed during resume")
                validator = current_validator or validator
                declared_value = response.headers.get("Content-Length")
                declared = int(declared_value) if declared_value else None
                if append and declared is not None \
                        and declared != end - offset + 1:
                    raise RuntimeError(
                        "update package resume length disagrees with Content-Range"
                    )
                if declared is not None and offset + declared > _MAX_PACKAGE_BYTES:
                    raise RuntimeError("update package exceeds 128 MiB")

                received = 0
                with destination.open("ab" if append else "wb") as output:
                    while True:
                        # Do not ask HTTPResponse for more than the upstream
                        # ASGI chunk.  For a small response read(1 MiB) waits
                        # for EOF and an intermediary stall can discard an
                        # already received 64 KiB prefix in IncompleteRead.
                        chunk = response.read(64 * 1024)
                        if not chunk:
                            break
                        received += len(chunk)
                        if offset + received > _MAX_PACKAGE_BYTES:
                            raise RuntimeError("update package exceeds 128 MiB")
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
                if declared is not None and received != declared:
                    raise http.client.IncompleteRead(b"", declared - received)
                if append and total is not None \
                        and destination.stat().st_size != total:
                    raise http.client.IncompleteRead(
                        b"", total - destination.stat().st_size,
                    )
            if downloaded_matches():
                return
            # A complete but corrupt intermediary response is safe to retry
            # only from byte zero; retaining it would make Range ineffective.
            destination.unlink(missing_ok=True)
            raise _RetryablePackageError(
                "update package SHA-256 mismatch"
            )
        except urllib.error.HTTPError as exc:
            last_error = exc
            retryable = exc.code in (408, 425, 429) or exc.code >= 500
        except (urllib.error.URLError, TimeoutError, OSError,
                http.client.HTTPException) as exc:
            last_error = exc
            retryable = True
        except _RetryablePackageError as exc:
            last_error = exc
            retryable = True
        except Exception:
            destination.unlink(missing_ok=True)
            raise
        # A timeout can happen after the final bytes arrived but before the
        # HTTP response reached EOF.  The digest is stronger evidence than
        # transport completion, so do not request an unnecessary 416 range.
        if downloaded_matches():
            return
        if not retryable or attempt >= max_retries:
            destination.unlink(missing_ok=True)
            assert last_error is not None
            raise last_error
        time.sleep(min(0.25 * (2 ** attempt), 2.0))

    raise AssertionError("unreachable")


def _extract_package(archive: Path, destination: Path) -> None:
    """Extract the code-only Worker bundle without trusting archive paths."""
    seen: set[Path] = set()
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        if sum(member.size for member in members if member.isfile()) \
                > _MAX_PACKAGE_BYTES:
            raise RuntimeError("expanded update package exceeds 128 MiB")
        roots = {Path(member.name).parts[0] for member in members if member.name}
        if len(roots) != 1:
            raise RuntimeError("update package must have one root directory")
        root = next(iter(roots))
        for member in members:
            parts = Path(member.name).parts
            if not parts or parts[0] != root or any(part in ("", ".", "..") for part in parts):
                raise RuntimeError("unsafe update package path")
            relative = Path(*parts[1:])
            if not relative.parts:
                continue
            if relative.parts[0] not in ("client", "shared", "requirements.txt"):
                raise RuntimeError(f"unsupported update package path: {relative}")
            if relative.parts[0] == "requirements.txt" and len(relative.parts) != 1:
                raise RuntimeError(f"unsupported update package path: {relative}")
            if relative == Path("client/worker.json"):
                raise RuntimeError("update package must not contain worker.json")
            if relative in seen or member.issym() or member.islnk() \
                    or not (member.isdir() or member.isfile()):
                raise RuntimeError(f"unsafe update package member: {relative}")
            seen.add(relative)
            target = destination / relative
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = bundle.extractfile(member)
            if source is None:
                raise RuntimeError(f"cannot read update package member: {relative}")
            with source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(0o755 if member.mode & 0o111 else 0o644)
    for required in (
        "client/start.sh", "client/h3worker/worker.py",
        "client/h3worker/upgrade.py", "shared/h3proto.py",
    ):
        if not (destination / required).is_file():
            raise RuntimeError(f"update package missing {required}")


def _apply_package_update(repo: Path, data_dir: str, url: str, sha256: str,
                          headers: dict[str, str] | None = None, *,
                          timeout: float = 60.0,
                          max_retries: int = 4, revision: str) -> None:
    """Verify a bundle, smoke-test it, then replace only Worker code."""
    if not _REVISION.fullmatch(revision):
        raise ValueError("package revision must be an exact SHA")
    temp_parent = Path(data_dir) / "tmp"
    temp_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="worker-update-", dir=temp_parent) as name:
        temp = Path(name)
        archive = temp / "update.tar.gz"
        staged = temp / "staged"
        backup = temp / "backup"
        staged.mkdir()
        backup.mkdir()
        _download_package(
            url, archive, sha256, headers,
            timeout=timeout, max_retries=max_retries,
        )
        _extract_package(archive, staged)

        subprocess.run(
            [sys.executable, "-m", "compileall", "-q",
             str(staged / "client"), str(staged / "shared")], check=True,
        )
        env = os.environ.copy()
        env["PYTHONPATH"] = str(staged / "client") + os.pathsep + str(staged)
        subprocess.run(
            [sys.executable, "-c", "import h3worker.worker, shared.h3proto"],
            cwd=staged, env=env, check=True,
        )

        client_dir = (repo / "client").resolve()
        config = Path(os.environ.get(
            "H3WORKER_CONFIG", str(client_dir / "worker.json"),
        )).resolve()
        if config.is_file() and config.is_relative_to(client_dir):
            relative_config = config.relative_to(client_dir)
            target_config = staged / "client" / relative_config
            target_config.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(config, target_config)

        replaced: list[tuple[str, bool]] = []
        try:
            for relative in ("client", "shared", "requirements.txt"):
                source = staged / relative
                if not source.exists():
                    continue
                current = repo / relative
                saved = backup / relative
                saved.parent.mkdir(parents=True, exist_ok=True)
                had_current = current.exists()
                if had_current:
                    os.replace(current, saved)
                try:
                    os.replace(source, current)
                except Exception:
                    if had_current:
                        os.replace(saved, current)
                    raise
                replaced.append((relative, had_current))
            write_installation_metadata(repo, data_dir, "package", revision)
        except Exception:
            for relative, had_current in reversed(replaced):
                current = repo / relative
                saved = backup / relative
                if current.exists():
                    if current.is_dir():
                        shutil.rmtree(current)
                    else:
                        current.unlink()
                if had_current:
                    os.replace(saved, current)
            raise


def _package_headers(config: WorkerConfig, package_url: str) -> dict[str, str]:
    headers = {"User-Agent": _USER_AGENT}
    if _origin(package_url) != _origin(config.server_url):
        return headers
    headers["Authorization"] = f"Bearer {config.worker_token}"
    if config.cf_access_client_id:
        headers["CF-Access-Client-Id"] = config.cf_access_client_id
        headers["CF-Access-Client-Secret"] = config.cf_access_client_secret
    return headers


def _active_local(journal_path: str) -> bool:
    uri = Path(journal_path).absolute().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=5) as conn:
        active = conn.execute(
            "SELECT count(*) FROM attempts WHERE confirmed_terminal = 0"
        ).fetchone()[0]
        processes = conn.execute("SELECT count(*) FROM processes").fetchone()[0]
    return active > 0 or processes > 0


def _admin_worker(client: HttpClient, worker_id: str) -> dict:
    _, body = client.request("GET", "/v1/admin/workers")
    worker = next(
        (w for w in body["workers"] if w["worker_id"] == worker_id), None
    )
    if worker is None:
        raise RuntimeError(f"worker {worker_id} missing from admin view")
    return worker


def _set_drain(client: HttpClient, worker_id: str, draining: bool) -> None:
    client.request(
        "PATCH", "/v1/admin/workers/" + quote(worker_id, safe=""),
        {"operator_draining": draining},
    )


def _retry(fn, deadline: float):
    while True:
        try:
            return fn()
        except ApiError as exc:
            if not (exc.retryable or exc.status == 0) or time.monotonic() >= deadline:
                raise
            time.sleep(min(2.0, max(0.05, deadline - time.monotonic())))


def upgrade() -> None:
    config = WorkerConfig.from_env()
    timeout = float(os.environ.get("H3WORKER_RESTART_TIMEOUT_SECONDS", "86400"))
    if timeout <= 0:
        raise ValueError("restart timeout must be positive")
    job = f"gui/{os.getuid()}/com.relife.h3worker"
    domain = f"gui/{os.getuid()}"
    plist = Path.home() / "Library/LaunchAgents/com.relife.h3worker.plist"
    revision = os.environ.get("H3WORKER_UPDATE_REVISION", "").strip()
    operation = "update" if revision else "restart"
    repo = _repo_root()
    mode = validate_lifecycle(repo, config)
    if revision:
        if mode == "package" and not (repo / ".git").exists():
            raise RuntimeError("package installation: use the Server package update API")
        _prepare_update(repo, revision, config.data_dir)
    subprocess.run(["launchctl", "print", job], check=True,
                   stdout=subprocess.DEVNULL)
    if not plist.is_file():
        raise RuntimeError(f"launch agent plist missing: {plist}")
    client = HttpClient(
        config.server_url, config.worker_token,
        config.http_connect_timeout_seconds, config.http_timeout_seconds,
        cf_access_client_id=config.cf_access_client_id,
        cf_access_client_secret=config.cf_access_client_secret,
        server_connect_ip=config.server_connect_ip,
    )
    worker = _admin_worker(client, config.worker_id)
    capacity = worker["capacity"]
    if not worker["enabled"] or not isinstance(capacity, int) or capacity < 1:
        raise RuntimeError("worker must be enabled with positive capacity")
    if not os.path.isfile(config.journal_path):
        raise RuntimeError("worker journal missing; cannot prove idle")

    deadline = time.monotonic() + timeout
    _retry(lambda: _set_drain(client, config.worker_id, True), deadline)
    print(f"[{operation}] draining {config.worker_id}; waiting for active attempt",
          flush=True)
    while True:
        worker = _retry(lambda: _admin_worker(client, config.worker_id), deadline)
        if worker["capacity"] != capacity or not worker.get("operator_draining"):
            raise RuntimeError("worker drain was changed by another operator")
        if not worker["active_attempts"] and not _active_local(config.journal_path):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("active attempt did not finish; worker remains drained")
        time.sleep(min(2.0, max(0.05, deadline - time.monotonic())))

    # Keep positive capacity and operator drain through restart and verification.
    # Process recovery preserves unresolved engine identity; code mutation must
    # still prove the ComfyUI runtime safe before stopping or changing anything.
    if revision:
        from .comfy_runner import maintenance_preflight
        maintenance_preflight(config, config.journal_path)
    subprocess.run(["launchctl", "bootout", job], check=True)
    if revision:
        print(f"[update] applying verified revision {revision}", flush=True)
        _apply_update(repo, revision, config.data_dir)
        finish_git_update(repo, config.data_dir, revision)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist)], check=True)
    subprocess.run(["launchctl", "print", job], check=True,
                   stdout=subprocess.DEVNULL)
    action = f"updated to {revision} and restarted" if revision else "restarted"
    print(f"[{operation}] {action} {job} after the active attempt finished; operator drain remains held", flush=True)


def main() -> None:
    try:
        if sys.argv[1:] == ["--check-installation"]:
            validate_lifecycle(_repo_root(), WorkerConfig.from_env())
        else:
            upgrade()
    except Exception as exc:
        operation = "update" if os.environ.get("H3WORKER_UPDATE_REVISION", "").strip() else "restart"
        if sys.argv[1:] == ["--check-installation"]:
            operation = "installation"
        raise SystemExit(f"[{operation}] stopped safely: {exc}") from exc


if __name__ == "__main__":
    main()
