"""Immutable per-command provenance for controlled pipeline runs."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tempfile
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from foundinspace.pipeline.project import PipelineProject

PROVENANCE_SCHEMA_VERSION = 1
_DIST_NAME = "found-in-space-pipeline"


class ProvenanceError(RuntimeError):
    """Raised when controlled-run provenance cannot be captured completely."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for block in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fingerprint_path(path: Path) -> dict[str, Any]:
    """Return a content identity without copying the referenced artifact."""
    path = Path(path).expanduser()
    identity: dict[str, Any] = {"path": str(path.absolute())}
    try:
        path_stat = path.stat()
    except FileNotFoundError:
        identity["exists"] = False
        identity["kind"] = "missing"
        return identity
    identity["exists"] = True
    if stat.S_ISREG(path_stat.st_mode):
        identity.update(
            {
                "kind": "file",
                "size_bytes": path_stat.st_size,
                "sha256": _sha256_file(path),
            }
        )
        return identity
    if not stat.S_ISDIR(path_stat.st_mode):
        identity["kind"] = "other"
        return identity

    digest = hashlib.sha256()
    file_count = 0
    total_bytes = 0
    children: list[Path] = []

    def raise_walk_error(exc: OSError) -> None:
        raise exc

    for directory, directory_names, file_names in os.walk(
        path,
        onerror=raise_walk_error,
    ):
        directory_names.sort()
        children.extend(Path(directory) / name for name in sorted(file_names))

    for child in children:
        relative = child.relative_to(path).as_posix()
        size = child.stat().st_size
        checksum = _sha256_file(child)
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\0")
        digest.update(checksum.encode("ascii"))
        digest.update(b"\n")
        file_count += 1
        total_bytes += size
    identity.update(
        {
            "kind": "directory",
            "file_count": file_count,
            "size_bytes": total_bytes,
            "tree_sha256": digest.hexdigest(),
        }
    )
    return identity


def _json_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value.absolute())
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def _artifact_identities(
    artifacts: Mapping[str, Path | Sequence[Path]] | None,
) -> tuple[dict[str, Any], list[str]]:
    if not artifacts:
        return {}, []
    identities: dict[str, Any] = {}
    errors: list[str] = []
    for role, value in artifacts.items():
        values = (
            value
            if isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray, Path))
            else None
        )
        if values is None:
            identities[role] = _fingerprint_artifact(
                role=role,
                value=Path(value),
                errors=errors,
            )
        else:
            identities[role] = [
                _fingerprint_artifact(
                    role=f"{role}[{index}]",
                    value=Path(path),
                    errors=errors,
                )
                for index, path in enumerate(values)
            ]
    return identities, errors


def _fingerprint_artifact(
    *,
    role: str,
    value: Path,
    errors: list[str],
) -> dict[str, Any]:
    try:
        return fingerprint_path(value)
    except OSError as exc:
        detail = f"{type(exc).__name__}: {exc}"
        errors.append(f"{role} ({value.absolute()}): {detail}")
        return {
            "error": detail,
            "path": str(value.absolute()),
        }


def _pipeline_code_identity() -> dict[str, Any]:
    try:
        version = metadata.version(_DIST_NAME)
    except metadata.PackageNotFoundError:
        version = None

    repo_root = Path(__file__).resolve().parents[3]
    identity: dict[str, Any] = {"package_version": version}
    try:
        sha = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        identity.update({"git_sha": None, "git_dirty": None})
    else:
        identity.update({"git_sha": sha, "git_dirty": bool(status.strip())})
    return identity


def _error_details(exc: BaseException) -> dict[str, str]:
    return {"type": type(exc).__name__, "message": str(exc)}


def _provenance_error(stage: str, errors: Sequence[str]) -> ProvenanceError:
    joined = "; ".join(errors)
    return ProvenanceError(f"Could not fingerprint controlled-run {stage}: {joined}")


def _add_finalization_note(primary: BaseException, secondary: BaseException) -> None:
    add_note = getattr(primary, "add_note", None)
    if add_note is not None:
        add_note(
            "Controlled-run provenance finalization also failed: "
            f"{type(secondary).__name__}: {secondary}"
        )


def _write_event(provenance_dir: Path, event: dict[str, Any]) -> Path:
    provenance_dir.mkdir(parents=True, exist_ok=True)
    timestamp = str(event["started_at"]).replace(":", "").replace("-", "")
    command_slug = str(event["command"]).replace(" ", "-").replace("/", "-")
    filename = f"{timestamp}-{command_slug}-{uuid.uuid4().hex[:8]}.json"
    target = provenance_dir / filename
    payload = json.dumps(event, indent=2, sort_keys=True) + "\n"
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=provenance_dir,
            prefix=f".{filename}.",
            suffix=".tmp",
            delete=False,
        ) as fp:
            temporary = Path(fp.name)
            fp.write(payload)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temporary, target)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return target


def _event(
    *,
    run_id: str,
    command: str,
    status: str,
    started: datetime,
    finished: datetime,
    project_path: Path,
    project_sha256: str,
    resolved_paths: Mapping[str, Any],
    code: Mapping[str, Any],
    parameters: Any,
    inputs: Mapping[str, Any],
    outputs: Mapping[str, Any],
    error: BaseException | None,
    provenance_errors: Sequence[str] = (),
) -> dict[str, Any]:
    event = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "run_id": run_id,
        "command": command,
        "status": status,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_seconds": (finished - started).total_seconds(),
        "project": {
            "path": str(project_path),
            "sha256": project_sha256,
            "resolved_paths": resolved_paths,
        },
        "code": code,
        "parameters": parameters,
        "inputs": inputs,
        "outputs": outputs,
    }
    if error is not None:
        event["error"] = _error_details(error)
    if provenance_errors:
        event["provenance_errors"] = list(provenance_errors)
    return event


@contextmanager
def record_command(
    project: PipelineProject,
    *,
    command: str,
    parameters: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Path | Sequence[Path]] | None = None,
    outputs: Mapping[str, Path | Sequence[Path]] | None = None,
) -> Iterator[None]:
    """Record one controlled-run command, including handled failures."""
    if not project.run.is_configured:
        yield
        return

    try:
        run_id = project.run.id
        provenance_dir = project.run.provenance_dir
        try:
            provenance_dir_stat = provenance_dir.stat()
        except FileNotFoundError:
            provenance_dir_stat = None
    except (OSError, ValueError) as exc:
        raise ProvenanceError(str(exc)) from exc
    if provenance_dir_stat is not None and not stat.S_ISDIR(
        provenance_dir_stat.st_mode
    ):
        raise ProvenanceError(
            f"run.provenance_dir must be a directory: {provenance_dir}"
        )

    started = _utc_now()
    project_path = project.project_path
    parameters_snapshot = _json_value(parameters or {})

    try:
        project_sha256 = _sha256_file(project_path)
        resolved_paths = project.resolved_paths()
        code = _pipeline_code_identity()
    except (OSError, ValueError) as exc:
        finished = _utc_now()
        pre_execution_error = ProvenanceError(str(exc))
        event = _event(
            run_id=run_id,
            command=command,
            status="failed",
            started=started,
            finished=finished,
            project_path=project_path,
            project_sha256="",
            resolved_paths={},
            code={},
            parameters=parameters_snapshot,
            inputs={},
            outputs={},
            error=pre_execution_error,
        )
        try:
            _write_event(provenance_dir, event)
        except Exception as finalization_error:
            raise ProvenanceError(
                "Could not record controlled-run provenance: "
                f"{type(finalization_error).__name__}: {finalization_error}"
            ) from finalization_error
        raise pre_execution_error from exc

    input_identities, input_errors = _artifact_identities(inputs)
    if input_errors:
        finished = _utc_now()
        pre_execution_error = _provenance_error("inputs", input_errors)
        event = _event(
            run_id=run_id,
            command=command,
            status="failed",
            started=started,
            finished=finished,
            project_path=project_path,
            project_sha256=project_sha256,
            resolved_paths=resolved_paths,
            code=code,
            parameters=parameters_snapshot,
            inputs=input_identities,
            outputs={},
            error=pre_execution_error,
            provenance_errors=input_errors,
        )
        try:
            _write_event(provenance_dir, event)
        except Exception as finalization_error:
            raise ProvenanceError(
                "Could not record controlled-run provenance: "
                f"{type(finalization_error).__name__}: {finalization_error}"
            ) from finalization_error
        raise pre_execution_error

    primary_error: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        primary_error = exc

    finished = _utc_now()
    output_identities, output_errors = _artifact_identities(outputs)
    output_error = (
        _provenance_error("outputs", output_errors) if output_errors else None
    )
    event_error = primary_error or output_error
    event = _event(
        run_id=run_id,
        command=command,
        status="failed" if event_error is not None else "succeeded",
        started=started,
        finished=finished,
        project_path=project_path,
        project_sha256=project_sha256,
        resolved_paths=resolved_paths,
        code=code,
        parameters=parameters_snapshot,
        inputs=input_identities,
        outputs=output_identities,
        error=event_error,
        provenance_errors=output_errors,
    )
    try:
        _write_event(provenance_dir, event)
    except Exception as exc:
        finalization_error = ProvenanceError(
            f"Could not record controlled-run provenance: {type(exc).__name__}: {exc}"
        )
        if primary_error is not None:
            _add_finalization_note(primary_error, finalization_error)
        else:
            raise finalization_error from exc

    if primary_error is not None:
        if output_error is not None:
            _add_finalization_note(primary_error, output_error)
        raise primary_error
    if output_error is not None:
        raise output_error
