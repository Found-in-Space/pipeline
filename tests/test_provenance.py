from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import foundinspace.pipeline.gaia.cli as gaia_cli_module
import foundinspace.pipeline.provenance as provenance_module
from foundinspace.pipeline.gaia.cli import cli as gaia_cli
from foundinspace.pipeline.project import (
    load_project,
    render_run_project_template,
)
from foundinspace.pipeline.provenance import (
    ProvenanceError,
    fingerprint_path,
    record_command,
)


def _controlled_project(tmp_path: Path):
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        """
format_version = 1

[run]
id = "run-1"
provenance_dir = "provenance"

[gaia]
input_dir = "inputs"
input_mode = "directory"
output_dir = "outputs"
""".lstrip(),
        encoding="utf-8",
    )
    return load_project(project_path)


def test_fingerprint_path_hashes_files_and_directories(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "a.txt").write_text("alpha\n", encoding="utf-8")
    (tree / "b.txt").write_text("beta\n", encoding="utf-8")

    file_identity = fingerprint_path(tree / "a.txt")
    tree_identity = fingerprint_path(tree)

    assert file_identity["kind"] == "file"
    assert len(file_identity["sha256"]) == 64
    assert tree_identity["kind"] == "directory"
    assert tree_identity["file_count"] == 2
    assert len(tree_identity["tree_sha256"]) == 64


def test_record_command_writes_success_and_failure_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _controlled_project(tmp_path)
    source = tmp_path / "source.txt"
    source.write_text("source\n", encoding="utf-8")
    output = tmp_path / "output.txt"
    monkeypatch.setattr(
        "foundinspace.pipeline.provenance._pipeline_code_identity",
        lambda: {"package_version": "test", "git_sha": "abc", "git_dirty": False},
    )

    with record_command(
        project,
        command="test success",
        parameters={"force": False},
        inputs={"source": source},
        outputs={"output": output},
    ):
        output.write_text("output\n", encoding="utf-8")

    with (
        pytest.raises(RuntimeError, match="broken"),
        record_command(project, command="test failure"),
    ):
        raise RuntimeError("broken")

    events = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(project.run.provenance_dir.glob("*.json"))
    ]
    assert {event["status"] for event in events} == {"failed", "succeeded"}
    success = next(event for event in events if event["status"] == "succeeded")
    failure = next(event for event in events if event["status"] == "failed")
    assert success["run_id"] == "run-1"
    assert success["project"]["sha256"]
    assert success["inputs"]["source"]["sha256"]
    assert success["outputs"]["output"]["sha256"]
    assert failure["error"] == {"type": "RuntimeError", "message": "broken"}


def test_record_command_snapshots_start_identity_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _controlled_project(tmp_path)
    source = tmp_path / "source.txt"
    source.write_text("before\n", encoding="utf-8")
    output = tmp_path / "output.txt"
    original_project_sha = hashlib.sha256(project.project_path.read_bytes()).hexdigest()
    parameters = {"selection": {"limit": 1}}
    code_calls = 0

    def code_identity() -> dict[str, object]:
        nonlocal code_calls
        code_calls += 1
        return {
            "package_version": "start",
            "git_sha": "start-sha",
            "git_dirty": False,
        }

    monkeypatch.setattr(provenance_module, "_pipeline_code_identity", code_identity)

    with record_command(
        project,
        command="test snapshots",
        parameters=parameters,
        inputs={"source": source},
        outputs={"output": output},
    ):
        assert code_calls == 1
        source.write_text("after\n", encoding="utf-8")
        output.write_text("output\n", encoding="utf-8")
        parameters["selection"]["limit"] = 2
        project.project_path.write_text("changed during command\n", encoding="utf-8")
        assert project.run._raw is not None
        assert project.gaia._raw is not None
        project.run._raw["id"] = "changed-run"
        project.run._raw["provenance_dir"] = "changed-provenance"
        project.gaia._raw["output_dir"] = "changed-output"

    event_path = next((tmp_path / "run-1/provenance").glob("*.json"))
    event = json.loads(event_path.read_text(encoding="utf-8"))
    assert code_calls == 1
    assert event["run_id"] == "run-1"
    assert event["project"]["sha256"] == original_project_sha
    assert event["project"]["resolved_paths"]["gaia.output_dir"].endswith("/outputs")
    assert event["code"]["git_sha"] == "start-sha"
    assert event["parameters"] == {"selection": {"limit": 1}}
    assert (
        event["inputs"]["source"]["sha256"] == hashlib.sha256(b"before\n").hexdigest()
    )
    assert (
        event["outputs"]["output"]["sha256"] == hashlib.sha256(b"output\n").hexdigest()
    )


def test_input_fingerprint_failure_prevents_execution_and_records_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _controlled_project(tmp_path)
    source = tmp_path / "source.txt"
    source.write_text("source\n", encoding="utf-8")
    original_fingerprint = provenance_module.fingerprint_path
    executed = False

    def fail_source(path: Path) -> dict[str, object]:
        if Path(path) == source:
            raise PermissionError("input unreadable")
        return original_fingerprint(path)

    monkeypatch.setattr(provenance_module, "fingerprint_path", fail_source)

    with (
        pytest.raises(ProvenanceError, match="controlled-run inputs"),
        record_command(
            project,
            command="test input failure",
            inputs={"source": source},
        ),
    ):
        executed = True

    assert not executed
    event_path = next(project.run.provenance_dir.glob("*.json"))
    event = json.loads(event_path.read_text(encoding="utf-8"))
    assert event["status"] == "failed"
    assert event["error"]["type"] == "ProvenanceError"
    assert "PermissionError" in event["inputs"]["source"]["error"]


def test_output_fingerprint_failure_marks_event_failed_and_returns_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _controlled_project(tmp_path)
    output = tmp_path / "output.txt"
    original_fingerprint = provenance_module.fingerprint_path

    def fail_output(path: Path) -> dict[str, object]:
        if Path(path) == output:
            raise PermissionError("output unreadable")
        return original_fingerprint(path)

    monkeypatch.setattr(provenance_module, "fingerprint_path", fail_output)

    with (
        pytest.raises(ProvenanceError, match="controlled-run outputs"),
        record_command(
            project,
            command="test output failure",
            outputs={"output": output},
        ),
    ):
        output.write_text("output\n", encoding="utf-8")

    event_path = next(project.run.provenance_dir.glob("*.json"))
    event = json.loads(event_path.read_text(encoding="utf-8"))
    assert event["status"] == "failed"
    assert event["error"]["type"] == "ProvenanceError"
    assert "PermissionError" in event["outputs"]["output"]["error"]


def test_finalization_failure_does_not_replace_command_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _controlled_project(tmp_path)

    def fail_write(*args, **kwargs):
        raise RuntimeError("provenance unavailable")

    monkeypatch.setattr(provenance_module, "_write_event", fail_write)

    with (
        pytest.raises(RuntimeError, match="primary failure") as raised,
        record_command(project, command="test primary failure"),
    ):
        raise RuntimeError("primary failure")

    assert any(
        "provenance finalization also failed" in note.lower()
        for note in getattr(raised.value, "__notes__", ())
    )


def test_download_state_gaia_build_hashes_state_and_votable_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = tmp_path / "state-build"
    run_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        render_run_project_template(run_id=run_dir.name, profile="small"),
        encoding="utf-8",
    )
    project = load_project(project_path)
    project.run.provenance_dir.mkdir()
    project.gaia.input_dir.mkdir(parents=True)
    votable = project.gaia.input_dir / "batch.vot"
    votable.write_text("votable\n", encoding="utf-8")
    project.gaia_download.state_db.parent.mkdir(parents=True, exist_ok=True)
    project.gaia_download.state_db.write_text("state\n", encoding="utf-8")

    monkeypatch.setattr(
        gaia_cli_module,
        "gaia_download_input_files_for_build",
        lambda project: (votable,),
    )

    def fake_build(input_path: Path, output_path: Path, **kwargs) -> None:
        output_path.write_text("parquet\n", encoding="utf-8")

    monkeypatch.setattr(gaia_cli_module, "main", fake_build)

    result = CliRunner().invoke(
        gaia_cli,
        ["build", "--project", str(project_path)],
    )

    assert result.exit_code == 0, result.output
    event_path = next(project.run.provenance_dir.glob("*.json"))
    event = json.loads(event_path.read_text(encoding="utf-8"))
    assert (
        event["inputs"]["download_state"]["sha256"]
        == hashlib.sha256(b"state\n").hexdigest()
    )
    assert event["inputs"]["gaia_votables"]["tree_sha256"]
