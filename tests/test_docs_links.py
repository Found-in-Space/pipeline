from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def _markdown_files() -> list[Path]:
    return [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))]


def _local_link_target(source: Path, raw_target: str) -> Path | None:
    target = raw_target.strip()
    if not target or target.startswith(("#", "http://", "https://", "mailto:")):
        return None
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    target = target.split("#", 1)[0]
    if not target:
        return None
    return (source.parent / target).resolve()


def test_repo_markdown_links_resolve() -> None:
    broken: list[str] = []
    for path in _markdown_files():
        text = path.read_text(encoding="utf-8")
        for match in LINK_RE.finditer(text):
            target = _local_link_target(path, match.group(1))
            if target is not None and not target.exists():
                broken.append(f"{path.relative_to(ROOT)} -> {match.group(1)}")

    assert not broken
