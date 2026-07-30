# Agent Instructions

## Python Tooling: Use `uv`

- Use `uv` for all Python dependency and environment operations in this repository.
- Do not use `pip`, `poetry`, or `conda` commands directly.

### Cache placement

- Set `UV_CACHE_DIR` to a repository-local directory, such as `.cache/uv`, for
  every `uv` invocation.
- Do not use the default user cache or a system temporary directory: either may
  be on a different filesystem and force expensive cross-filesystem copies.
- If the repository path is a symlink, keeping the cache inside the worktree
  ensures it follows the filesystem containing the repository data.

### Standard commands

- Sync/install dependencies: `uv sync`
- Run Python entrypoints/tools: `uv run <command>`
- Run tests: `uv run pytest`
- CI: on push/PR to `main`, GitHub Actions runs `uv run pytest` with coverage (see `.github/workflows/ci.yml`).
- Add a dependency: `uv add <package>`
- Add a dev dependency: `uv add --dev <package>`

### Pre-commit (Ruff)

- Install git hooks once: `uv run pre-commit install`
- Run on all files: `uv run pre-commit run --all-files`

Hooks: `ruff-check` (lint + fix) and `ruff-format`, scoped to `src/`, `tests/`,
and `tools/`.

### Examples

- `uv run fis-pipeline --help`
- `uv run pytest tests/merge/test_pipeline.py`
