# Agent Guide for Panza

This document contains context and guidelines for AI agents working on the `panza` repository.

## Project Overview
`panza` is an experimental Python library for SQLite-based caching and concurrency limiting. It supports pluggable backends, currently implementing SQLite and S3.

## Development Environment

### Dependency Management
This project uses `uv` for dependency management.
- **Install/Sync dependencies**: `uv sync`
- **Add a dependency**: `uv add <package>` (or `uv add --dev <package>`)
- **Run commands**: `uv run <command>`

### Testing
Tests are written using `pytest` and `pytest-asyncio`.
- **Run all tests**: `uv run pytest`
- **S3 Testing**: The project uses `moto` to mock S3 interactions.
  - A session-scoped `moto_server` fixture is defined in `src/panza/conftest.py`.
  - `src/panza/test_s3_cache.py` configures fake AWS credentials to ensure no real AWS resources are touched.

## Code Structure

The source code is located in `src/panza/`:
- **`cache.py`**: Contains the main `Cache` class and the `@cache()` decorator logic. It handles serialization (hashing) of arguments, including Pydantic models.
- **`sqlite_backend.py`**: Implements the `CacheBackend` interface using `aiosqlite`.
- **`s3_backend.py`**: Implements the `CacheBackend` interface using `aioboto3` and `aiobotocore`.
- **`limit_concurrency.py`**: Utilities for limiting async concurrency.
- **`conftest.py`**: Shared pytest fixtures (specifically the `moto` server).

## Key Implementation Details
- **Hashing**: `cache.py` includes a `normalize_for_hashing` function that handles Pydantic models, sets (sorting them), and dictionaries to ensure deterministic cache keys.
- **Async**: The library is designed for asynchronous Python (`asyncio`). The `Cache` decorator works on `async def` functions.
- **Backends**: Backends must implement the `CacheBackend` abstract base class defined in `cache.py`.

## Common Tasks
- **Adding a new backend**: Implement the `CacheBackend` ABC in a new file and expose it in `__init__.py`.
- **Updating dependencies**: Edit `pyproject.toml` and run `uv sync`.
- **Publishing to PyPI**:
  1. Bump the version in `pyproject.toml`.
  2. Run `uv build` to create the distribution packages.
  3. Ensure `UV_PUBLISH_TOKEN` is set in `.env`.
  4. Run `export $(grep -v '^#' .env | xargs) && uv publish`.

