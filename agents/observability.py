import contextlib
import json
import time
import uuid
from typing import Any


def new_run_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _safe_json(value: Any, max_chars: int = 8000) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value[:max_chars]
    return value


@contextlib.contextmanager
def trace_context(
    name: str,
    *,
    run_id: str | None = None,
    user_id: str | int | None = None,
    metadata: dict[str, Any] | None = None,
    input: Any | None = None,
):
    """Tiny local trace hook used by the demo without adding another service."""
    started = time.time()
    print(f"[trace] start {name} run_id={run_id} user_id={user_id}")
    try:
        yield None
    except Exception as exc:
        print(f"[trace] error {name}: {exc}")
        raise
    finally:
        elapsed_ms = round((time.time() - started) * 1000, 2)
        print(f"[trace] end {name} elapsed_ms={elapsed_ms}")


def set_trace_output(output: Any) -> None:
    return None


def trace_span(
    name: str,
    *,
    input: Any | None = None,
    output: Any | None = None,
    metadata: dict[str, Any] | None = None,
    start_time: float | None = None,
):
    elapsed_ms = round((time.time() - (start_time or time.time())) * 1000, 2)
    print(
        f"[span] {name} elapsed_ms={elapsed_ms} "
        f"metadata={_safe_json(metadata or {})}"
    )
