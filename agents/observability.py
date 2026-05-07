import contextlib
import contextvars
import json
import os
import time
import uuid
from typing import Any


LANGFUSE_HOST = os.getenv("LANGFUSE_HOST", "http://localhost:3001")

_langfuse = None
_langfuse_checked = False
_current_trace = contextvars.ContextVar("current_langfuse_trace", default=None)


def langfuse_enabled() -> bool:
    return bool(os.getenv("LANGFUSE_PUBLIC_KEY") and os.getenv("LANGFUSE_SECRET_KEY"))


def get_langfuse():
    """Return a Langfuse client when credentials are configured, else None."""
    global _langfuse, _langfuse_checked
    if _langfuse_checked:
        return _langfuse

    _langfuse_checked = True
    if not langfuse_enabled():
        return None

    try:
        from langfuse import Langfuse

        _langfuse = Langfuse(
            public_key=os.environ["LANGFUSE_PUBLIC_KEY"],
            secret_key=os.environ["LANGFUSE_SECRET_KEY"],
            host=LANGFUSE_HOST,
        )
    except Exception as exc:
        print(f"Langfuse disabled: {exc}")
        _langfuse = None

    return _langfuse


def flush_langfuse() -> None:
    client = get_langfuse()
    if client:
        with contextlib.suppress(Exception):
            client.flush()


def current_trace():
    return _current_trace.get()


def set_trace_output(output: Any) -> None:
    trace = current_trace()
    if trace is None:
        return

    with contextlib.suppress(Exception):
        trace.update(output=safe_json(output))


def new_run_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def safe_json(value: Any, max_chars: int = 12000) -> Any:
    """Keep trace payloads readable and bounded."""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return value[:max_chars]
        return parsed

    try:
        encoded = json.dumps(value, default=str)
    except TypeError:
        return str(value)[:max_chars]

    if len(encoded) <= max_chars:
        return value
    return encoded[:max_chars]


@contextlib.contextmanager
def trace_context(
    name: str,
    *,
    run_id: str | None = None,
    user_id: str | int | None = None,
    metadata: dict[str, Any] | None = None,
    input: Any | None = None,
):
    client = get_langfuse()
    if not client:
        yield None
        return

    trace = client.trace(
        id=run_id or new_run_id("trace"),
        name=name,
        user_id=str(user_id) if user_id is not None else None,
        metadata=metadata or {},
        input=safe_json(input),
    )
    token = _current_trace.set(trace)
    try:
        yield trace
    except Exception as exc:
        trace.update(
            level="ERROR",
            status_message=str(exc),
        )
        raise
    finally:
        _current_trace.reset(token)
        flush_langfuse()


@contextlib.contextmanager
def span_context(
    trace,
    name: str,
    *,
    input: Any | None = None,
    metadata: dict[str, Any] | None = None,
):
    if trace is None:
        yield None
        return

    started = time.time()
    span = trace.span(
        name=name,
        input=safe_json(input),
        metadata=metadata or {},
    )
    try:
        yield span
    except Exception as exc:
        span.end(
            output={"error": str(exc)},
            level="ERROR",
            status_message=str(exc),
        )
        raise
    else:
        span.end(
            output={"ok": True},
            metadata={**(metadata or {}), "latency_ms": round((time.time() - started) * 1000, 2)},
        )


def trace_span(
    name: str,
    *,
    input: Any | None = None,
    output: Any | None = None,
    metadata: dict[str, Any] | None = None,
    start_time: float | None = None,
):
    """Write a single span on the active trace if Langfuse is configured.

    Pass start_time (unix timestamp from time.time()) recorded before the work
    began so the span reflects actual tool execution time, not SDK overhead.
    """
    trace = current_trace()
    if trace is None:
        return

    from datetime import datetime, timezone
    t0 = start_time if start_time is not None else time.time()
    t1 = time.time()
    latency_ms = round((t1 - t0) * 1000, 2)

    try:
        span = trace.span(
            name=name,
            start_time=datetime.fromtimestamp(t0, tz=timezone.utc),
            input=safe_json(input),
            metadata={**(metadata or {}), "latency_ms": latency_ms},
        )
        span.end(
            end_time=datetime.fromtimestamp(t1, tz=timezone.utc),
            output=safe_json(output),
            metadata={**(metadata or {}), "latency_ms": latency_ms},
        )
    except Exception:
        pass
