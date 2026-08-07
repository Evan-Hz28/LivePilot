from __future__ import annotations

import json
import logging
import re
import hashlib
from datetime import datetime, timezone
from typing import Any

from opentelemetry import trace

SENSITIVE_FIELD_NAMES = {
    "api_key",
    "authorization",
    "cookie",
    "input",
    "output",
    "password",
    "payload",
    "prompt",
    "secret",
    "sec-websocket-protocol",
    "text",
    "token",
}
BEARER_TOKEN = re.compile(r"(?i)bearer\s+[^\s,]+")
JWT_TOKEN = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")


def redact_value(value: Any, *, field_name: str | None = None) -> Any:
    if field_name is not None:
        normalized_name = field_name.lower().replace("-", "_")
        field_parts = set(filter(None, re.split(r"[^a-z0-9]+", normalized_name)))
        if normalized_name in SENSITIVE_FIELD_NAMES or field_parts & SENSITIVE_FIELD_NAMES:
            return "[REDACTED]"
    if isinstance(value, dict):
        return {
            str(key): redact_value(item, field_name=str(key))
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return JWT_TOKEN.sub("[REDACTED]", BEARER_TOKEN.sub("Bearer [REDACTED]", value))
    return value


class RedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_value(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(redact_value(item) for item in record.args)
        else:
            record.args = redact_value(record.args)
        return True


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        span_context = trace.get_current_span().get_span_context()
        event = record.getMessage() if record.args else record.msg
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", "livepilot"),
            "event": redact_value(event),
        }
        if span_context.is_valid:
            payload["trace_id"] = f"{span_context.trace_id:032x}"
            payload["span_id"] = f"{span_context.span_id:016x}"
        for name in ("trace_id", "span_id", "error_code", "duration_ms"):
            value = getattr(record, name, None)
            if value is not None:
                payload[name] = redact_value(value, field_name=name)
        for name in ("session_id", "task_id"):
            value = getattr(record, name, None)
            if value is not None:
                payload[name] = hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
        return json.dumps(payload, ensure_ascii=True, sort_keys=True)


def configure_structured_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        root.addHandler(logging.StreamHandler())
    for handler in root.handlers:
        if not any(isinstance(filter_, RedactionFilter) for filter_ in handler.filters):
            handler.addFilter(RedactionFilter())
        handler.setFormatter(StructuredFormatter())
