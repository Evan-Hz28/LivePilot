from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind

from app.config import settings
from app.logging import configure_structured_logging

_initialized = False


def _otlp_traces_endpoint(endpoint: str) -> str:
    normalized = endpoint.rstrip("/")
    if normalized.endswith("/v1/traces"):
        return normalized
    return f"{normalized}/v1/traces"


def bootstrap_observability(*, enable_exporter: bool = True) -> None:
    """Install a best-effort tracer and redacted structured logging once per process."""
    global _initialized
    configure_structured_logging()
    if _initialized:
        return
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": settings.otel_service_name,
                "deployment.environment": settings.app_env,
            }
        )
    )
    if enable_exporter and settings.otel_exporter_otlp_endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=_otlp_traces_endpoint(
                        settings.otel_exporter_otlp_endpoint
                    )
                )
            )
        )
    trace.set_tracer_provider(provider)
    _initialized = True


def current_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def inject_trace_context(fields: dict[str, str]) -> dict[str, str]:
    result = dict(fields)
    propagate.inject(result)
    return result


def extract_trace_context(fields: Mapping[str, str]):
    return propagate.extract(dict(fields))


@contextmanager
def trace_scope(
    name: str,
    attributes: Mapping[str, str | int | bool] | None = None,
    *,
    parent=None,
) -> Iterator[trace.Span]:
    tracer = trace.get_tracer("livepilot")
    with tracer.start_as_current_span(
        name,
        context=parent,
        kind=SpanKind.INTERNAL,
        attributes=dict(attributes or {}),
    ) as span:
        yield span
