from __future__ import annotations

import math
from typing import Any

from fastapi import HTTPException, status

from app.config import settings


def validate_json_value(value: Any, *, depth: int = 0) -> None:
    if depth > settings.max_json_depth:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="JSON nesting exceeds the allowed limit",
        )
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, str):
        if len(value) > settings.max_json_string_length:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="JSON string exceeds the allowed limit",
            )
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if math.isfinite(value):
            return
    elif isinstance(value, list):
        if len(value) > settings.max_json_items:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="JSON collection exceeds the allowed limit",
            )
        for item in value:
            validate_json_value(item, depth=depth + 1)
        return
    elif isinstance(value, dict):
        if len(value) > settings.max_json_items:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="JSON collection exceeds the allowed limit",
            )
        for key, item in value.items():
            if not isinstance(key, str) or len(key) > settings.max_json_key_length:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="JSON key exceeds the allowed limit",
                )
            validate_json_value(item, depth=depth + 1)
        return
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail="Unsupported JSON value",
    )
