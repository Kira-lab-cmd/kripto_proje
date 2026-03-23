# backend/middleware/error_handler.py
from __future__ import annotations

import logging
import uuid
from typing import Callable

from fastapi import Request
from fastapi.responses import JSONResponse
from ..trader import TradingPausedError

logger = logging.getLogger("error_handler")


def register_exception_handlers(app) -> None:
    @app.exception_handler(TradingPausedError)
    async def trading_paused_error_handler(request: Request, exc: TradingPausedError):
        request_id = getattr(request.state, "request_id", None)
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "code": "TRADING_PAUSED",
                    "message": str(exc) or "Trading paused due to a safety condition.",
                },
                "meta": {
                    "path": str(request.url.path),
                    "method": request.method,
                    "request_id": request_id,
                },
            },
            headers={"x-request-id": request_id} if request_id else {},
        )


async def error_handler_middleware(request: Request, call_next: Callable):
    """
    - Yakalanmayan exception'ları tek format JSON'a çevirir
    - request_id üretir ve response'a ekler
    - prod'da detay sızdırmaz
    """
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    request.state.request_id = request_id

    try:
        response = await call_next(request)
        response.headers["x-request-id"] = request_id
        return response
    except Exception as exc:
        # stacktrace log'a
        logger.exception(
            "Unhandled error",
            extra={
                "request_id": request_id,
                "path": str(request.url.path),
                "method": request.method,
            },
        )

        # kullanıcıya minimal hata
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "request_id": request_id,
            },
            headers={"x-request-id": request_id},
        )
