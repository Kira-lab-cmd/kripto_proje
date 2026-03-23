from __future__ import annotations

import asyncio
import json
import unittest

from fastapi import FastAPI
from starlette.requests import Request

from backend.middleware.error_handler import register_exception_handlers
from backend.trader import TradingPausedError


class TradingPausedErrorHandlerTests(unittest.TestCase):
    def test_trading_paused_returns_503(self) -> None:
        app = FastAPI()
        register_exception_handlers(app)
        handler = app.exception_handlers[TradingPausedError]

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/boom",
            "headers": [],
            "query_string": b"",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 123),
            "root_path": "",
            "app": app,
        }
        request = Request(scope)
        request.state.request_id = "req-123"
        resp = asyncio.run(handler(request, TradingPausedError("DB persistence failed; trading paused for safety.")))

        self.assertEqual(resp.status_code, 503)
        body = json.loads(resp.body.decode("utf-8"))
        self.assertEqual(body["error"]["code"], "TRADING_PAUSED")
        self.assertEqual(body["error"]["message"], "DB persistence failed; trading paused for safety.")
        self.assertEqual(body["meta"]["path"], "/boom")
        self.assertEqual(body["meta"]["method"], "GET")


if __name__ == "__main__":
    unittest.main()
