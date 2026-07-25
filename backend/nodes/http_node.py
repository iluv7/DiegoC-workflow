"""HTTP 节点 — 发送 HTTP 请求。"""

import logging
from typing import Any

import httpx

from .base import Node

logger = logging.getLogger(__name__)


class HTTPNode(Node):
    node_type = "http"
    label = "HTTP 请求"
    description = "发送 HTTP 请求到外部 API"
    color = "#f97316"
    config_schema = {
        "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE"], "default": "GET"},
        "url": {"type": "string", "description": "请求 URL"},
        "headers": {"type": "object", "description": "请求头"},
        "body": {"type": "object", "description": "请求体 (JSON)"},
    }

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        method = self.config.get("method", "GET").upper()
        url = self._render_url(str(self.config.get("url", "")), inputs)
        headers = self.config.get("headers") or {}
        body = self.config.get("body") or {}

        if not url:
            raise ValueError("请填写 URL")

        logger.info("HTTP 节点 %s → %s %s", self.node_id, method, url)

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=body if method in ("POST", "PUT") else None,
            )

        content_type = resp.headers.get("content-type", "")
        try:
            data = resp.json() if "json" in content_type else resp.text
        except Exception:
            data = resp.text

        return {
            "status_code": resp.status_code,
            "data": data,
            "headers": dict(resp.headers),
        }

    @staticmethod
    def _render_url(url: str, inputs: dict[str, Any]) -> str:
        """替换 URL 中的 {{var}} 占位符"""
        import re

        def replace(match: re.Match[str]) -> str:
            var = match.group(1)
            return str(inputs.get(var, match.group(0)))

        return re.sub(r"\{\{\s*(\w+)\s*\}\}", replace, url)
