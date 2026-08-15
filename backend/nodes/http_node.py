"""HTTP 节点 — 发送 HTTP 请求。"""

import logging
import asyncio
import ipaddress
import os
import socket
from typing import Any
from urllib.parse import urlparse

import httpx

from .base import Node

logger = logging.getLogger(__name__)


class HTTPNode(Node):
    node_type = "http"
    label = "HTTP 请求"
    description = "发送 HTTP 请求到外部 API"
    color = "#f97316"
    config_schema = {
        "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"], "default": "GET"},
        "url": {"type": "string", "description": "请求 URL"},
        "headers": {"type": "object", "description": "请求头"},
        "body": {"type": "object", "description": "请求体 (JSON)"},
    }

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        method = self.config.get("method", "GET").upper()
        url = self._render_url(str(self.config.get("url", "")), inputs)
        headers = dict(self.config.get("headers") or {})
        body = self.config.get("body") or {}

        if not url:
            raise ValueError("请填写 URL")

        await self._validate_destination(url)
        logical_id = self.execution_context.get("logical_execution_id")
        if logical_id and method in ("POST", "PUT", "PATCH", "DELETE"):
            headers.setdefault("Idempotency-Key", str(logical_id))

        logger.info("HTTP 节点 %s → %s %s", self.node_id, method, url)

        async with httpx.AsyncClient(timeout=float(self.config.get("request_timeout", 30)), follow_redirects=False) as client:
            resp = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=body if method in ("POST", "PUT", "PATCH") else None,
            )
            resp.raise_for_status()

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

    def validate_config(self) -> list[str]:
        errors = []
        method = str(self.config.get("method", "GET")).upper()
        if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            errors.append(f"不支持的 HTTP 方法: {method}")
        if not self.config.get("url"):
            errors.append("url 不能为空")
        return errors

    @staticmethod
    async def _validate_destination(url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("HTTP URL 必须使用 http 或 https")
        allow_private = os.getenv("WORKFLOW_ALLOW_PRIVATE_HTTP", "false").lower() == "true"
        if allow_private:
            return
        try:
            infos = await asyncio.to_thread(socket.getaddrinfo, parsed.hostname, parsed.port or 443)
        except socket.gaierror as exc:
            raise ValueError(f"无法解析 HTTP 目标主机: {parsed.hostname}") from exc
        for info in infos:
            address = ipaddress.ip_address(info[4][0])
            if not address.is_global:
                raise ValueError(f"禁止访问内网或本机地址: {address}")

    @staticmethod
    def _render_url(url: str, inputs: dict[str, Any]) -> str:
        """替换 URL 中的 {{var}} 占位符"""
        import re

        def replace(match: re.Match[str]) -> str:
            var = match.group(1)
            return str(inputs.get(var, match.group(0)))

        return re.sub(r"\{\{\s*(\w+)\s*\}\}", replace, url)
