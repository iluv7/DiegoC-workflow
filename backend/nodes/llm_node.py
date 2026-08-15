"""LLM 节点 — 调用 OpenAI 兼容的 Chat Completions API。"""

import logging
from typing import Any

import httpx

from .base import Node

logger = logging.getLogger(__name__)


class LLMNode(Node):
    node_type = "llm"
    label = "LLM"
    description = "调用 DeepSeek 等 OpenAI 兼容接口的大模型"
    color = "#3b82f6"
    config_schema = {
        "model": {"type": "string", "description": "模型名称", "default": "deepseek-v4-pro"},
        "api_key": {"type": "string", "description": "API Key"},
        "base_url": {"type": "string", "description": "API Base URL", "default": "https://api.deepseek.com/v1"},
        "system_prompt": {"type": "string", "description": "系统提示词，支持 {{local_var}} 变量"},
        "user_prompt": {"type": "string", "description": "用户提示词，支持 {{local_var}} 变量"},
        "temperature": {"type": "number", "description": "温度", "default": 0.7, "minimum": 0, "maximum": 2},
    }

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        model = self.config.get("model", "deepseek-v4-pro")
        api_key = self.config.get("api_key", "")
        base_url = self.config.get("base_url", "https://api.deepseek.com/v1").rstrip("/")
        system_prompt = str(self.config.get("system_prompt", ""))
        user_prompt = str(self.config.get("user_prompt", ""))
        temperature = float(self.config.get("temperature", 0.7))

        if not api_key:
            raise ValueError("请配置 api_key")

        # 用 inputs 中的值渲染模板变量（简单的 {{var}} 替换）
        system_prompt = self._render_template(system_prompt, inputs)
        user_prompt = self._render_template(user_prompt, inputs)

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})

        url = f"{base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": int(self.config.get("max_tokens", 4096)),
        }

        logger.info("LLM 调用 %s → %s", self.node_id, url)
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(url, json=body, headers=headers)
            if resp.status_code >= 400:
                error_body = resp.text
                logger.error("LLM 调用失败 %s: %s", resp.status_code, error_body[:500])
                raise ValueError(f"LLM API 错误 ({resp.status_code}): {error_body[:500]}")
            data = resp.json()

        choice = data["choices"][0]
        content = choice["message"]["content"]
        usage = data.get("usage", {})

        return {
            "text": content,
            "model": data.get("model", model),
            "tokens": usage.get("total_tokens", 0),
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
        }

    def validate_config(self) -> list[str]:
        errors = []
        if not self.config.get("api_key"):
            errors.append("api_key 不能为空")
        temperature = float(self.config.get("temperature", 0.7))
        if not 0 <= temperature <= 2:
            errors.append("temperature 必须在 0 到 2 之间")
        return errors

    @staticmethod
    def _render_template(template: str, inputs: dict[str, Any]) -> str:
        """简单的 {{var}} 模板渲染，不需要 Jinja2。"""
        import re

        def replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            if var_name in inputs:
                return str(inputs[var_name])
            return match.group(0)  # 未找到变量保留原文

        return re.sub(r"\{\{\s*(\w+)\s*\}\}", replace, template)
