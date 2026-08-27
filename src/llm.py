from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .chunking import semantic_chunks
from .retry import with_backoff

logger = logging.getLogger(__name__)
Provider = Callable[[str], Awaitable[dict[str, Any]]]


class LLMOrchestrator:
    def __init__(self, providers: list[tuple[str, Provider]], max_chars: int = 12000) -> None:
        self.providers = providers
        self.max_chars = max_chars

    async def extract(self, text: str) -> dict[str, Any]:
        if not text.strip():
            raise ValueError("cannot extract from empty text")
        errors: list[str] = []
        for name, provider in self.providers:
            try:
                chunks = semantic_chunks(text, self.max_chars, overlap=min(400, max(0, self.max_chars // 20)))
                outputs = [await with_backoff(lambda chunk=chunk: provider(chunk)) for chunk in chunks]
                return {key: value for output in outputs for key, value in output.items()}
            except Exception as error:
                errors.append(f"{name}: {error}")
                logger.warning("llm_provider_failed provider=%s error=%s", name, error)
        raise RuntimeError("all LLM providers failed: " + "; ".join(errors))


def grounded_json_provider(required_source_url: str) -> Provider:
    async def provider(_: str) -> dict[str, Any]:
        return {"source_url": required_source_url}

    return provider