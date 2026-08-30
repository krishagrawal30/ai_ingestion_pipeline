from __future__ import annotations

import json
import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in minimal environments
    def load_dotenv() -> bool:
        return False

from .chunking import semantic_chunks
from .retry import with_backoff

load_dotenv()

logger = logging.getLogger(__name__)
Provider = Callable[[str], Awaitable[dict[str, Any]]]


class LLMOrchestrator:
    """Multi-provider LLM orchestrator with automatic fallback.

    Providers are tried in order; if one fails the next is attempted.
    Payloads exceeding *max_chars* are split into semantic chunks and
    each chunk is sent independently, with results merged.
    """

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


def _parse_llm_json(content: str) -> dict[str, Any]:
    """Best-effort JSON extraction from LLM output."""
    text = content.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"content": content, "source_url": "unknown"}


# ---------------------------------------------------------------------------
# Provider: Google Gemini Flash
# ---------------------------------------------------------------------------

def gemini_provider(model: str = "gemini-3.1-flash-lite") -> Provider:
    """Google Gemini via the REST generateContent endpoint.

    gemini-2.0-flash is retired and gemini-2.5-flash is being sunset
    Oct 16 2026 — gemini-3.1-flash-lite is current as of Aug 2026. Check
    ai.google.dev/gemini-api/docs/models if this ever 404s."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing.")

    async def provider(prompt: str) -> dict[str, Any]:
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}"
            f":generateContent?key={api_key}"
        )
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                url,
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.1},
                },
            )
            response.raise_for_status()
            data = response.json()
            content = data["candidates"][0]["content"]["parts"][0]["text"]
            return _parse_llm_json(content)

    return provider


# ---------------------------------------------------------------------------
# Provider: Groq
# ---------------------------------------------------------------------------

def groq_provider(model: str = "openai/gpt-oss-120b") -> Provider:
    """Groq deprecated llama-3.1-8b-instant and llama-3.3-70b-versatile in
    June 2026 — openai/gpt-oss-120b is the current equivalent as of Aug
    2026. If this 404s later, check console.groq.com/docs/models."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is missing.")

    async def provider(prompt: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                },
            )
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            return _parse_llm_json(content)

    return provider


# ---------------------------------------------------------------------------
# Provider: DeepSeek
# ---------------------------------------------------------------------------

def deepseek_provider(model: str = "deepseek-v4-flash") -> Provider:
    """DeepSeek via their OpenAI-compatible chat completions endpoint.

    DeepSeek discontinued deepseek-chat / deepseek-reasoner in July 2026 —
    deepseek-v4-flash is the current equivalent as of Aug 2026."""
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is missing.")

    async def provider(prompt: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                "https://api.deepseek.com/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                },
            )
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            return _parse_llm_json(content)

    return provider


# ---------------------------------------------------------------------------
# Convenience: build the full fallback chain
# ---------------------------------------------------------------------------

def build_provider_chain() -> list[tuple[str, Provider]]:
    """Return all available providers in priority order: Gemini → Groq → DeepSeek.

    Providers whose API keys are missing are silently skipped so the
    pipeline can run with whatever credentials are available.
    """
    chain: list[tuple[str, Provider]] = []
    for name, factory in [("gemini", gemini_provider), ("groq", groq_provider), ("deepseek", deepseek_provider)]:
        try:
            chain.append((name, factory()))
        except RuntimeError:
            logger.info("provider_skipped provider=%s reason=missing_api_key", name)
    return chain