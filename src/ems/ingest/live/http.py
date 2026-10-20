"""HTTP access for live sources: retries on 429/5xx/timeouts only, secret redaction,
recording hook.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = structlog.get_logger(__name__)

REDACTED = "***"
USER_AGENT = "ems-gen/0.1 (MSc thesis research; https://github.com)"


class SourceHTTPError(Exception):
    def __init__(self, status: int, url: str, body: str) -> None:
        super().__init__(f"HTTP {status} from {url}: {body[:300]}")
        self.status = status
        self.url = url
        self.body = body


class RetryableError(SourceHTTPError):
    """429, 5xx or a transport failure: worth retrying."""


class NonRetryableError(SourceHTTPError):
    """Other 4xx: the request itself is wrong (bad parameters, auth); never retried."""


@dataclass(frozen=True)
class Fetched:
    source: str
    url: str  # redacted
    status: int
    content_type: str
    body: bytes
    fetched_at: datetime

    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


Recorder = Callable[[Fetched], None]


def redact(text: str, secrets: list[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTED)
    return text


class Fetcher:
    """Shared client for one source. ``secrets`` are scrubbed from URLs, errors and recordings."""

    def __init__(
        self,
        source: str,
        *,
        client: httpx.AsyncClient | None = None,
        secrets: list[str] | None = None,
        attempts: int = 5,
        wait_initial_s: float = 1.0,
        wait_max_s: float = 60.0,
        recorder: Recorder | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.source = source
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=2),
            headers={"User-Agent": USER_AGENT},
            follow_redirects=False,
        )
        self._owns_client = client is None
        self._secrets = [s for s in (secrets or []) if s]
        self._attempts = attempts
        self._wait = wait_exponential_jitter(initial=wait_initial_s, max=wait_max_s)
        self._recorder = recorder
        self._clock = clock
        self._sleep = sleep

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _redacted_url(self, request: httpx.Request) -> str:
        return redact(str(request.url), self._secrets)

    async def _once(
        self, url: str, params: Mapping[str, Any] | None, headers: Mapping[str, str] | None
    ) -> Fetched:
        try:
            response = await self._client.get(url, params=params, headers=headers)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise RetryableError(
                0, redact(url, self._secrets), redact(str(exc), self._secrets)
            ) from exc
        shown = self._redacted_url(response.request)
        body = response.content
        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableError(response.status_code, shown, redact(response.text, self._secrets))
        if response.status_code >= 400:
            raise NonRetryableError(
                response.status_code, shown, redact(response.text, self._secrets)
            )
        fetched = Fetched(
            source=self.source,
            url=shown,
            status=response.status_code,
            content_type=response.headers.get("content-type", ""),
            body=redact(body.decode("utf-8", errors="replace"), self._secrets).encode()
            if self._secrets
            else body,
            fetched_at=self._clock(),
        )
        if self._recorder is not None:
            self._recorder(fetched)
        return fetched

    async def get(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Fetched:
        kwargs: dict[str, Any] = {
            "stop": stop_after_attempt(self._attempts),
            "wait": self._wait,
            "retry": retry_if_exception_type(RetryableError),
            "reraise": True,
        }
        if self._sleep is not None:
            kwargs["sleep"] = self._sleep
        async for attempt in AsyncRetrying(**kwargs):
            with attempt:
                n = attempt.retry_state.attempt_number
                if n > 1:
                    log.warning("http_retry", source=self.source, attempt=n)
                return await self._once(url, params, headers)
        raise AssertionError("unreachable")  # pragma: no cover
