"""AC-002-09/-10: retry policy and secret redaction of the live HTTP helper (respx, no network)."""

from __future__ import annotations

import asyncio

import httpx
import pytest
import respx

from ems.ingest.live.http import Fetched, Fetcher, NonRetryableError, Recorder, RetryableError

URL = "https://example.test/api"


async def no_sleep(_s: float) -> None:
    return None


def fetcher(secrets: list[str] | None = None, recorder: Recorder | None = None) -> Fetcher:
    return Fetcher(
        "test",
        client=httpx.AsyncClient(),
        sleep=no_sleep,
        attempts=4,
        secrets=secrets,
        recorder=recorder,
    )


@respx.mock
def test_ac_002_09_retries_429_and_5xx_then_succeeds() -> None:
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(429),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    got = asyncio.run(fetcher().get(URL))
    assert got.status == 200 and route.call_count == 3


@respx.mock
@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_ac_002_09_client_errors_are_not_retried(status: int) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(status, text="nope"))
    with pytest.raises(NonRetryableError):
        asyncio.run(fetcher().get(URL))
    assert route.call_count == 1


@respx.mock
def test_gives_up_after_the_attempt_budget() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(500))
    with pytest.raises(RetryableError):
        asyncio.run(fetcher().get(URL))
    assert route.call_count == 4


@respx.mock
def test_timeouts_are_retried() -> None:
    route = respx.get(URL).mock(
        side_effect=[httpx.ConnectTimeout("t"), httpx.Response(200, json={})]
    )
    assert asyncio.run(fetcher().get(URL)).status == 200 and route.call_count == 2


@respx.mock
def test_ac_002_10_secrets_never_reach_urls_errors_or_recordings() -> None:
    secret = "s3cr3t-key-123"
    respx.get(URL).mock(return_value=httpx.Response(200, text=f'{{"echo": "{secret}"}}'))
    recorded: list[Fetched] = []
    f = fetcher(secrets=[secret], recorder=recorded.append)
    got = asyncio.run(f.get(URL, params={"apikey": secret}))
    assert secret not in got.url and "***" in got.url
    assert secret.encode() not in got.body and secret not in recorded[0].text()
    respx.get(URL).mock(return_value=httpx.Response(401, text=f"bad key {secret}"))
    with pytest.raises(NonRetryableError) as err:
        asyncio.run(f.get(URL, params={"apikey": secret}))
    assert secret not in str(err.value)
