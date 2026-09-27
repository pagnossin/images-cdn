import asyncio
import math
import random
import time
from collections.abc import Awaitable, Callable


def backoff_delay(attempt: int, base: float, cap: float, rng: Callable[[], float] = random.random) -> float:
    # exponential with full jitter: uniform in [0, min(cap, base * 2**attempt)]
    return rng() * min(cap, math.ldexp(base, min(attempt, 32)))


class CircuitBreaker:
    def __init__(self, threshold: int = 3, reset_after: float = 5.0, clock: Callable[[], float] = time.monotonic):
        self._threshold = threshold
        self._reset_after = reset_after
        self._clock = clock
        self._failures: dict[str, int] = {}
        self._opened_at: dict[str, float] = {}

    def allow(self, peer: str) -> bool:
        opened_at = self._opened_at.get(peer)
        return opened_at is None or self._clock() - opened_at >= self._reset_after

    def success(self, peer: str) -> None:
        self._failures.pop(peer, None)
        self._opened_at.pop(peer, None)

    def failure(self, peer: str) -> None:
        self._failures[peer] = self._failures.get(peer, 0) + 1
        if self._failures[peer] >= self._threshold:
            self._opened_at[peer] = self._clock()

    def state(self, peer: str) -> str:
        if peer not in self._opened_at:
            return "closed"
        return "half-open" if self.allow(peer) else "open"


class SingleFlight[T]:
    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Task[T]] = {}

    async def do(self, key: str, fn: Callable[[], Awaitable[T]]) -> T:
        task = self._inflight.get(key)
        if task is None:
            # own task, so a caller that goes away doesn't cancel it for the others
            task = asyncio.ensure_future(fn())
            self._inflight[key] = task
            task.add_done_callback(lambda done: self._forget(key, done))
        return await asyncio.shield(task)

    def _forget(self, key: str, task: asyncio.Task[T]) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]
        if not task.cancelled():
            task.exception()

    def __len__(self) -> int:
        return len(self._inflight)


class Overloaded(Exception):
    pass


class Bulkhead:
    def __init__(self, limit: int, max_wait: float):
        self._semaphore = asyncio.Semaphore(limit)
        self._max_wait = max_wait

    async def run[R](self, fn: Callable[[], Awaitable[R]]) -> R:
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=self._max_wait)
        except TimeoutError:
            raise Overloaded("too many concurrent operations") from None
        try:
            return await fn()
        finally:
            self._semaphore.release()
