from __future__ import annotations

from collections import defaultdict


class FakeRedis:
    """Small Redis subset used by API and worker unit tests."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.counters: defaultdict[str, int] = defaultdict(int)
        self.streams: defaultdict[str, list[dict[str, str]]] = defaultdict(list)

    async def eval(self, _script: str, _keys: int, key: str, _ttl: int) -> int:
        self.counters[key] += 1
        return self.counters[key]

    async def set(
        self,
        key: str,
        value: str,
        *,
        ex: int,
        nx: bool = False,
    ) -> bool | None:
        del ex
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def getdel(self, key: str) -> str | None:
        return self.values.pop(key, None)

    async def exists(self, key: str) -> int:
        return int(key in self.values)

    async def xadd(self, stream: str, fields: dict[str, str]) -> str:
        self.streams[stream].append(dict(fields))
        return f"{len(self.streams[stream])}-0"

    async def aclose(self) -> None:
        return None
