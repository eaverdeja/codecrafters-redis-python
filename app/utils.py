from dataclasses import dataclass
from time import time


@dataclass
class Container:
    value: any
    expiry: int | None = None


@dataclass
class StreamContainer:
    entry_id: str
    attributes: dict[str, str]


def calculate_expiry(expires_in: int) -> float:
    return time() + int(expires_in) / 1e3
