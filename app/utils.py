from dataclasses import dataclass
from time import time


@dataclass
class Container:
    value: any
    expiry: int | None = None


def calculate_expiry(expires_in: int) -> float:
    return time() + int(expires_in) / 1e3
