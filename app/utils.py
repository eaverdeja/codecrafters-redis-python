from dataclasses import dataclass


@dataclass
class Container:
    value: any
    expiry: int | None = None
