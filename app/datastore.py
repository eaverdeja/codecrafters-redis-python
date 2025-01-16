from typing import Any
from dataclasses import dataclass
from time import time


@dataclass
class Container:
    value: Any
    expiry: int | None


class Datastore(dict):
    def __getitem__(self, key):
        try:
            item = super().__getitem__(key)
        except KeyError:
            return None

        if item.expiry and time() > item.expiry:
            del self[key]
            return None
        return item.value

    def __setitem__(self, key, value):
        if not isinstance(value, Container):
            value = Container(value=value)
        return super().__setitem__(key, value)

    def write(self, key, value, expires_in):
        expiry = time() + int(expires_in)
        container = Container(value=value, expiry=expiry)
        self[key] = container
