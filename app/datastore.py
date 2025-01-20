from time import time
from collections import deque, defaultdict
from typing import Deque

from .utils import Container, StreamContainer


class Datastore(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._streams: dict[str, Deque[StreamContainer]] = defaultdict(deque)

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

    def enqueue(self, key: str, entry_id: str, attributes: dict[str, str]):
        container = StreamContainer(entry_id=entry_id, attributes=attributes)
        self._streams[key].append(container)

    def peek(self, key: str):
        queue = self._streams[key]
        try:
            return queue[0]
        except IndexError:
            return None
