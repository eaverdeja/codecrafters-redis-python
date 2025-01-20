from time import time
from collections import deque, defaultdict
from typing import Deque

from .utils import Container, StreamContainer


class EnqueueError(Exception):
    pass


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
        queue = self._streams[key]
        time, sequence = entry_id.split("-")
        time = int(time)
        sequence = int(sequence)

        if time == 0 and sequence == 0:
            raise EnqueueError("The ID specified in XADD must be greater than 0-0")

        top_item = self.peek(key)
        if top_item:
            top_item_time, top_item_sequence = top_item.entry_id.split("-")
            top_item_time = int(top_item_time)
            top_item_sequence = int(top_item_sequence)
            if time < top_item_time:
                raise EnqueueError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                )
            elif time == top_item_time and sequence <= top_item_sequence:
                raise EnqueueError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                )

        container = StreamContainer(entry_id=entry_id, attributes=attributes)
        queue.append(container)
        return

    def peek(self, key: str):
        queue = self._streams[key]
        try:
            return queue[-1]
        except IndexError:
            return None
