from time import time
from collections import defaultdict, OrderedDict

from .utils import Container


class StreamError(Exception):
    pass


Attributes = dict[str, str]
Stream = OrderedDict[str, Attributes]


class Datastore(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._streams: dict[str, Stream] = defaultdict(OrderedDict)

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

    def add_to_stream(self, key: str, entry_id: str, attributes: dict[str, str]):
        stream = self._streams[key]
        time, sequence = entry_id.split("-")
        time, sequence = int(time), int(sequence)

        if time == 0 and sequence == 0:
            raise StreamError("The ID specified in XADD must be greater than 0-0")

        top_item_entry_id = self.peek(key)
        if top_item_entry_id:
            top_item_time, top_item_sequence = top_item_entry_id.split("-")
            top_item_time, top_item_sequence = int(top_item_time), int(
                top_item_sequence
            )
            if time < top_item_time:
                raise StreamError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                )
            elif time == top_item_time and sequence <= top_item_sequence:
                raise StreamError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                )

        stream[entry_id] = attributes
        return

    def peek(self, key: str):
        """
        Returns the key of the most recent entry on the stream.
        Returns none if no entries exist in the stream.
        """
        stream = self._streams[key]
        try:
            return next(reversed(stream))
        except StopIteration:
            return None
