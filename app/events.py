from dataclasses import dataclass
from typing import Protocol, Literal, Any
from collections import defaultdict

EventTypes = Literal["replica_connected", "replica_capabilities"]


@dataclass
class RedisEvent:
    type: EventTypes
    data: Any


class EventListener(Protocol):
    def __call__(self, event: RedisEvent) -> None: ...


class EventBus:
    def __init__(self):
        self._listeners: dict[str, list[EventListener]] = defaultdict(list)

    def on(self, event_type: EventTypes, listener: EventListener) -> None:
        self._listeners[event_type].append(listener)

    def emit(self, event: RedisEvent) -> None:
        for listener in self._listeners[event.type]:
            listener(event)
