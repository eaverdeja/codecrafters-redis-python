import asyncio
from dataclasses import asdict
import time

from .datastore import Datastore, StreamError
from .config import ServerInfo, RDBConfig
from .encoders import (
    encode_bulk_string,
    encode_simple_string,
    encode_array,
    encode_integer,
    encode_error,
)
from .utils import Container, calculate_expiry
from .events import EventBus, RedisEvent


class CommandHandler:
    def __init__(
        self,
        server_info: ServerInfo,
        rdb_config: RDBConfig,
        datastore: Datastore,
        event_bus: EventBus,
    ):
        self.server_info = server_info
        self.rdb_config = rdb_config
        self.datastore = datastore
        self.event_bus = event_bus

    async def handle_command(
        self,
        query: list[str],
        *,
        writer: asyncio.StreamWriter,
        offset: int = 0,
        replicas: dict | None = None,
    ) -> str:
        query[0] = query[0].upper()
        match query:
            case ["PING"]:
                return encode_simple_string("PONG")
            case ["ECHO", *rest]:
                message = " ".join(rest)
                return encode_bulk_string(message)
            case ["SET", key, value, "px", expires_in]:
                expiry = calculate_expiry(expires_in)
                self.datastore[key] = Container(value=value, expiry=expiry)
                return encode_simple_string("OK")
            case ["SET", key, value]:
                self.datastore[key] = value
                return encode_simple_string("OK")
            case ["XADD", key, entry_id, *rest]:
                if len(rest) % 2 != 0:
                    raise ValueError(
                        "Additional arguments must come in pairs (key, value)"
                    )
                keys = rest[::2]
                values = rest[1::2]
                attributes = dict(zip(keys, values))
                try:
                    entry_id = self.datastore.add_to_stream(key, entry_id, attributes)
                except StreamError as e:
                    return encode_error(e)
                return encode_bulk_string(entry_id)
            case ["XRANGE", key, start, end]:
                entries = [
                    encode_array(
                        [
                            encode_bulk_string(entry.entry_id),
                            encode_array(
                                [
                                    item
                                    for key, value in entry.attributes.items()
                                    for item in (
                                        encode_bulk_string(key),
                                        encode_bulk_string(value),
                                    )
                                ]
                            ),
                        ]
                    )
                    for entry in self.datastore.query_from_stream(key, start, end)
                ]
                return encode_array(entries)
            case ["XREAD", "streams", *arguments]:
                return await self._handle_xread(*arguments)
            case ["XREAD", "block", blocking_time, "streams", *arguments]:
                return await self._handle_xread(
                    *arguments, blocking_time=int(blocking_time)
                )
            case ["GET", key]:
                value = self.datastore[key]
                return encode_bulk_string(value if value else None)
            case ["TYPE", key]:
                if self.datastore[key]:
                    return encode_simple_string("string")
                if self.datastore.peek(key):
                    return encode_simple_string("stream")
                return encode_simple_string("none")
            case ["CONFIG", "GET", config]:
                data = [encode_bulk_string(config)]
                if config == "dir":
                    data.append(encode_bulk_string(self.rdb_config.directory))
                elif config == "dbfilename":
                    data.append(encode_bulk_string(self.rdb_config.filename))
                else:
                    raise Exception("Unknown config")
                return encode_array(data)
            case ["KEYS", _pattern]:
                keys = self.datastore.keys()
                return encode_array([encode_bulk_string(key) for key in keys])
            case ["INFO", _section]:
                encoded_info = [
                    f"{key}:{value}" for key, value in asdict(self.server_info).items()
                ]
                return encode_bulk_string("\n".join(encoded_info))
            case ["REPLCONF", "listening-port", port]:
                client_addr = writer.get_extra_info("peername")
                self.event_bus.emit(
                    RedisEvent(
                        type="replica_connected",
                        data={"addr": client_addr, "port": port, "connection": writer},
                    )
                )
                return encode_simple_string("OK")
            case ["REPLCONF", "capa", *capabilities]:
                client_addr = writer.get_extra_info("peername")
                self.event_bus.emit(
                    RedisEvent(
                        type="replica_capabilities",
                        data={"addr": client_addr, "capabilities": capabilities},
                    )
                )
                return encode_simple_string("OK")
            case ["REPLCONF", "GETACK", "*"]:
                return encode_array(
                    [
                        encode_bulk_string("REPLCONF"),
                        encode_bulk_string("ACK"),
                        encode_bulk_string(str(offset)),
                    ]
                )
            case ["PSYNC", "?", "-1"]:
                return encode_simple_string(
                    f"FULLRESYNC {self.server_info.master_replid} {self.server_info.master_repl_offset}"
                )
            case ["WAIT", num_replicas, timeout]:
                num_replicas = int(num_replicas)
                timeout = int(timeout)

                now = time.time()
                caught_up_replicas = set()
                while True:
                    for replica in replicas.values():
                        if replica.offset >= offset:
                            caught_up_replicas.add(replica.port)

                    if len(caught_up_replicas) >= num_replicas:
                        break

                    # time is in seconds and timeout is in milliseconds
                    if time.time() - now >= timeout / 1e3:
                        break

                    # sleep a bit before checking again
                    await asyncio.sleep(0.01)

                return encode_integer(len(caught_up_replicas))
            case ["COMMAND", "DOCS"]:
                return encode_simple_string("not_implemented")
            case _:
                raise Exception(f"Unsupported command: {query}")

    async def _handle_xread(self, *rest, blocking_time: int = -1):
        if len(rest) % 2 != 0:
            raise ValueError(
                "Additional arguments must come in pairs (stream_key, entry_id)"
            )
        middle = int(len(rest) / 2)
        stream_keys = rest[:middle]
        entry_ids = rest[middle:]

        response = []
        for stream_key, entry_id in zip(stream_keys, entry_ids):
            stream = self.datastore.query_from_stream(
                stream_key, start=entry_id, inclusive=False
            )
            if blocking_time != -1:
                stream = await self._wait_for_stream(
                    stream_key, entry_id, blocking_time
                )

            if len(stream) == 0:
                return encode_bulk_string(None)

            entries = encode_array(
                [
                    encode_array(
                        [
                            encode_bulk_string(entry.entry_id),
                            encode_array(
                                [
                                    item
                                    for entry_key, value in entry.attributes.items()
                                    for item in (
                                        encode_bulk_string(entry_key),
                                        encode_bulk_string(value),
                                    )
                                ]
                            ),
                        ]
                    )
                    for entry in stream
                ]
            )
            response.append(
                encode_array(
                    [
                        encode_bulk_string(stream_key),
                        entries,
                    ]
                )
            )
        return encode_array(response)

    async def _wait_for_stream(
        self, stream_key: str, entry_id: str, blocking_time: int
    ):
        now = time.time()
        stream = []
        # Is blocking_time positive?
        # Then wait for some time
        if blocking_time > 0:
            while time.time() - now < blocking_time / 10e2:
                stream = self.datastore.query_from_stream(
                    stream_key, start=entry_id, inclusive=False
                )
                await asyncio.sleep(0.01)
            return stream

        # Otherwise wait until we have data
        while True:
            stream = self.datastore.query_from_stream(
                stream_key, start=entry_id, inclusive=False
            )
            if len(stream) > 0:
                break
            await asyncio.sleep(0.01)
        return stream
