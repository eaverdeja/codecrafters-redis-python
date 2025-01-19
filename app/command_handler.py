import asyncio
from dataclasses import asdict

from .datastore import Datastore
from .config import ServerInfo, RDBConfig
from .encoders import (
    encode_bulk_string,
    encode_simple_string,
    encode_array,
    encode_integer,
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

    def handle_command(
        self,
        query: list[str],
        writer: asyncio.StreamWriter,
        offset: int = 0,
    ) -> str:
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
            case ["GET", key]:
                value = self.datastore[key]
                return encode_bulk_string(value if value else None)
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
            case ["WAIT", _num_replicas, _timeout]:
                return encode_integer(0)
            case _:
                raise Exception(f"Unsupported command: {query}")
