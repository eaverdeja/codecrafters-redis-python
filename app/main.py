import asyncio
import argparse
from dataclasses import dataclass, asdict
from pathlib import Path
import random
import string

from .parsers import RedisProtocolParser, RDBParser
from .encoders import (
    encode_bulk_string,
    encode_simple_string,
    encode_array,
)
from .datastore import Datastore
from .constants import BUFFER_SIZE_BYTES
from .utils import Container, calculate_expiry


@dataclass
class RDBConfig:
    directory: str
    filename: str


@dataclass
class ReplicaConfig:
    port: int
    connection: asyncio.StreamWriter
    capabilities: set


@dataclass
class ServerInfo:
    role: str
    master_replid: str | None = None
    master_repl_offset: str | None = None


class RedisServer:
    info: ServerInfo
    master_connection: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None
    replication_task: asyncio.Task | None

    def __init__(
        self,
        port: int,
        replica_of: str | None,
        datastore: Datastore,
        rdb_config: RDBConfig,
    ):
        self.port = port
        self.datastore = datastore
        self.rdb_config = rdb_config
        self.replica_of = replica_of
        self.replicas: dict[tuple, ReplicaConfig] = {}
        self.replication_task = None
        self.master_connection = None

        self._setup()

    def _setup(self):
        if self.replica_of:
            self.info = ServerInfo(role="slave")
        else:
            self.info = ServerInfo(role="master")

        if self.info.role == "master":
            alphanumerics = string.ascii_letters + string.digits
            self.info.master_replid = "".join(random.choices(alphanumerics, k=40))
            self.info.master_repl_offset = 0

        # Merge RDB with in-memory datastore
        records = self._get_records_from_rdb()
        for key, value in records.items():
            self.datastore[key] = value

    def _process_query(self, query: list[str], writer: asyncio.StreamWriter) -> str:
        match query:
            case ["PING"]:
                response = encode_simple_string("PONG")
            case ["ECHO", *rest]:
                message = " ".join(rest)
                response = encode_bulk_string(message)
            case ["SET", key, value, "px", expires_in]:
                expiry = calculate_expiry(expires_in)
                self.datastore[key] = Container(value=value, expiry=expiry)
                response = encode_simple_string("OK")
            case ["SET", key, value]:
                self.datastore[key] = value
                response = encode_simple_string("OK")
            case ["GET", key]:
                value = self.datastore[key]
                if value:
                    response = encode_bulk_string(value)
                else:
                    response = encode_bulk_string(None)
            case ["CONFIG", "GET", config]:
                data = [encode_bulk_string(config)]
                if config == "dir":
                    data.append(encode_bulk_string(self.rdb_config.directory))
                elif config == "dbfilename":
                    data.append(encode_bulk_string(self.rdb_config.filename))
                else:
                    raise Exception("Unknown config")
                response = encode_array(data)
            case ["KEYS", _pattern]:
                keys = self.datastore.keys()

                response = encode_array([encode_bulk_string(key) for key in keys])
            case ["INFO", _section]:
                encoded_info = [
                    f"{key}:{value}" for key, value in asdict(self.info).items()
                ]
                response = encode_bulk_string("\n".join(encoded_info))
            case ["REPLCONF", "listening-port", port]:
                client_addr = writer.get_extra_info("peername")
                self.replicas[client_addr] = ReplicaConfig(
                    port=port,
                    connection=writer,
                    capabilities=set(),
                )
                response = encode_simple_string("OK")
            case ["REPLCONF", "capa", *capabilities]:
                client_addr = writer.get_extra_info("peername")
                if client_addr in self.replicas:
                    self.replicas[client_addr].capabilities.update(capabilities)
                    response = encode_simple_string("OK")
                else:
                    raise Exception("Unknown replica trying to set capabilities")
            case ["PSYNC", "?", "-1"]:
                response = encode_simple_string(
                    f"FULLRESYNC {self.info.master_replid} {self.info.master_repl_offset}"
                )
            case _:
                raise Exception(f"Unsupported command: {query}")

        return response

    async def _handle_full_resync(self, writer: asyncio.StreamWriter):
        # FULLRESYNC should be handled by sending
        # the RDB file to the replica.
        # We'll use an empty RDB file for our purposes.
        rdb_file_path = Path("./").parent / "empty.rdb"
        with open(rdb_file_path, "rb") as file:
            data = file.read()

        # Send the length of the file
        writer.write(f"${len(data)}\r\n".encode())
        await writer.drain()

        # Send the actual RDB file
        writer.write(data)
        await writer.drain()

    async def _handle_replication(self, data: bytes):
        for replica in self.replicas.values():
            replica.connection.write(data)
            await replica.connection.drain()

    def _get_records_from_rdb(self) -> dict[str, Container]:
        if not self.rdb_config.directory or not self.rdb_config.filename:
            return {}

        file_path = Path(self.rdb_config.directory) / self.rdb_config.filename
        try:
            parser = RDBParser.from_file(file_path)
            records = parser.parse()
        except FileNotFoundError:
            # If the file does not exist,
            # treat it as an empty DB
            records = {}
        return records

    async def _process_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            while data := await reader.read(BUFFER_SIZE_BYTES):
                query = RedisProtocolParser(data=data).parse()
                response = self._process_query(query, writer)

                writer.write(response.encode())
                await writer.drain()

                if "SET" in query and "OK" in response:
                    await self._handle_replication(data)
                if "FULLRESYNC" in response:
                    await self._handle_full_resync(writer)
        except Exception as e:
            print(f"Error processing connection: {e.__class__.__name__} - {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def _connect_to_master(self):
        host, port = self.replica_of.split(" ")
        reader, writer = await asyncio.open_connection(host, port)
        self.master_connection = (reader, writer)

        # 1st part of the handshake - send a PING
        request = encode_array([encode_simple_string("PING")])
        writer.write(request.encode())
        await writer.drain()

        # Wait for the pong
        response = await reader.readline()
        query = RedisProtocolParser(data=response).parse()
        if query != "PONG":
            raise Exception("Excepted response to be a PONG command")

        # 2nd part of the handshake - send the first REPLCONF
        request = encode_array(
            [
                encode_bulk_string("REPLCONF"),
                encode_bulk_string("listening-port"),
                encode_bulk_string(str(self.port)),
            ]
        )
        writer.write(request.encode())
        await writer.drain()

        # Wait for the OK
        response = await reader.readline()
        query = RedisProtocolParser(data=response).parse()
        if query != "OK":
            raise Exception("Excepted response to be an OK")

        # 3rd part of the handshake - send the second REPLCONF
        request = encode_array(
            [
                encode_bulk_string("REPLCONF"),
                encode_bulk_string("capa"),
                encode_bulk_string("psync2"),
            ]
        )
        writer.write(request.encode())
        await writer.drain()

        # Wait for the OK
        response = await reader.readline()
        query = RedisProtocolParser(data=response).parse()
        if query != "OK":
            raise Exception("Excepted response to be an OK")

        # PSYNC
        request = encode_array(
            [
                encode_bulk_string("PSYNC"),
                encode_bulk_string("?"),
                encode_bulk_string("-1"),
            ]
        )
        writer.write(request.encode())
        await writer.drain()

        # Wait for the FULLRESYNC response
        response = await reader.readline()
        query = RedisProtocolParser(data=response).parse()
        if "FULLRESYNC" not in query:
            raise Exception("Excepted response to be a FULLRESYNC")

        # Wait for the RDB file
        response = await reader.readline()
        file_length = response.split(b"$")[-1]
        await reader.readexactly(int(file_length))

        # Handle incoming data in a separate task to avoid blocking
        self.replication_task = asyncio.create_task(self._handle_replication_stream())

    async def _handle_replication_stream(self):
        reader, writer = self.master_connection
        try:
            while data := await reader.read(BUFFER_SIZE_BYTES):
                print(f"reading data from master: {data}")
                query = RedisProtocolParser(data=data).parse()
                print(f"handling query: {query}")
                self._process_query(query, writer)
        except Exception as e:
            print(f"Error processing replicated data: {e.__class__.__name__} - {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def _cleanup(self):
        # Close all replica connections
        for replica_info in self.replicas.values():
            writer = replica_info.connection
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

        # If we're a replica, close master connection
        if self.master_connection:
            _, writer = self.master_connection
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

        if self.replication_task:
            self.replication_task.cancel()

    async def execute(self):
        server = await asyncio.start_server(
            self._process_connection, host="localhost", port=self.port
        )
        if self.replica_of:
            await self._connect_to_master()

        print(f"Listening on port {self.port}")
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            print("Shutting down server")
            await self._cleanup()
        finally:
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple Redis server")
    parser.add_argument("--dir", type=str, help="The directory of the RDB file")
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    parser.add_argument(
        "--port", type=int, help="The port to run on. Defaults to 6379", default=6379
    )
    parser.add_argument(
        "--replicaof",
        type=str,
        help="Master host and port information, used for replicas",
    )
    args = parser.parse_args()

    server = RedisServer(
        port=args.port,
        replica_of=args.replicaof,
        datastore=Datastore(),
        rdb_config=RDBConfig(directory=args.dir, filename=args.dbfilename),
    )

    try:
        asyncio.run(server.execute())
    except KeyboardInterrupt:
        print("Server stopped")
