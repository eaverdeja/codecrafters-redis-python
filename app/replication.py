import asyncio
from pathlib import Path

from .parsers import RedisProtocolParser
from .config import ReplicaConfig, ServerInfo
from .encoders import encode_array, encode_bulk_string, encode_simple_string
from .constants import BUFFER_SIZE_BYTES
from .events import EventBus, RedisEvent
from .command_handler import CommandHandler


class ReplicationManager:
    def __init__(
        self,
        server_info: ServerInfo,
        event_bus: EventBus,
        command_handler: CommandHandler,
    ):
        self.info = server_info
        self.replicas: dict[tuple, ReplicaConfig] = {}
        self.master_connection: (
            tuple[asyncio.StreamReader, asyncio.StreamWriter] | None
        ) = None
        self.replication_task: asyncio.Task | None = None
        self.command_handler = command_handler

        event_bus.on("replica_connected", self._handle_replica_connected)
        event_bus.on("replica_capabilities", self._handle_replica_capabilities)

    async def handle_full_resync(self, writer: asyncio.StreamWriter):
        rdb_file_path = Path("./").parent / "empty.rdb"
        with open(rdb_file_path, "rb") as file:
            data = file.read()

        writer.write(f"${len(data)}\r\n".encode())
        await writer.drain()

        writer.write(data)
        await writer.drain()

    async def handle_replication(self, data: bytes):
        for replica in self.replicas.values():
            replica.connection.write(data)
            await replica.connection.drain()

    async def connect_to_master(self, replica_of: str, port: int):
        host, master_port = replica_of.split(" ")
        reader, writer = await asyncio.open_connection(host, master_port)
        self.master_connection = (reader, writer)

        await self._perform_handshake(port)
        await self._initialize_sync()

        self.replication_task = asyncio.create_task(self._handle_replication_stream())

    async def cleanup(self):
        for replica_info in self.replicas.values():
            writer = replica_info.connection
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

        if self.master_connection:
            _, writer = self.master_connection
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

        if self.replication_task:
            self.replication_task.cancel()

    async def _perform_handshake(self, port: int):
        _, writer = self.master_connection
        # PING
        request = encode_array([encode_simple_string("PING")])
        writer.write(request.encode())
        await writer.drain()
        await self._read_for("PONG")

        # 1st REPLCONF
        request = encode_array(
            [
                encode_bulk_string("REPLCONF"),
                encode_bulk_string("listening-port"),
                encode_bulk_string(str(port)),
            ]
        )
        writer.write(request.encode())
        await writer.drain()
        await self._read_for("OK")

        # 2nd REPLCONF
        request = encode_array(
            [
                encode_bulk_string("REPLCONF"),
                encode_bulk_string("capa"),
                encode_bulk_string("psync2"),
            ]
        )
        writer.write(request.encode())
        await writer.drain()
        await self._read_for("OK")

    async def _initialize_sync(self):
        reader, writer = self.master_connection
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

    async def _handle_replication_stream(self):
        if not self.master_connection:
            return

        reader, writer = self.master_connection
        try:
            while data := await reader.read(BUFFER_SIZE_BYTES):
                parser = RedisProtocolParser(data=data)
                while query := parser.parse():
                    self.command_handler.handle_command(query, writer)
        except Exception as e:
            print(f"Error processing replicated data: {e.__class__.__name__} - {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def _read_for(self, command: str):
        reader, _ = self.master_connection
        response = await reader.readline()

        query = RedisProtocolParser(data=response).parse()
        if query != command:
            raise Exception(f"Excepted response to be: {command}")

    def _handle_replica_connected(self, event: RedisEvent):
        addr = event.data["addr"]
        port = event.data["port"]
        connection = event.data["connection"]
        self.replicas[addr] = ReplicaConfig(
            port=port, connection=connection, capabilities=set()
        )

    def _handle_replica_capabilities(self, event: RedisEvent):
        addr = event.data["addr"]
        capabilities = event.data["capabilities"]
        if addr in self.replicas:
            self.replicas[addr].capabilities.update(capabilities)
        else:
            raise Exception("Unknown replica trying to set capabilities")
