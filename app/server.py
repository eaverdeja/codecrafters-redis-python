import asyncio
import string
import random

from .config import RDBConfig, ServerInfo
from .datastore import Datastore
from .parsers import RDBParser, RedisProtocolParser
from .command_handler import CommandHandler
from .replication import ReplicationManager
from .events import EventBus
from .constants import BUFFER_SIZE_BYTES


class RedisServer:
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
        self._setup()

    def _setup(self):
        self.info = ServerInfo(role="slave" if self.replica_of else "master")

        if self.info.role == "master":
            alphanumerics = string.ascii_letters + string.digits
            self.info.master_replid = "".join(random.choices(alphanumerics, k=40))
            self.info.master_repl_offset = 0

        self._load_rdb()

        self.event_bus = EventBus()
        self.command_handler = CommandHandler(
            server_info=self.info,
            rdb_config=self.rdb_config,
            datastore=self.datastore,
            event_bus=self.event_bus,
        )
        self.replication_manager = ReplicationManager(
            server_info=self.info,
            event_bus=self.event_bus,
            command_handler=self.command_handler,
        )

    def _load_rdb(self):
        if not self.rdb_config.directory or not self.rdb_config.filename:
            return

        try:
            parser = RDBParser.from_file(self.rdb_config.file_path)
            records = parser.parse()
            for key, value in records.items():
                self.datastore[key] = value
        except FileNotFoundError:
            pass

    async def _process_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            while data := await reader.read(BUFFER_SIZE_BYTES):
                parser = RedisProtocolParser(data=data)
                while query := parser.parse():
                    response = self.command_handler.handle_command(query, writer)

                    writer.write(response.encode())
                    await writer.drain()

                    if "SET" in query and "OK" in response:
                        await self.replication_manager.handle_replication(data)
                    if "FULLRESYNC" in response:
                        await self.replication_manager.handle_full_resync(writer)
        except Exception as e:
            print(f"Error processing connection: {e.__class__.__name__} - {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def execute(self):
        server = await asyncio.start_server(
            self._process_connection, host="localhost", port=self.port
        )

        if self.replica_of:
            await self.replication_manager.connect_to_master(self.replica_of, self.port)

        print(f"Listening on port {self.port}")
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            print("Shutting down server")
            await self.replication_manager.cleanup()
        finally:
            server.close()
            await server.wait_closed()
