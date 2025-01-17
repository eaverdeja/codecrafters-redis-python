import asyncio
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .parsers import RedisProtocolParser, RDBParser
from .encoders import encode_bulk_string, encode_simple_string, encode_array
from .datastore import Datastore
from .constants import BUFFER_SIZE_BYTES


@dataclass
class RDBConfig:
    directory: str
    filename: str


class RedisServer:
    def __init__(self, datastore: Datastore, rdb_config: RDBConfig):
        self.datastore = datastore
        self.rdb_config = rdb_config
        self.server: RedisServer = None

    def _process_query(self, query: list[str]) -> str:
        match query:
            case ["PING"]:
                response = encode_simple_string("PONG")
            case ["ECHO", *rest]:
                message = " ".join(rest)
                response = encode_bulk_string(message)
            case ["SET", key, value, "px", expires_in]:
                self.datastore.write(key, value, expires_in)
                response = encode_simple_string("OK")
            case ["SET", key, value]:
                self.datastore[key] = value
                response = encode_simple_string("OK")
            case ["GET", key]:
                records = self._get_records_from_rdb()
                value = self.datastore[key] or records.get(key)
                if value:
                    response = encode_bulk_string(value)
                else:
                    response = encode_bulk_string("-1")
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
                records = self._get_records_from_rdb()
                keys = list(records.keys()) + list(self.datastore.keys())

                response = encode_array([encode_bulk_string(key) for key in keys])
            case _:
                raise Exception(f"Unsupported command: {query}")

        return response

    def _get_records_from_rdb(self) -> dict[str, Any]:
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
                response = self._process_query(query)

                writer.write(response.encode())
                await writer.drain()
        except Exception as e:
            print(f"Error processing connection: {e.__class__.__name__} - {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def execute(self):
        server = await asyncio.start_server(
            self._process_connection, host="localhost", port=6379
        )

        print("Listening on port 6379")
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            print("Shutting down server")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple Redis server")
    parser.add_argument("--dir", type=str, help="The directory of the RDB file")
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    args = parser.parse_args()

    server = RedisServer(
        datastore=Datastore(),
        rdb_config=RDBConfig(directory=args.dir, filename=args.dbfilename),
    )

    try:
        asyncio.run(server.execute())
    except KeyboardInterrupt:
        print("Server stopped")
