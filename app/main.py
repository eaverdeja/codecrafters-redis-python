import asyncio

from .parser import RedisProtocolParser
from .encoders import encode_bulk_string, encode_simple_string
from .datastore import Datastore
from .constants import BUFFER_SIZE_BYTES

global datastore


def process_query(query: list[str]) -> str:
    match query:
        case ["PING"]:
            response = encode_simple_string("PONG")
        case ["ECHO", *rest]:
            message = " ".join(rest)
            response = encode_bulk_string(message)
        case ["SET", key, value, "px", expires_in]:
            datastore.write(key, value, expires_in)
            response = encode_simple_string("OK")
        case ["SET", key, value]:
            datastore[key] = value
            response = encode_simple_string("OK")
        case ["GET", key]:
            value = datastore[key]
            if value:
                response = encode_bulk_string(value)
            else:
                response = encode_bulk_string("-1")
        case _:
            raise Exception("Unsupported command")

    return response


async def process_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    try:
        while data := await reader.read(BUFFER_SIZE_BYTES):
            query = RedisProtocolParser(data=data).parse()
            response = process_query(query)

            writer.write(response.encode())
            await writer.drain()
    except Exception as e:
        print(f"Error processing connection: {e.__class__.__name__} - {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    server = await asyncio.start_server(process_connection, host="localhost", port=6379)

    print("Listening on port 6379")
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        print("Shutting down server")


if __name__ == "__main__":
    datastore = Datastore()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped")
