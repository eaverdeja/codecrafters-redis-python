import asyncio

from .parser import RedisProtocolParser

BUFFER_SIZE_BYTES = 4096
CRLF = "\r\n"


async def process_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    try:
        while data := await reader.read(BUFFER_SIZE_BYTES):
            query = RedisProtocolParser(data=data).parse()

            if "PING" in query:
                response = f"+PONG{CRLF}"
            elif "ECHO" in query:
                message = " ".join(query[1:])
                response = f"${len(message)}{CRLF}{message}{CRLF}"
            else:
                raise Exception("Unsupported command")

            writer.write(response.encode())
            await writer.drain()
    except Exception as e:
        print(f"Error processing connection: {e}")
    finally:
        writer.close()


async def main():
    server = await asyncio.start_server(process_connection, host="localhost", port=6379)

    print("Listening on port 6379")
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        print("Shutting down server")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped")
