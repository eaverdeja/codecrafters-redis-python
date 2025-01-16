import socket
from concurrent.futures import ThreadPoolExecutor

from .parser import RedisProtocolParser

BUFFER_SIZE_BYTES = 4096
CRLF = "\r\n"
MAX_WORKERS = 5


def process_connection(conn: socket.SocketType):
    while request := conn.recv(BUFFER_SIZE_BYTES):
        query = RedisProtocolParser(data=request).parse()

        if "PING" in query:
            response = f"+PONG{CRLF}"
        elif "ECHO" in query:
            message = " ".join(query[1:])
            response = f"${len(message)}{CRLF}{message}{CRLF}"
        conn.send(response.encode())
    conn.close()


def main():
    server_socket = socket.create_server(("localhost", 6379))

    print("Listening on port 6379")
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while True:
                conn, _addr = server_socket.accept()
                executor.submit(process_connection, conn)
    except KeyboardInterrupt:
        print("Shutting down server")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
