import socket

BUFFER_SIZE_BYTES = 4096
CRLF = "\r\n"


def process_connection(conn: socket.SocketType):
    while request := conn.recv(BUFFER_SIZE_BYTES).decode():
        lines = request.split(CRLF)
        if "PING" in lines:
            response = f"+PONG{CRLF}".encode()
            conn.send(response)


def main():
    server_socket = socket.create_server(("localhost", 6379))

    print("Listening on port 6379")
    try:
        while True:
            conn, _addr = server_socket.accept()
            process_connection(conn)
            conn.close()
    except KeyboardInterrupt:
        print("Shutting down server")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
