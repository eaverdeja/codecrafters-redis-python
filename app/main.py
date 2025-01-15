import socket

BUFFER_SIZE_BYTES = 1024
CRLF = "\r\n"


def main():
    server_socket = socket.create_server(("localhost", 6379))

    print("Listening on port 6379")
    try:
        while True:
            connection, _addr = server_socket.accept()

            request = connection.recv(BUFFER_SIZE_BYTES).decode()
            commands = request.split("\n")
            for command in commands:
                lines = command.split(CRLF)
                if "PING" in lines:
                    response = f"+PONG{CRLF}".encode()
                    connection.send(response)

            connection.close()
    except KeyboardInterrupt:
        print("Shutting down server")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
