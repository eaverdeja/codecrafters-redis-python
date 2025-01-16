from collections import deque


class RedisProtocolError(Exception):
    pass


class RedisProtocolParser:
    """
    Simple implementation of a parser of
    the Redis Serialization Protocol (RESP)
    """

    def __init__(self, data: bytes):
        # Having a deque split by CRLF allows us
        # read from top to bottom by using popleft
        self.data = deque(data.split(b"\r\n"))

    def parse(self):
        if not self.data:
            raise RedisProtocolError("No data to parse")

        line = self.data.popleft()

        if line.startswith(b"*"):
            return self._parse_array(line)
        elif line.startswith(b"$"):
            return self._parse_bulk_string(line)
        else:
            raise RedisProtocolError(f"Unsupported data type: {line}")

    def _parse_array(self, line: bytes):
        # For array data types, the prefix will
        # contain the number of elements in the array
        number_of_elements = int(line[1:])

        # Null array
        if number_of_elements == -1:
            return None

        # Recursively parse the following elements
        return [self.parse() for _ in range(number_of_elements)]

    def _parse_bulk_string(self, line: bytes):
        # For bulk string data types, the prefix will
        # describe the length of the string
        string_length = int(line[1:])

        # Null bulk string
        if string_length == 1:
            return None

        content_line = self.data.popleft()
        if len(content_line) != string_length:
            raise RedisProtocolError(
                f"Length mismatch on bulk string, expected {string_length} got {len(content_line)}"
            )

        return content_line.decode()
