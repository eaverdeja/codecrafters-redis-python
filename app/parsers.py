from collections import deque
import builtins

from .utils import Container


class RedisProtocolError(Exception):
    pass


class RDBProtocolError(Exception):
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
        if string_length == -1:
            return None

        content_line = self.data.popleft()
        if len(content_line) != string_length:
            raise RedisProtocolError(
                f"Length mismatch on bulk string, expected {string_length} got {len(content_line)}"
            )

        return content_line.decode()


class RDBParser:
    MAGIC_STRING = b"REDIS"
    VERSION_SIZE = 4

    # See https://rdb.fnordig.de/file_format.html#op-codes
    METADATA_FIELD_MARKER = 0xFA
    DATABASE_START_MARKER = 0xFE
    HASH_TABLE_MARKER = 0xFB
    EOF_MARKER = 0xFF
    EXPIRY_IN_SECONDS_MARKER = 0xFD
    EXPIRY_IN_MILLISECONDS_MARKER = 0xFC

    def __init__(self, data: bytes):
        self.buffer = data
        self.pos = 0

    @classmethod
    def from_file(cls, file_path: str):
        with open(file_path, "rb") as file:
            return cls(file.read())

    def parse(self):
        self._parse_header()
        self._parse_metadata()
        self._parse_database_subsection()
        kv = self._parse_key_value_pairs()

        return kv

    def _parse_header(self):
        magic_string = self._read_bytes(len(self.MAGIC_STRING))
        if magic_string != self.MAGIC_STRING:
            raise RDBProtocolError(
                f"Expected magic string to be {self.MAGIC_STRING}, got {magic_string}"
            )
        version = self._read_bytes(self.VERSION_SIZE)
        try:
            int(version)
        except ValueError:
            raise ValueError("Invalid RDB file: wrong version format")

    def _parse_metadata(self):
        metadata = {}

        while True:
            type_byte = self._read_byte()
            if type_byte != self.METADATA_FIELD_MARKER:
                # Go back one position - we still want to read this down the line
                self.pos -= 1
                break

            key = self._parse_string()
            value = self._parse_string()
            metadata[key] = value

        return metadata

    def _parse_database_subsection(self):
        database_start_marker = self._read_byte()
        if database_start_marker != self.DATABASE_START_MARKER:
            raise RDBProtocolError(
                f"Expected to find database start marker, found: {hex(database_start_marker)}"
            )

        _database_index = self._read_byte()
        hash_table_marker = self._read_byte()
        if hash_table_marker != self.HASH_TABLE_MARKER:
            raise RDBProtocolError(
                f"Expected to find hash table marker, found: {hex(hash_table_marker)}"
            )
        _kv_hash_table_size = self._read_byte()
        _expiry_hash_table_size = self._read_byte()

    def _parse_key_value_pairs(self):
        result = {}

        while True:
            type_byte = self._read_byte()
            if type_byte == self.EOF_MARKER:
                break

            expiry = self._parse_expiry(type_byte)
            if expiry:
                # If there was expiry data, the byte that describes
                # the data type is the next byte
                type_byte = self._read_byte()

            key = self._parse_string()
            value = self._parse_string()
            result[key] = Container(value=value, expiry=expiry)

        return result

    def _parse_expiry(self, type_byte: int) -> float | None:
        # https://rdb.fnordig.de/file_format.html#key-expiry-timestamp
        expiration_data = None
        if type_byte == self.EXPIRY_IN_SECONDS_MARKER:
            expiration_data = self._read_bytes(4)
        elif type_byte == self.EXPIRY_IN_MILLISECONDS_MARKER:
            expiration_data = self._read_bytes(8)

        if expiration_data:
            return int.from_bytes(expiration_data, byteorder="little") / 1e3

    def _parse_string(self):
        # https://rdb.fnordig.de/file_format.html#string-encoding
        # See "Length Prefixed String"
        length_type = self._read_byte()
        length, data_type = self._parse_length(length_type)
        data = self._read_bytes(length)

        match data_type:
            case builtins.int:
                return int.from_bytes(data)
            case builtins.str:
                return data.decode()
            case _:
                raise RDBProtocolError("Unexpected data type when parsing string")

    def _parse_length(self, byte: int) -> tuple[int, str | int]:
        # https://rdb.fnordig.de/file_format.html#length-encoding

        top_2_bits = (byte & 0b11000000) >> 6
        if top_2_bits == 0b00:
            # See "Length Encoding" - the 6 LSBs represent the length
            return byte & 0b00111111, str
        elif top_2_bits == 0b11:
            # https://rdb.fnordig.de/file_format.html#string-encoding
            # See "Integers as String" - the 6 LSBs will determine the length
            lower_six_bits = byte & 0b00111111
            if lower_six_bits == 0b00:
                # An 8-bit integer follows
                return 1, int
            elif lower_six_bits == 0b01:
                # A 16-bit integer follows
                return 2, int
            elif lower_six_bits == 0b10:
                # A 32-bit integer follows
                return 4, int
            else:
                raise RDBProtocolError(
                    f"Unexpected lower six bits for length type byte: {bin(lower_six_bits)}"
                )
        else:
            raise RDBProtocolError(f"Malformed length type byte: {byte}")

    def _read_bytes(self, n: int) -> bytes:
        data = self.buffer[self.pos : self.pos + n]
        self.pos += n
        return data

    def _read_byte(self) -> int:
        return self._read_bytes(1)[0]
