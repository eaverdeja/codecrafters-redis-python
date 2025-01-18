from .constants import CRLF


def encode_simple_string(value: str) -> str:
    return f"+{value}{CRLF}"


def encode_bulk_string(value: str | None) -> str:
    if value is None:
        return f"$-1{CRLF}"
    return f"${len(value)}{CRLF}{value}{CRLF}"


def encode_array(values: list) -> str:
    length = len(values)
    message = "".join(values)
    return f"*{length}{CRLF}{message}"


def encode_file(bytes: bytes) -> str:
    return f"${len(bytes)}{CRLF}{str(bytes)}"
