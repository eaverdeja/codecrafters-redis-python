from .constants import CRLF


def encode_simple_string(value: str) -> str:
    return f"+{value}{CRLF}"


def encode_bulk_string(value: str) -> str:
    if value == "-1":
        return f"$-1{CRLF}"
    return f"${len(value)}{CRLF}{value}{CRLF}"
