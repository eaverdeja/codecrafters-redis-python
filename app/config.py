import asyncio
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RDBConfig:
    directory: str
    filename: str

    @property
    def file_path(self) -> Path:
        return Path(self.directory) / self.filename


@dataclass
class ReplicaConfig:
    port: int
    connection: asyncio.StreamWriter
    capabilities: set
    offset: int


@dataclass
class ServerInfo:
    role: str
    master_replid: str | None = None
    master_repl_offset: str | None = None
