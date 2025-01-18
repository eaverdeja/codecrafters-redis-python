import asyncio
import argparse

from .server import RedisServer
from .config import RDBConfig
from .datastore import Datastore


def main():
    parser = argparse.ArgumentParser(description="Simple Redis server")
    parser.add_argument("--dir", type=str, help="The directory of the RDB file")
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    parser.add_argument(
        "--port", type=int, help="The port to run on. Defaults to 6379", default=6379
    )
    parser.add_argument(
        "--replicaof",
        type=str,
        help="Master host and port information, used for replicas",
    )
    args = parser.parse_args()

    server = RedisServer(
        port=args.port,
        replica_of=args.replicaof,
        datastore=Datastore(),
        rdb_config=RDBConfig(directory=args.dir, filename=args.dbfilename),
    )

    try:
        asyncio.run(server.execute())
    except KeyboardInterrupt:
        print("Server stopped")


if __name__ == "__main__":
    main()
