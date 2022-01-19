import asyncio
import logging
from codecs import StreamReader, StreamWriter
from collections import defaultdict, deque
import sys
from typing import DefaultDict, Deque

from utils import Reader, Writer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("server")

SUBSCRIBERS: DefaultDict[str, Deque] = defaultdict(deque)


async def on_client_connect(stream_reader: StreamReader, stream_writer: StreamWriter):
    reader = Reader(stream_reader)
    writer = Writer(stream_writer)

    channel_to_subscribe = await reader.read()
    SUBSCRIBERS[channel_to_subscribe].append(writer)
    logger.info(f"Remote {writer.peername} subscribed to {channel_to_subscribe}.")

    try:
        while channel_name := await reader.read():
            data = await reader.read()
            connections = SUBSCRIBERS[channel_name]
            if connections and channel_name.startswith("/queue"):
                connections.rotate()
                connections = [connections[0]]
            logger.info(f"Sending to {channel_name}: {data[:20]}.")
            await asyncio.gather(*[w.write(data) for w in connections])
    except asyncio.CancelledError:
        logger.info(f"Remote {writer.peername} closing connection.")
        await writer.close()
    except asyncio.IncompleteReadError:
        logger.log(f"Remote {writer.peername} disconnected.")
    finally:
        logger.info(f"Remote {writer.peername} closed.")
        SUBSCRIBERS[channel_to_subscribe].remove(writer)


async def main(*args, **kwargs):
    logger.info(f"Starting the server.")
    server = await asyncio.start_server(*args, **kwargs)
    async with server:
        await server.serve_forever()


try:
    asyncio.run(main(on_client_connect, host="127.0.0.1", port=25000))
except KeyboardInterrupt:
    logger.info("Shutting down server.")
