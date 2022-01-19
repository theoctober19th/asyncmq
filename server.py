import asyncio
from contextlib import suppress
import logging
from codecs import StreamReader, StreamWriter
from collections import defaultdict, deque
import sys
from typing import DefaultDict, Deque, Dict
from webbrowser import get

from utils import Reader, Writer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("server")

SUBSCRIBERS: DefaultDict[str, Deque] = defaultdict(deque)
SENDER_QUEUES: DefaultDict[Writer, asyncio.Queue] = defaultdict(asyncio.Queue)
CHANNEL_QUEUES: Dict[str, asyncio.Queue] = {}

async def send_client(writer: Writer, queue: asyncio.Queue):
    while True:
        try:
            data = await queue.get()
        except asyncio.CancelledError:
            continue

        if not data:
            break

        try:
            await writer.write(data)
        except asyncio.CancelledError:
            await writer.write(data)
    await writer.close()

async def channel_sender(channel_name: str):
    with suppress(asyncio.CancelledError):
        while True:
            writers = SUBSCRIBERS[channel_name]
            if not writers:
                await asyncio.sleep(1)
                continue
            if channel_name.startswith("/queue"):
                writers.rotate()
                writers = [writers[0]]
            if not (data := await CHANNEL_QUEUES[channel_name].get()):
                break

            for writer in writers:
                if not SENDER_QUEUES[writer].full():
                    logger.info(f"Sending to {channel_name}: {data[:20]}.")
                    await SENDER_QUEUES[writer].put(data)


async def on_client_connect(stream_reader: StreamReader, stream_writer: StreamWriter):
    reader = Reader(stream_reader)
    writer = Writer(stream_writer)

    channel_to_subscribe = await reader.read()
    SUBSCRIBERS[channel_to_subscribe].append(writer)
    sender_task = asyncio.create_task(
        send_client(writer, SENDER_QUEUES[writer])
    )
    logger.info(f"Remote {writer.peername} subscribed to {channel_to_subscribe}.")

    try:
        while channel_name := await reader.read():
            data = await reader.read()
            if channel_name not in CHANNEL_QUEUES:
                CHANNEL_QUEUES[channel_name] = asyncio.Queue(maxsize=10)
                asyncio.create_task(
                    channel_sender(channel_name)
                )
            await CHANNEL_QUEUES[channel_name].put(data)
    except asyncio.CancelledError:
        logger.info(f"Remote {writer.peername} closing connection.")
        await writer.close()
    except asyncio.IncompleteReadError:
        logger.info(f"Remote {writer.peername} disconnected.")
    finally:
        logger.info(f"Remote {writer.peername} closed.")
        await SENDER_QUEUES[writer].put(None)
        await sender_task
        del SENDER_QUEUES[writer]
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
