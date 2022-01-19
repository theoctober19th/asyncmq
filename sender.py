import argparse
import asyncio
from email.policy import default
from itertools import count
import uuid
from utils import Reader, Writer
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("sender")


async def main(args):
    my_id = uuid.uuid4().hex[:8]
    reader, writer = await asyncio.open_connection(
        host=args.host, port=args.port
    )
    reader = Reader(reader)
    writer = Writer(writer)
    logger.info(f"Started {my_id} at {writer.sockname}")

    channel_to_subscribe = "/null"
    await writer.write(channel_to_subscribe)

    try:
        for i in count():
            await asyncio.sleep(args.interval)
            data = "X"*args.size or f"Msg {i} from {my_id}"
            try:
                await writer.write(args.channel)
                await writer.write(data)
                logger.info(f"Sent data: {data[:20]} to channel {args.channel}")
            except OSError:
                logger.info("Connection ended.")
                break
    except asyncio.CancelledError:
        await writer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=25000, type=int)
    parser.add_argument("--channel", default="/topic/foo")
    parser.add_argument("--interval", default=1, type=float)
    parser.add_argument("--size", default=0, type=int)
    try:
        asyncio.run(main(parser.parse_args()))
    except KeyboardInterrupt:
        logger.info("Sender shutting down, bye!")