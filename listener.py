
import argparse
import asyncio
import logging
import sys
import uuid
from utils import Reader, Writer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("listener")

async def main(args):
    my_id = uuid.uuid4().hex[:8]
    reader, writer = await asyncio.open_connection(
        args.host, args.port
    )
    reader = Reader(reader)
    writer = Writer(writer)
    sockname = writer.sockname

    logger.info(f"Started {my_id} at {sockname}.")

    channel = args.listen
    await writer.write(channel)
    try:
        while data:= await reader.read():
            logger.info(f"Received by {my_id} data: {data[:20]}.")
        logger.info("Connection ended.")
    except asyncio.IncompleteReadError:
        logger.info("Server has been closed.")
    finally:
        await writer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=25000, type=int)
    parser.add_argument("--listen", default="/topic/foo")
    try:
        asyncio.run(main(parser.parse_args()))
    except KeyboardInterrupt:
        logger.info("Listener shutting down, bye!")