from asyncio import StreamReader, StreamWriter


class Reader:
    def __init__(self, stream_reader: StreamReader, *args, **kwargs) -> None:
        self.stream_reader = stream_reader
        super().__init__(*args, **kwargs)

    async def read_bytes(self) -> bytes:
        length_bytes = await self.stream_reader.readexactly(4)
        size = int.from_bytes(length_bytes, byteorder='big')
        print(size)
        data = await self.stream_reader.readexactly(size)
        return data

    async def read(self) -> str:
        data = await self.read_bytes()
        return str(data)


class Writer:
    def __init__(self, stream_writer: StreamWriter, *args, **kwargs) -> None:
        self.stream_writer = stream_writer
        super().__init__(*args, **kwargs)

    async def write_bytes(self, data: bytes) -> None:
        size = len(bytes)
        size_bytes = size.to_bytes(4, byteorder="big")
        self.stream_writer.writelines([size_bytes, data])
        await self.stream_writer.drain()

    async def write(self, data: str) -> None:
        data_bytes = data.encode()
        self.write_bytes(data_bytes)

    async def close(self) -> None:
        self.stream_writer.close()
        await self.stream_writer.wait_closed()
