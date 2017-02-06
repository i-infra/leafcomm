import asyncio
import beepshrink
import phase1
import cbor

import sys

async def main():
    reader, writer = await asyncio.open_connection(sys.argv[1], int(sys.argv[2]))
    subscriber = await phase1.get_dequeuer()
    while True:
        msg = await subscriber.next_published()
        info = cbor.loads(msg.value)
        ba = beepshrink.decompress(info['size'], info['dtype'], info['data'])
        if ba is not None:
            writer.write(ba.tobytes())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
