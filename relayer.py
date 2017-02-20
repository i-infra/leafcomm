import asyncio
import beepshrink
import phase1
import cbor

import sys

async def main():
    reader, writer = await asyncio.open_connection(sys.argv[1], int(sys.argv[2]))
    conn = await phase1.get_connection()
    failed = await conn.smembers('nontrivial_timestamps')
    for ts in sorted([await x for x in failed]):
        print(ts)
        info = await conn.get(ts) 
        ba = beepshrink.decompress(info['size'], info['dtype'], info['data'])*255
        if ba is not None:
            writer.write(ba.tobytes())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
