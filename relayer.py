import asyncio
import beepshrink
import phase1
import cbor

import sys

async def main():
    reader, writer = await asyncio.open_connection(sys.argv[1], int(sys.argv[2]))
    conn = await phase1.get_connection()
    failed = await conn.smembers('nontrivial_timestamps')
    byte_count = 0
    infos = await conn.mget(sorted([await x for x in failed]))
    def forwarder(info):
        ba = beepshrink.decompress(info['size'], info['dtype'], info['data'])*255
        writer.write(ba.tobytes())
        ab = ba*0.0
        writer.write(ab.tobytes())
        return len(ab)*2
    for info in infos:
        info = await info
        if (info is not None) and (info is not {}):
            byte_count += forwarder(info)
            await writer.drain()
    print('bytes sent', byte_count)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
