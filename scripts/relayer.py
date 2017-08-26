import sys
sys.path.append('./')
import asyncio
import asyncio_redis
import node_core
import cbor
import base64


async def main():
    reader, writer = await asyncio.open_connection(sys.argv[1], int(sys.argv[2]))
    conn = await asyncio_redis.Connection.create('localhost', 6379, encoder=node_core.CborEncoder())
    failed = await conn.smembers_asset('nontrivial_timestamps')
    byte_count = 0
    print('found', len(failed), 'failed sequences')
    def forwarder(info):
        if writer is not None:
            ba = node_core.decompress(**info) * 255
            writer.write(ba.tobytes())
            ab = ba * 0.0
            writer.write(ab.tobytes())
            return len(ab) * 2
        return 0
    for id_ in failed:
        info = await conn.get(id_)
        info = (id_, info)
        if info[1] is not None and 'size' in info[1].keys():
            print(base64.b64encode(cbor.dumps(info[0])), info[1]['size'])
            byte_count += forwarder(info[1])
            if writer is not None:
                await writer.drain()
    print('bytes sent', byte_count)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
