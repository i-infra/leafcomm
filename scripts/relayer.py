import sys
sys.path.append('./')
import asyncio
import beepshrink
import phase1
import cbor
import base64

async def main():
    try:
        reader, writer = await asyncio.open_connection(sys.argv[1], int(sys.argv[2]))
    except:
        writer = None
    conn = await phase1.get_connection()
    failed = await conn.smembers_asset('nontrivial_timestamps')
    byte_count = 0
    def forwarder(info):
        if writer is not None:
            ba = beepshrink.decompress(**info)*255
            writer.write(ba.tobytes())
            ab = ba*0.0
            writer.write(ab.tobytes())
            return len(ab)*2
        return 0
    infos = await conn.mget_aslist(sorted(failed))
    for info in sorted([x for x in zip(sorted(failed),infos) if x[1] is not None and 'size' in x[1].keys()], key=lambda x: x[1]['size']):
        print(base64.b64encode(cbor.dumps(info[0])), info[1]['size'])
        byte_count += forwarder(info[1])
        if writer is not None:
            await writer.drain()
    print('bytes sent', byte_count)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
