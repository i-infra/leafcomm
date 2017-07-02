import sys
sys.path.append('./')
import asyncio
import asyncio_redis
import phase1
import iq 

async def main():
    try:
        reader, writer = await asyncio.open_connection(sys.argv[1], int(sys.argv[2]))
    except:
        writer = None
    conn = await asyncio_redis.Connection.create('localhost', 6379, encoder=phase1.CborEncoder())
    failed = await conn.smembers_asset('trivial_timestamps')
    byte_count = 0
    infos = await conn.mget_aslist(sorted(failed))
    for info in sorted([x for x in zip(sorted(failed),infos) if x[1] is not None and 'size' in x[1].keys()], key=lambda x: x[1]['size']):
        info = info[1]
        ba = phase1.decompress(**info)
        iq.fix_iq_imbalance(ba)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
