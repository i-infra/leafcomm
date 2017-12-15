import sys
sys.path.append('./') 

import asyncio
import asyncio_redis
import node_core
import cbor
import base64

async def main():
    conn = await asyncio_redis.Connection.create('localhost', 6379, encoder=node_core.CborEncoder())
    keys = sys.argv[1::]
    if len(keys) == 0:
        keys = [await x for x in await conn.smembers('nontrivial_timestamps')]
    else:
        keys = [cbor.loads(base64.b64decode(arg)) for arg in keys]
    print(keys)
    for key in keys:
        info = await conn.get(key)
        if (info != None):
            f = open(str(key)+'.beep', 'wb')
            f.write(cbor.dumps(info))
            f.flush()
    conn.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
