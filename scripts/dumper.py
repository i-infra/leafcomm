import sys
sys.path.append('./') 

import asyncio
import asyncio_redis
import node_core
import cbor
import base64


# accepts one argument, the ascii base64 representation of the cbor-encoded f64 timestamp
# this is awkward, but unambiguous and space-efficient. you can at least type it, don't complain.

async def main():
    conn = await asyncio_redis.Connection.create('localhost', 6379, encoder=node_core.CborEncoder())
    for arg in sys.argv[1::]:
        key = cbor.loads(base64.b64decode(arg))
        info = await conn.get(key)
        f = open(str(key)+'.beep', 'wb')
        f.write(cbor.dumps(info))
        f.flush()
    conn.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
