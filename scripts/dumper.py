import sys
sys.path.append('./') 

import asyncio
import phase1
import cbor
import base64


# accepts one argument, the ascii base64 representation of the cbor-encoded f64 timestamp
# this is awkward, but unambiguous and space-efficient. you can at least type it, don't complain.

async def main():
    for arg in sys.argv[1::]:
        key = cbor.loads(base64.b64decode(arg))
        conn = await phase1.get_connection()
        info = await conn.get(key)
        f = open(str(key)+'.beep', 'wb')
        f.write(cbor.dumps(info))
        f.flush()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
