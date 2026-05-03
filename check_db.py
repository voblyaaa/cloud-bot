import aiosqlite
import asyncio

async def check():
    async with aiosqlite.connect("storage.db") as db:
        async with db.execute("SELECT * FROM folders") as cur:
            rows = await cur.fetchall()
            for r in rows:
                print(r)

asyncio.run(check())