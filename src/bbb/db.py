import asyncio
from concurrent.futures import ThreadPoolExecutor

THREADPOOL = ThreadPoolExecutor(max_workers=1)

_loop = asyncio.get_event_loop


def set_loop(loop):
    global _loop
    _loop = loop


async def run_in_thread(func):
    async def wrapped(*args, **kwargs):
        return await _loop.run_in_executor(THREADPOOL, func, *args, **kwargs)


@run_in_thread
def get_cancelled_build_requests(bb_db, build_request_ids):
    q = bb_db.select([bb_db.buildrequests_table.c.id]).where(
        bb_db.buildrequests_table.c.id in build_request_ids).where(
            bb_db.buildrequests_table.c.complete == 1).where(
            bb_db.buildrequests_table.c.claimed_at == 0)
    return [r[0] for r in q.fetchall()]
