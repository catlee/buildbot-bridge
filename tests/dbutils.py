import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
import bbb.db


async def make_bbb_db():
    await bbb.db._bbb_conn.execute(CreateTable(bbb.db._bbb_tasks))


async def make_bb_db():
    await bbb.db._bb_conn.execute(CreateTable(bbb.db._bb_requests))


async def create_dbs():
    await make_bb_db()
    await make_bbb_db()
