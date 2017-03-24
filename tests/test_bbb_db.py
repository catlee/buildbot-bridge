import bbb.db
import pytest
from .dbutils import create_dbs
import sqlalchemy as sa


def test_init():
    bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    assert bbb.db._bbb_tasks is not None
    assert bbb.db._bb_requests is not None
    assert bbb.db._bb_db  is not None
    assert bbb.db._bbb_db is not None


@pytest.mark.asyncio
async def test_delete_task_by_request_id():
    bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db._bbb_db.execute(
        bbb.db._bbb_tasks.insert().values(
            buildrequestId=1,
            taskId="a",
            runId=0,
            createdDate=12,
            processedDate=17,
            takenUntil=200))
    await bbb.db._bbb_db.execute(
        bbb.db._bbb_tasks.insert().values(
            buildrequestId=2,
            taskId="ab",
            runId=0,
            createdDate=13,
            processedDate=14,
            takenUntil=250))
    await bbb.db.delete_task_by_request_id(1)
    tasks = await bbb.db.fetch_all_tasks()
    assert tasks == [(2, 'ab', 0, 13, 14, 250)]


@pytest.mark.asyncio
async def test_fetch_all_tasks():
    bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db._bbb_db.execute(
        bbb.db._bbb_tasks.insert().values(
            buildrequestId=1,
            taskId="a",
            runId=0,
            createdDate=12,
            processedDate=17,
            takenUntil=200))
    await bbb.db._bbb_db.execute(
        bbb.db._bbb_tasks.insert().values(
            buildrequestId=2,
            taskId="ab",
            runId=0,
            createdDate=13,
            processedDate=14,
            takenUntil=250))
    tasks = await bbb.db.fetch_all_tasks()
    assert tasks == [(2, 'ab', 0, 13, 14, 250), (1, 'a', 0, 12, 17, 200)]


@pytest.mark.asyncio
async def test_get_cancelled_build_requests():
    bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db._bb_db.execute(
        bbb.db._bb_requests.insert().values(
            id=1,
            complete=1,
            claimed_at=0))
    await bbb.db._bb_db.execute(
        bbb.db._bb_requests.insert().values(
            id=2,
            complete=1,
            claimed_at=5))
    await bbb.db._bb_db.execute(
        bbb.db._bb_requests.insert().values(
            id=3,
            complete=0,
            claimed_at=0))
    await bbb.db._bb_db.execute(
        bbb.db._bb_requests.insert().values(
            id=4,
            complete=1,
            claimed_at=6))
    req = await bbb.db.get_cancelled_build_requests([1, 2])
    assert req == [1]


@pytest.mark.asyncio
async def test_update_taken_until():
    bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db._bbb_db.execute(
        bbb.db._bbb_tasks.insert().values(
            buildrequestId=1,
            taskId="xx",
            runId=0,
            createdDate=12,
            processedDate=17,
            takenUntil=200))
    await bbb.db.update_taken_until(1, 1)
    res = await bbb.db._bbb_db.execute(
        sa.select([bbb.db._bbb_tasks.c.takenUntil]).where(
            bbb.db._bbb_tasks.c.buildrequestId == 1
        )
    )
    records = await res.fetchall()
    assert records[0][0] == 1
