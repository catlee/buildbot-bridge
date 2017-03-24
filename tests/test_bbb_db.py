import bbb.db
import pytest
from .dbutils import create_dbs
import sqlalchemy as sa


@pytest.mark.asyncio
async def test_init():
    await bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    assert bbb.db._bbb_tasks is not None
    assert bbb.db._bb_requests is not None
    assert bbb.db._bb_conn  is not None
    assert bbb.db._bbb_conn is not None


@pytest.mark.asyncio
async def test_delete_task_by_request_id():
    await bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db.delete_task_by_request_id(1)


@pytest.mark.asyncio
async def test_fetch_all_tasks():
    await bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db.fetch_all_tasks()


@pytest.mark.asyncio
async def test_get_cancelled_build_requests():
    await bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db.get_cancelled_build_requests([1, 2])


@pytest.mark.asyncio
async def test_update_taken_until():
    await bbb.db.init("sqlite:///:memory:", "sqlite:///:memory:")
    await create_dbs()
    await bbb.db._bbb_conn.execute(
        bbb.db._bbb_tasks.insert().values(
            buildrequestId=1,
            taskId="xx",
            runId=0,
            createdDate=12,
            processedDate=17,
            takenUntil=200,
    ))
    await bbb.db.update_taken_until(1, 1)
    res = await bbb.db._bbb_conn.execute(
        sa.select([bbb.db._bbb_tasks.c.takenUntil]).where(
            bbb.db._bbb_tasks.c.buildrequestId == 1
        )
    )
    records = await res.fetchall()
    assert records[0][0] == 1
