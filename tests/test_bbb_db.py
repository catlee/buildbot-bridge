import bbb.db
import pytest
from .dbutils import create_dbs


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
