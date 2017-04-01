from concurrent.futures import CancelledError

from bbb.reflector import ReflectedTask

import pytest
from unittest import mock

class AsyncMock(mock.MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


class BBB_Task:
    def __init__(self, taskId=None, runId=None, takenUntil=0, buildrequestId=0):
        self.taskId = taskId
        self.runId = runId
        self.takenUntil = takenUntil
        self.buildrequestId = buildrequestId


@pytest.mark.asyncio
async def test_start_and_cancel_task(event_loop):
    bbb_task = BBB_Task()

    t = ReflectedTask(bbb_task)
    t.loop = event_loop
    t.start()

    assert t.future
    assert not t.future.cancelled()
    assert not t.future.done()

    with pytest.raises(CancelledError), mock.patch('bbb.taskcluster.reclaim_task'):
        t.cancel()
        await t.future
    assert t.future.cancelled()
    assert t.future.done()


def test_reclaim_at():
    bbb_task = BBB_Task(takenUntil='2017-03-31 22:00:00Z')
    t = ReflectedTask(bbb_task)
    assert t.reclaim_at == 1490997300


def test_should_reclaim():
    bbb_task = BBB_Task(takenUntil='2017-03-31 22:00:00Z')
    t = ReflectedTask(bbb_task)
    with mock.patch('arrow.now') as now:
        now.return_value.timestamp = 1490997300
        assert t.should_reclaim

        now.return_value.timestamp = 1490997300 - 1
        assert not t.should_reclaim

def test_snooze_time():
    bbb_task = BBB_Task(takenUntil='2017-03-31 22:00:00Z')
    t = ReflectedTask(bbb_task)
    assert t.reclaim_at == 1490997300

    with mock.patch('arrow.now') as now:
        now.return_value.timestamp = 1490997300
        assert t.snooze_time == 0

        now.return_value.timestamp = 1490997300 - 10
        assert t.snooze_time == 10

        now.return_value.timestamp = 1490997300 + 10
        assert t.snooze_time == 0

@pytest.mark.asyncio
async def test_snooze():
    bbb_task = BBB_Task(takenUntil='2017-03-31 22:00:00Z')
    t = ReflectedTask(bbb_task)
    with mock.patch('arrow.now') as now, mock.patch('asyncio.sleep', new_callable=AsyncMock) as sleep:
        now.return_value.timestamp = 1490997300 - 10
        assert t.snooze_time == 10

        await t.snooze()
        assert sleep.called_with(10)
