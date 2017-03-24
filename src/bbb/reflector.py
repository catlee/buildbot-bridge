import arrow
import asyncio
import bbb.db as db
from taskcluster.async import Queue


def parse_date(datestring):
    """Parses ISO 8601 date string and returns a unix epoch time"""
    return arrow.get(datestring).timestamp


class AsyncTask:
    def __init__(self, task, tc_queue):
        self.task = task
        self.tc_queue = tc_queue
        self.aio_task = None

    def start(self):
        loop = asyncio.get_event_loop()
        self.aio_task = loop.create_task(self.reclaim_loop())

    def cancel(self):
        self.aio_task.cancel()

    async def reclaim_loop(self):
        # TODO: exception handling:
        # ignore CancelledError
        # retry reclaims unless it's 409
        # retry db operations
        while True:
            reclaim_threshold = 600
            now = arrow.now().timestamp
            next_tick = parse_date(self.task.takenUntil) - reclaim_threshold - now
            delay = min([next_tick, 0])
            asyncio.sleep(delay)
            res = await self.tc_queue.reclaimTask(self.task.taskId,
                                                  int(self.task.runId))
            self.task.takenUntil = res["takenUntil"]
            await db.update_taken_until(self.task.buildrequestId,
                                        parse_date(res["takenUntil"]))


class Reflector:
    """The Reflector is responsible for two things:
        1) Cancelling tasks when a buildrequest has been cancelled before starting
           Cancelling tasks that were running is handled by the BB listener
        2) Reclaiming active tasks
    """
    def __init__(self, tc_config):
        # Mapping of task id to a kind of future handle that is in charge of
        # reclaiming this task
        self.active_tasks = []
        self.tc_queue = Queue(tc_config)

    async def get_cancelled_build_requests(self, build_request_ids):
        return await db.get_cancelled_build_requests(build_request_ids)

    async def delete_task(self, task_id, request_id):
        await self.tc_queue.cancelTask(task_id)
        await db.delete_task_by_request_id(request_id)

    async def process_inactive_tasks(self, tasks):
        """Process tasks with no `takenUntil` set."""
        request_ids = [t.buildrequestId for t in tasks]
        cancelled = await db.get_cancelled_build_requests(request_ids)
        cancelled_tasks = [t for t in tasks if t.buildRequestId in cancelled]
        await asyncio.wait(
            [self.delete_task(task_id=t.taskId, request_id=t.buildRequestId)
             for t in cancelled_tasks])

    async def main_loop(self):
        all_tasks = await db.fetch_all_tasks()
        inactive_tasks = [t for t in all_tasks if t.takenUntil is None]
        actionable = [t for t in all_tasks if t.takenUntil is not None]
        finished_tasks = [t for t in self.active_tasks if t not in actionable]
        new_tasks = [t for t in actionable if t not in self.active_tasks]
        await asyncio.wait([
            self.process_inactive_tasks(inactive_tasks),
            self.remove_finished_tasks(finished_tasks),
            self.add_new_tasks(new_tasks)
        ])

    async def remove_finished_tasks(self, finished_tasks):
        async def cancel(t):
            t.cancel()
            self.active_tasks.remove(t)

        await asyncio.wait([cancel(t) for t in finished_tasks])

    async def add_new_tasks(self, new_tasks):
        for t in new_tasks:
            task = AsyncTask(t, self.tc_queue)
            task.start()
            self.active_tasks.append(task)
