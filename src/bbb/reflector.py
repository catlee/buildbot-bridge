import asyncio
import arrow
import logging

import bbb.db as db
import bbb.taskcluster as tc

log = logging.getLogger(__name__)


def parse_date(datestring):
    """Parses ISO 8601 date string and returns a unix epoch time"""
    return arrow.get(datestring).timestamp


class AsyncTask:
    def __init__(self, task):
        self.task = task
        self.aio_task = None

    def start(self):
        log.info("Start watching task: %s run: %s", self.task.taskId,
                 self.task.runId)
        loop = asyncio.get_event_loop()
        self.aio_task = loop.create_task(self.reclaim_loop())

    def cancel(self):
        log.info("Stop watching task: %s run: %s", self.task.taskId,
                 self.task.runId)
        self.aio_task.cancel()

    async def reclaim_loop(self):
        # TODO: exception handling:
        # no need to handle CancelledError
        # retry reclaims unless it's 409
        # retry db operations
        while True:
            reclaim_threshold = 600
            now = arrow.now().timestamp
            next_tick = parse_date(self.task.takenUntil) - reclaim_threshold - now
            delay = max([next_tick, 0])
            log.info("Will reclaim task: %s run:%s in %s seconds",
                     self.task.taskId, self.task.runId, delay)
            await asyncio.sleep(delay)
            log.info("Reclaim task: %s run:%s ", self.task.taskId,
                     self.task.runId)
            res = await tc.reclaim_task(self.task.taskId, int(self.task.runId))
            self.task.takenUntil = res["takenUntil"]
            log.info("Update takenUntil of task: %s run:%s to %s",
                     self.task.taskId, self.task.runId, res["takenUntil"])
            await db.update_taken_until(self.task.buildrequestId,
                                        parse_date(res["takenUntil"]))


async def delete_task(task_id, request_id):
    log.info("Cancelling task: %s", task_id)
    await tc.cancel_task(task_id)
    log.info("Removing from DB, task: %s, requestid: %s", task_id, request_id)
    await db.delete_task_by_request_id(request_id)


async def process_inactive_tasks(tasks):
    """Process tasks with no `takenUntil` set."""
    request_ids = [t.buildrequestId for t in tasks]
    cancelled = await db.get_cancelled_build_requests(request_ids)
    cancelled_tasks = [t for t in tasks if t.buildRequestId in cancelled]
    if cancelled_tasks:
        log.info("Found cancelled-before-started tasks: %s",
                 ", ".join(t.taskId for t in cancelled_tasks))
        await asyncio.wait([
            delete_task(task_id=t.taskId, request_id=t.buildRequestId)
            for t in cancelled_tasks
        ])


class Reflector:
    """The Reflector is responsible for two things:
        1) Cancelling tasks when a buildrequest has been cancelled before starting
           Cancelling tasks that were running is handled by the BB listener
        2) Reclaiming active tasks
    """
    def __init__(self):
        # Mapping of task id to a kind of future handle that is in charge of
        # reclaiming this task
        self.active_tasks = []

    async def main_loop(self):
        all_tasks = await db.fetch_all_tasks()
        inactive_tasks = [t for t in all_tasks if t.takenUntil is None]
        actionable = [t for t in all_tasks if t.takenUntil is not None]
        finished_tasks = [t for t in self.active_tasks if t not in actionable]
        new_tasks = [t for t in actionable if t not in self.active_tasks]
        await asyncio.wait([
            process_inactive_tasks(inactive_tasks),
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
            task = AsyncTask(t)
            task.start()
            self.active_tasks.append(task)
