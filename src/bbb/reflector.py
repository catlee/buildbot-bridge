import asyncio
import logging

import arrow

import bbb.db as db
import bbb.taskcluster as tc

_reflected_tasks = dict()
RECLAIM_THRESHOLD = 10 * 60
log = logging.getLogger(__name__)


class ReflectedTask:
    def __init__(self, bbb_task):
        self.bbb_task = bbb_task
        self.future = None

    def start(self):
        log.info("Start watching task: %s run: %s", self.bbb_task.taskId,
                 self.bbb_task.runId)
        loop = asyncio.get_event_loop()
        self.future = loop.create_task(self.reclaim_loop())

    def cancel(self):
        log.info("Stop watching task: %s run: %s", self.bbb_task.taskId,
                 self.bbb_task.runId)
        self.future.cancel()

    async def reclaim_loop(self):
        while True:
            now = arrow.now().timestamp
            taken_until = arrow.get(self.bbb_task.takenUntil).timestamp
            reclaim_at = taken_until - RECLAIM_THRESHOLD
            if now >= reclaim_at:
                log.info("Reclaim task: %s run:%s ", self.bbb_task.taskId,
                         self.bbb_task.runId)
                res = await tc.reclaim_task(
                    self.bbb_task.taskId,
                    int(self.bbb_task.runId),
                    self.bbb_task.buildrequestId)
                if res:
                    self.bbb_task.takenUntil = res["takenUntil"]
                    log.info("Update takenUntil of task: %s run:%s to %s",
                             self.bbb_task.taskId, self.bbb_task.runId,
                             res["takenUntil"])
                    await db.update_taken_until(
                        self.bbb_task.buildrequestId,
                        arrow.get(res["takenUntil"]).timestamp)
            else:
                snooze = max([reclaim_at - now, 0])
                log.info("Will reclaim task: %s run:%s in %s seconds",
                         self.bbb_task.taskId, self.bbb_task.runId, snooze)
                await asyncio.sleep(snooze)


async def main_loop():
    log.info("Fetching BBB tasks...")
    all_bbb_tasks = await db.fetch_all_tasks()
    log.info("Total BBB tasks: %s", len(all_bbb_tasks))

    inactive_bbb_tasks = [t for t in all_bbb_tasks if t.takenUntil is None]
    log.info("BBB tasks without takenUntil set: %s", len(inactive_bbb_tasks))

    actionable_bbb_tasks = [t for t in all_bbb_tasks
                            if t.takenUntil is not None]
    log.info("BBB tasks with takenUntil set: %s", len(actionable_bbb_tasks))

    actionable_request_ids = [bbb_task.buildrequestId for bbb_task in
                              actionable_bbb_tasks]
    new_bbb_tasks = [bbb_task for bbb_task in actionable_bbb_tasks if
                     bbb_task.buildrequestId not in _reflected_tasks.keys()]
    log.info("New BBB tasks: %s", len(new_bbb_tasks))
    finished_tasks = [rt for brid, rt in _reflected_tasks.items() if
                      brid not in actionable_request_ids]
    log.info("Finished BBB tasks: %s", len(finished_tasks))

    # Dry run only for now.
    return

    refresh_reflected_tasks(actionable_bbb_tasks)

    await asyncio.wait([
        add_new_tasks(new_bbb_tasks),
        remove_finished_tasks(finished_tasks),
        process_inactive_tasks(inactive_bbb_tasks),
    ])


def refresh_reflected_tasks(bbb_tasks):
    """Refresh in-memory data
    Assuming that we can run multiple instances of the reflector, update the
    in-memory copies of the BBB tasks to avoid extra DB and TC calls to fetch
    latest values, e.g. takenUntil.
    """
    for bbb_task in bbb_tasks:
        if bbb_task.buildrequestId in _reflected_tasks.keys():
            _reflected_tasks[bbb_task.buildrequestId].bbb_task = bbb_task


async def add_new_tasks(new_bbb_tasks):
    """Start reflecting a task"""
    for bbb_task in new_bbb_tasks:
        rt = ReflectedTask(bbb_task)
        _reflected_tasks[bbb_task.buildrequestId] = rt
        rt.start()


async def remove_finished_tasks(finished__reflected_tasks):
    """Stop reflecting finished tasks

    After the BBListener removes a task from the database, the reflector
    stops reclaiming the task.
    """
    async def cancel(reflected_task):
        reflected_task.cancel()
        del _reflected_tasks[reflected_task.bbb_task.buildrequestId]

    await asyncio.wait([cancel(rt) for rt in finished__reflected_tasks])


async def process_inactive_tasks(tasks):
    """Process tasks with no `takenUntil` set.

    Cancel tasks when a buildrequest has been cancelled before starting.
    Cancelling tasks that were running is handled by the BB listener.
    """

    async def delete_task(task_id, request_id):
        log.info("Cancelling task: %s", task_id)
        await tc.cancel_task(task_id)
        log.info("Removing from DB, task: %s, requestid: %s", task_id,
                 request_id)
        await db.delete_task_by_request_id(request_id)

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
