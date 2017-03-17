import asyncio


class Reflector:
    """The Reflector is responsible for two things:
        1) Cancelling tasks when a buildrequest has been cancelled before starting
           Cancelling tasks that were running is handled by the BB listener
        2) Reclaiming active tasks
    """
    def __init__(self):
        # Mapping of task id to a kind of future handle that is in charge of
        # reclaiming this task
        self.active_tasks = {}

    def reclaim_running_tasks(self):
        pass

    def refresh_active_tasks(self):
        pass

    def cancel_pending(self):
        pass

    async def get_cancelled_build_requests(self, build_request_ids):
        q = self.bb_db.select([self.buildrequests_table.c.id]).where(
            self.buildrequests_table.c.id in build_request_ids).where(
                self.buildrequests_table.c.complete == 1).where(
                self.buildrequests_table.c.claimed_at == 0)
        return [r[0] for r in q.fetchall()]

    async def schedule_cancelTask(self, task_id, build_request_id):
        # TODO: retry/handle exceptions
        await self.tc_queue.cancelTask(task_id)
        # TODO: retry/handle exceptions
        await self.bbb_db.delete_build_request(build_request_id)

    async def handle_no_takenUntil(self, tasks):
        """Process tasks with no `takenUntil` set."""
        # 1. get all tasks without takenUntil (passed)
        # 2. fetch the corresponding data for them from buildbot
        # 3. find cancelled jobs
        # 4. schedule tc_queue.cancelTask()
        # 5. Do nothing for the rest (pending)
        build_request_ids = [t["buildrequestId"] for t in tasks]
        cancelled = await self.get_cancelled_build_requests(build_request_ids)
        cancelled_tasks = [t for t in tasks
                           if t["buildRequestId"] in cancelled]
        await asyncio.wait(
            [self.schedule_cancelTask(task_id=t["taskId"],
                                      build_request_id=t["buildRequestId"])
             for t in cancelled_tasks])
