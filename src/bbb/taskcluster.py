import logging

from taskcluster.async import Queue
from taskcluster.async import TaskclusterRestFailure

import bbb.db as db
import bbb.selfserve as selfserve

_queue = None
log = logging.getLogger(__name__)


def init(options):
    global _queue
    _queue = Queue(options=options)


async def reclaim_task(task_id, run_id, request_id):

    try:
        return await _queue.reclaimTask(task_id, run_id)

    except TaskclusterRestFailure as e:
        status_code = e.superExc.response.status_code

        if status_code == 404:
            # Expired tasks are removed from the TC DB
            log.warning("task %s: run %s: Cannot find task in TC, removing it",
                        task_id, run_id)
            await db.delete_task_by_request_id(request_id)

        elif status_code == 403:
            # Bug 1270785. Claiming a completed task returns 403.
            log.warning("task %s: run %s: Cannot modify task in TC, removing",
                        task_id, run_id)
            await db.delete_task_by_request_id(request_id)

        elif status_code == 409:
            job_complete = await db.get_build_request(request_id)['complete']
            if job_complete:
                log.info("task %s: run %s: buildrequest %s: got 409 when "
                         "reclaiming task; assuming task is complete",
                         task_id, run_id, request_id)
            else:
                log.warning(
                    "task %s: run %s: deadline exceeded; cancelling it",
                    task_id, run_id)
                branch = await db.get_branch(request_id)
                build_id = await db.get_build_id(request_id)
                try:
                    await selfserve.cancel_build(branch, build_id)
                    # delete from the DB only if all cancel requests pass
                    await db.delete_task_by_request_id(request_id)

                except:  # TODO: RequestException:
                    log.exception("task %s: run %s: buildrequest %s: failed "
                                  "to cancel task", task_id, run_id,
                                  request_id)

        else:
            log.warning("task %s: run %s: Unhandled TC status code %s",
                        task_id, run_id, status_code)


async def cancel_task(task_id):
    try:
        await _queue.cancelTask(task_id)
    except TaskclusterRestFailure as e:
        log.error("task %s:  status_code: %s body: %s", task_id, e.status_code,
                  e.body)
