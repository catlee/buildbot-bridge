from taskcluster.async import Queue

_queue = None


def init(options):
    global _queue
    _queue = Queue(options=options)


async def reclaim_task(task_id, run_id):
    await _queue.reclaimTask(task_id, run_id)


async def cancel_task(task_id):
    _queue.cancelTask(task_id)

