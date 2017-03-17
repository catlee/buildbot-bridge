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
