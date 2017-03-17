from bbb.reflector import Reflector

from pytest import fixture

@fixture
def r():
    return Reflector()

def test_reclaim_running(r):
    r.reclaim_running_tasks()

def test_cancel_pending(r):
    r.cancel_pending()

def test_refresh_active_tasks(r):
    r.refresh_active_tasks()
