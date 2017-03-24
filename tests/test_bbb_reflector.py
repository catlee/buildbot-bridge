from bbb.reflector import Reflector
from pytest import fixture


@fixture
def r():
    return Reflector()


def test_add_new_tasks(r):
    r.add_new_tasks([])
