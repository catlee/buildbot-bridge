from pytest import fixture

from bbb.reflector import Reflector


@fixture
def r():
    return Reflector()


def test_add_new_tasks(r):
    r.add_new_tasks([])
