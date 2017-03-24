from sqlalchemy_aio import ASYNCIO_STRATEGY
import sqlalchemy as sa

_bbb_tasks = None
_bb_requests = None
_bb_conn = None
_bbb_conn = None

async def init(bridge_uri, buildbot_uri):
    global _bbb_tasks, _bb_requests, _bb_conn, _bbb_conn
    _bbb_db = _get_engine(bridge_uri)
    _bb_db = _get_engine(buildbot_uri)
    _bbb_tasks = _get_bbb_tasks_table(_bbb_db)
    _bb_requests = _get_bb_requests_table(_bb_db)
    _bb_conn = await _bb_db.connect()
    _bbb_conn = await _bbb_db.connect()


async def delete_task_by_request_id(id_):
    await _bbb_conn.execute(_bbb_tasks.delete(_bbb_tasks.c.buildrequestId == id_))


async def fetch_all_tasks():
    return await _bbb_conn.execute(_bbb_tasks.select().order_by(
        _bbb_tasks.c.takenUntil.desc()))


async def get_cancelled_build_requests(build_request_ids):
    q = _bb_requests.select(_bb_requests.c.id).where(
        _bb_requests.c.id.in_(build_request_ids)).where(
        _bb_requests.c.complete == 1).where(
        _bb_requests.c.claimed_at == 0)
    res = await _bb_conn.execute(q)
    records = await res.fetchall()
    return [r[0] for r in records]


async def update_taken_until(request_id, taken_until):
    await _bbb_conn.execute(
        _bbb_tasks.update(_bbb_tasks.c.buildrequestId == request_id).values(
            takenUntil=taken_until))


def _get_engine(uri):
    if "mysql" in uri:
        db = sa.create_engine(uri, pool_size=1, pool_recycle=60,
                              pool_timeout=60, strategy=ASYNCIO_STRATEGY)
    else:
        db = sa.create_engine(uri, pool_size=1, pool_recycle=60,
                              strategy=ASYNCIO_STRATEGY)
    return db


def _get_bbb_tasks_table(engine):
    return sa.Table(
        'tasks',
        sa.MetaData(engine),
        sa.Column('buildrequestId', sa.Integer, primary_key=True),
        sa.Column('taskId', sa.String(32), index=True),
        sa.Column('runId', sa.Integer),
        sa.Column('createdDate', sa.Integer,
                  doc="When the task was submitted to TC"),
        sa.Column('processedDate', sa.Integer, doc="When we put it into BB"),
        sa.Column('takenUntil', sa.Integer, index=True,
                  doc="How long until our claim needs to be renewed")
    )


def _get_bb_requests_table(engine):
    return sa.Table(
        'buildrequests',
        sa.MetaData(engine),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('buildsetid', sa.Integer),
        sa.Column('buildername', sa.String(256)),
        sa.Column('priority', sa.Integer),
        sa.Column('claimed_at', sa.Integer),
        sa.Column('claimed_by_name', sa.String(256)),
        sa.Column('claimed_by_incarnation', sa.String(256)),
        sa.Column('complete', sa.Integer),
        sa.Column('results', sa.Integer),
        sa.Column('submitted_at', sa.Integer),
        sa.Column('complete_at', sa.Integer)
    )
