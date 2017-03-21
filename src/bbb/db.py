from sqlalchemy_aio import ASYNCIO_STRATEGY
import sqlalchemy as sa


class BuildBotBridgeDB:
    def __init__(self, uri):
        db = sa.create_engine(uri, pool_size=1, pool_recycle=60,
                              pool_timeout=60,
                              strategy=ASYNCIO_STRATEGY)
        metadata = sa.MetaData(db)
        self.tasks_table = sa.Table(
            'tasks', metadata,
            sa.Column('buildrequestId', sa.Integer, primary_key=True),
            sa.Column('taskId', sa.String(32), index=True),
            sa.Column('runId', sa.Integer),
            sa.Column('createdDate', sa.Integer,
                      doc="When the task was submitted to TC"),
            sa.Column('processedDate', sa.Integer,
                      doc="When we put it into BB"),
            sa.Column('takenUntil', sa.Integer, index=True,
                      doc="How long until our claim needs to be renewed")
        )

    async def delete_task_by_request_id(self, request_id):
        await self.tasks_table.delete(
            self.tasks_table.c.buildrequestId == request_id).execute()

    async def fetch_all_tasks(self):
        return await self.tasks_table.select().order_by(
            self.tasks_table.c.takenUntil.desc()).execute()


class BuildBotDB:
    def __init__(self, uri):
        db = sa.create_engine(uri, pool_size=1, pool_recycle=60,
                              #pool_timeout=60,
                              strategy=ASYNCIO_STRATEGY)
        metadata = sa.MetaData(db)
        metadata.reflect()
        self.buildrequests_table = metadata.tables["buildrequests"]

    async def get_cancelled_build_requests(self, build_request_ids):
        q = await self.buildrequests_table.select(
            [self.buildrequests_table.c.id]
        ).where(
            self.buildrequests_table.c.id in build_request_ids
        ).where(
                self.buildrequests_table.c.complete == 1
        ).where(
                self.buildrequests_table.c.claimed_at == 0
        ).fetchall()
        return [r[0] for r in q]
