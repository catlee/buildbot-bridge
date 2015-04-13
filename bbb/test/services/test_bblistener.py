from mock import Mock
import unittest

import sqlalchemy as sa

from ...services.bblistener import BuildbotListener


def make_fake_schedulerdb(db):
    db.execute(sa.text("""
CREATE TABLE buildrequests (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `buildsetid` INTEGER NOT NULL,
                `buildername` VARCHAR(256) NOT NULL,
                `priority` INTEGER NOT NULL default 0,
                `claimed_at` INTEGER default 0,
                `claimed_by_name` VARCHAR(256) default NULL,
                `claimed_by_incarnation` VARCHAR(256) default NULL,
                `complete` INTEGER default 0,
                `results` SMALLINT,
                `submitted_at` INTEGER NOT NULL,
                `complete_at` INTEGER
            );
"""))
    db.execute(sa.text("""
CREATE TABLE builds (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `number` INTEGER NOT NULL,
                `brid` INTEGER NOT NULL,
                `start_time` INTEGER NOT NULL,
                `finish_time` INTEGER
            );
"""))
    db.execute(sa.text("""
CREATE TABLE sourcestamps (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `branch` VARCHAR(256) default NULL,
                `revision` VARCHAR(256) default NULL,
                `patchid` INTEGER default NULL,
                `repository` TEXT not null default '',
                `project` TEXT not null default ''
            );
"""))
    db.execute(sa.text("""
CREATE TABLE buildset_properties (
    `buildsetid` INTEGER NOT NULL,
    `property_name` VARCHAR(256) NOT NULL,
    `property_value` VARCHAR(1024) NOT NULL
);
"""))
    db.execute(sa.text("""
CREATE TABLE buildsets (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `external_idstring` VARCHAR(256),
                `reason` VARCHAR(256),
                `sourcestampid` INTEGER NOT NULL,
                `submitted_at` INTEGER NOT NULL,
                `complete` SMALLINT NOT NULL default 0,
                `complete_at` INTEGER,
                `results` SMALLINT
            );
"""))

class TestBuildbotListener(unittest.TestCase):
    def setUp(self):
        self.bblistener = BuildbotListener(
            bbb_db="sqlite:///:memory:",
            buildbot_db="sqlite:///:memory:",
            tc_credentials={
                "credentials": {
                    "clientId": "fake",
                    "accessToken": "fake",
                }
            },
            pulse_user="fake",
            pulse_password="fake",
            exchange="fake",
            topic="fake",
            tcWorkerGroup="workwork",
            tcWorkerId="workwork",
        )
        make_fake_schedulerdb(self.bblistener.buildbot_db.db)
        # Replace the TaskCluster Queue object with a Mock because we never
        # want to actually talk to TC, just check if the calls that would've
        # been made are correct
        self.bblistener.tc_queue = Mock()
        self.tasks = self.bblistener.bbb_db.tasks_table
        self.buildbot_db = self.bblistener.buildbot_db.db

    def testHandleStartedOneBuildRequest(self):
        self.buildbot_db.execute(sa.text("""
INSERT INTO buildrequests
    (id, buildsetid, buildername, submitted_at)
    VALUES (4, 0, "foo", 50);"""))
        self.buildbot_db.execute(sa.text("""
INSERT INTO builds
    (id, number, brid, start_time)
    VALUES (0, 2, 4, 60);"""))
        self.tasks.insert().execute(
            buildrequestId=4,
            taskId="OAgHxS9MRQSajDpOaChY6g",
            runId=0,
            createdDate=50,
            processedDate=60,
            takenUntil=None
        )
        data = {"payload": {"build": {"number": 2}}}
        self.bblistener.tc_queue.claimTask.return_value = {"takenUntil": 100}
        self.bblistener.handleStarted(data, {})

        self.assertEquals(self.bblistener.tc_queue.claimTask.call_count, 1)
        bbb_state = self.tasks.select().execute().fetchall()
        self.assertEquals(len(bbb_state), 1)
        self.assertEquals(bbb_state[0].takenUntil, 100)
