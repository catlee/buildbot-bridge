import sqlalchemy as sa


def makeSchedulerDb(db):
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

    db.execute(sa.text("""
CREATE TABLE changes (
                `changeid` INTEGER PRIMARY KEY AUTOINCREMENT, -- also serves as 'change number'
                `author` VARCHAR(1024) NOT NULL,
                `comments` VARCHAR(1024) NOT NULL, -- too short?
                `is_dir` SMALLINT NOT NULL, -- old, for CVS
                `branch` VARCHAR(1024) NULL,
                `revision` VARCHAR(256), -- CVS uses NULL. too short for darcs?
                `revlink` VARCHAR(256) NULL,
                `when_timestamp` INTEGER NOT NULL, -- copied from incoming Change
                `category` VARCHAR(256) NULL,

                -- repository specifies, along with revision and branch, the
                -- source tree in which this change was detected.
                `repository` TEXT NOT NULL default '',

                -- project names the project this source code represents.  It is used
                -- later to filter changes
                `project` TEXT NOT NULL default ''
            );
"""))

    db.execute(sa.text("""
CREATE TABLE change_files (
                `changeid` INTEGER NOT NULL,
                `filename` VARCHAR(1024) NOT NULL
            );
"""))

    db.execute(sa.text("""
CREATE TABLE change_links (
                `changeid` INTEGER NOT NULL,
                `link` VARCHAR(1024) NOT NULL
            );
"""))

    db.execute(sa.text("""
CREATE TABLE change_properties (
                `changeid` INTEGER NOT NULL,
                `property_name` VARCHAR(256) NOT NULL,
                `property_value` VARCHAR(1024) NOT NULL -- too short?
            );
"""))

    db.execute(sa.text("""
CREATE TABLE sourcestamp_changes (
                `sourcestampid` INTEGER NOT NULL,
                `changeid` INTEGER NOT NULL
            );
"""))


def makeBBBDb(db):
    db.execute(sa.text("""
CREATE TABLE tasks (
                `buildrequestId` INTEGER,
                `taskId` VARCHAR(32) NOT NULL,
                `runId` INTEGER NOT NULL,
                `createdDate` INTEGER,
                `processedDate` INTEGER,
                `takenUntil` INTEGER,
                CONSTRAINT bbb_taskId_runId UNIQUE (taskId, runId)
            );
"""))
