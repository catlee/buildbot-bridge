"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -mbbb` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``bbb.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``bbb.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import asyncio
import logging

import click
import yaml

import bbb.db
import bbb.taskcluster
import bbb.reflector
import bbb.selfserve


@click.command()
@click.option('--config', type=click.File('rb'), required=True,
              help='YAML Config file')
def main(config):
    logging.basicConfig(level=logging.INFO, format="%(name)s - %(message)s")
    cfg = yaml.safe_load(config)

    bbb.db.init(bridge_uri=cfg["bbb"]["uri"], buildbot_uri=cfg["bb"]["uri"])
    bbb.taskcluster.init(
        options={"credentials": cfg["taskcluster"]["credentials"]})
    bbb.selfserve.init(cfg["selfserve"]["api_root"])
    loop = asyncio.get_event_loop()
    while True:
        loop.run_until_complete(bbb.reflector.main_loop())
