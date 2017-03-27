import logging
import aiohttp

log = logging.getLogger(__name__)
_api_root = None
_session = None


def init(api_root):
    global _api_root, _session
    _api_root = api_root
    conn = aiohttp.TCPConnector(limit=10)  # TODO:
    _session = aiohttp.ClientSession(connector=conn)


async def _do_request(method, url):
    # The private BuildAPI interface we use doesn't require auth but it
    # _does_ require REMOTE_USER to be set.
    # https://bugzilla.mozilla.org/show_bug.cgi?id=1156810 has additional
    # background on this.
    url = "%s/%s" % (_api_root, url)
    log.debug("Making %s request to %s", method, url)
    r = await _session.request(method, url,
                               headers={"X-Remote-User": "buildbot-bridge"})
    r.raise_for_status()


async def cancel_build(branch, bild_id):
    url = "%s/build/%s" % (branch, bild_id)
    log.info("Cancelling build: %s", url)
    await _do_request("DELETE", url)
