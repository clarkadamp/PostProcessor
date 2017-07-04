from __future__ import absolute_import
from .filesystem import File
import collections
import os
import transmissionrpc

TREnvironment = collections.namedtuple('TREnvironment', ['app_version', 'time', 'directory', 'hash', 'id', 'name'])


class TorrentAdapter(object):

    def __init__(self, torrent):
        self._torrent = torrent

    def __getattr__(self, item):
        try:
            return getattr(self._torrent, item)
        except AttributeError:
            if item in self._torrent._fields:
                return self._torrent._fields[item].value

    def __get__(self, item):
        return self._torrent.files()[item]

    def __iter__(self):
        for file_id in self._torrent.files():
            yield File(torrent=self, torrent_file_id=file_id)


def get_environment_variables():
    return TREnvironment(
        app_version=os.environ.get('TR_APP_VERSION'),
        # If we find a decent use for this, use it
        # time=datetime.strptime(os.environ['TR_TIME_LOCALTIME'], '%a %b %d %H:%M:%S %Y'),
        directory=os.environ.get('TR_TORRENT_DIR'),
        hash=os.environ.get('TR_TORRENT_HASH'),
        id=os.environ.get('TR_TORRENT_ID'),
        name=os.environ.get('TR_TORRENT_NAME')
    )


def get_completed_torrent():
    environment_vars = get_environment_variables()
    client = transmissionrpc.Client(user='transremote', password='W@t3rm3l0n')
    torrent = TorrentAdapter(client.get_torrent(environment_vars.id))
