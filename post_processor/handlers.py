from __future__ import absolute_import
import logging
import os

from .config import TVSERIES, MOVIES
from .exceptions import IdentificationError
from .filesystem import link
from .framework import handler
from .tv import name_via_tvdb

logger = logging.getLogger(__name__)


@handler(r'\.(mkv)|(avi)|(mp4)$', TVSERIES)
@link
def tv_series_episodes(fyle):
    # Rename the file and directory structure based on tvdb
    try:
        fyle.internal_directory, fyle.filename = name_via_tvdb(fyle)
        yield fyle
    except IdentificationError:
        # The identifier will log
        pass


@handler(r'\.(mkv)|(avi)|(mp4)$', MOVIES)
@link
def movies(fyle):
    # only movie files bigger than 96Mb, removes dumbass samples
    if os.stat(fyle.source_filename) > 100000000:
        # make sure that it is in root
        fyle.internal_directory = ''
        yield fyle


@handler(r'.*')
def no_op(fyle):
    logger.info('no action taken for {}:{}'.format(fyle.category, fyle.source_filename))
    if False:
        # Turn this function into a generator
        yield None
    raise StopIteration
