from __future__ import absolute_import
import functools
import logging
import re
from .transmission import get_completed_torrent

logger = logging.getLogger(__name__)


file_handlers = list()


def handler(match_regex, category=None, order=65535):

    matcher = re.compile(match_regex, re.IGNORECASE)

    def wrapper(file_handler):

        @functools.wraps(file_handler)
        def wrapped(*args, **kwargs):
            return file_handler(*args, **kwargs)

        assert (matcher, category) not in [(m, c) for m, c, h in file_handlers]
        file_handlers.append((matcher, category, wrapped, order))

        return wrapped

    return wrapper


def handle_file(fyle):
    for matcher, category, file_handler in file_handlers:
        if category is None or fyle.category == category:
            if matcher.search(fyle.source_filename):
                logger.info("Handling with {} on {}".format(file_handler.__name__, fyle.source_filename))
                for sub_fyle in file_handler(fyle):
                    handle_file(sub_fyle)
                break
    else:
        raise RuntimeError("Don't know how to handle: {}:{}".format(fyle.category, fyle.source_filename))


def process_torrent(torrent):
    for fyle in torrent:
        handle_file(fyle)


def process_completed_torrent():
    process_torrent(get_completed_torrent())
