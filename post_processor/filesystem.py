from __future__ import absolute_import
import functools
import logging
import os

from .config import MOVIES, OTHER, TVSERIES
from .config import SOURCE_BASE, DESTINATION_BASE

logger = logging.getLogger(__name__)


def ensure_directory(path):
    try:
        os.makedirs(path)
        logger.info('created {}'.format(path))
    except OSError:
        if not os.path.exists(path):
            raise Exception('Failed to create directory {}'.format(path))


def link(file_handler):

    @functools.wraps(file_handler)
    def linker(*args, **kwargs):
        for fyle in file_handler(*args, **kwargs):
            if os.path.exists(fyle.destination_filename):
                logger.info('already exists: {}'.format(fyle.destination_filename))
                continue
            ensure_directory(fyle.destination_directory)
            logger.info('linking {} -> {}'.format(fyle.source_filename, fyle.destination_filename))
            os.link(fyle.source_filename, fyle.destination_filename)
            yield fyle

    return linker


class File(object):

    source_base = SOURCE_BASE
    destination_base = DESTINATION_BASE
    categories = [MOVIES, TVSERIES, OTHER]
    default_category = OTHER
    _category = None
    _internal_directory = None
    _filename = None

    def __init__(self, torrent=None, torrent_file_id=None, source_filename=None):
        self._torrent = torrent
        self._torrent_file_id = torrent_file_id
        self._source_filename = source_filename
        assert (torrent and torrent_file_id is not None) or source_filename

    def _split_on_category(self, path):
        parts = path_parts(path)
        if self.category in parts:
            category_index = parts.index(self.category)
            return os.path.join(*parts[:category_index]), os.path.join(*parts[category_index + 1:])
        return os.path.join(*parts)

    def _left_of_category(self, path):
        left, _ = self._split_on_category(path)
        return left

    def _right_of_category(self, path):
        _, right = self._split_on_category(path)
        return right

    @property
    def _torrent_file_data(self):
        return self._torrent.files()[self._torrent_file_id]

    @property
    def _torrent_internal_filename(self):
        return self._torrent_file_data['name']

    @property
    def _torrent_filename(self):
        return self._torrent.downloadDir + self._torrent_internal_filename

    @property
    def source_filename(self):
        if self._torrent:
            return self._torrent_filename
        return self._source_filename

    @property
    def source_directory(self):
        return os.path.dirname(self.source_filename).rstrip('/')

    @property
    def category(self):
        if self._category:
            return self._category

        parts = path_parts(self.source_directory)

        for category in self.categories:
            if category in parts:
                return category

        return self.default_category

    @category.setter
    def category(self, value):
        self._category = value

    @property
    def internal_directory(self):
        if self._internal_directory is not None:
            return self._internal_directory
        return self._right_of_category(self.source_directory)

    @internal_directory.setter
    def internal_directory(self, value):
        self._internal_directory = value

    @property
    def filename(self):
        if self._filename:
            return self._filename
        return os.path.basename(self.source_filename)

    @filename.setter
    def filename(self, value):
        self._filename = value

    @property
    def destination_directory(self):
        return os.path.join(self.destination_base, self.category, self.internal_directory)

    @property
    def destination_filename(self):
        return os.path.join(self.destination_directory, self.filename)

    def __copy__(self):
        return File(torrent=self._torrent, torrent_file_id=self._torrent_file_id, source_filename=self.source_filename)

    def __repr__(self):
        return "<{}: {} {} -> {}>".format(self.__class__.__name__, self.category, self.source_filename,
                                          self.destination_filename)


def path_parts(path):
    head, tail = os.path.split(path)
    if tail:
        return path_parts(head) + (tail,)
    return head,


def dissect_base_directory(path):
    parts = path_parts(path)
    if parts[-1] in [MOVIES, TVSERIES]:
        return '/' + '/'.join(parts[1:-1]), parts[-1]
    return '/' + '/'.join(parts[1:-1]), OTHER
