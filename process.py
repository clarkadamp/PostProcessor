import copy
import logging
import os
import re
import subprocess
import functools

import transmissionrpc
import tvdb_api

import pdb
import yaml

from collections import namedtuple
from datetime import datetime

logger = logging.getLogger('file_processor')
logging.basicConfig(level=logging.INFO)

TREnvironment = namedtuple('TREnvironment', ['app_version', 'time', 'directory', 'hash', 'id', 'name'])

MOVIES = 'Movies'
TVSERIES = 'TVSeries'
OTHER = 'Other'


class File(object):

    source_base = '/home/user/Done'
    destination_base = '/home/user/g_drive'
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


def get_environment_variables():
    return TREnvironment(
        app_version=os.environ['TR_APP_VERSION'],
        time=datetime.strptime(os.environ['TR_TIME_LOCALTIME'], '%a %b %d %H:%M:%S %Y'),
        directory=os.environ['TR_TORRENT_DIR'],
        hash=os.environ['TR_TORRENT_HASH'],
        id=os.environ['TR_TORRENT_ID'],
        name=os.environ['TR_TORRENT_NAME']
    )


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

file_handlers = list()


def handler(match_regex, category=None):

    matcher = re.compile(match_regex, re.IGNORECASE)

    def wrapper(file_handler):

        @functools.wraps(file_handler)
        def wrapped(*args, **kwargs):
            return file_handler(*args, **kwargs)

        assert (matcher, category) not in [(m, c) for m, c, h in file_handlers]
        file_handlers.append((matcher, category, wrapped))

        return wrapped

    return wrapper


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


def validate_episode_filename(filename, containing_directory):
    episode_regexes = [
        re.compile(r"^(?P<seriesname>.*)s(?P<season>\d+)e(?P<episode>\d+)(?P<episodename>.*)\.(?P<extension>[^.]+)$",
                   re.IGNORECASE)
        ]
    for hint in [filename, containing_directory]:
        for regex in episode_regexes:
            result = regex.search(hint)
            if result:
                return dict(
                    seriesname=normalise_show_name(result.group('seriesname')),
                    season=int(result.group('season')),
                    episode=int(result.group('episode')),
                    extension=result.group('extension')
                )

    message = "No regex match for '{}'".format(filename)
    raise IdentificationError(message)


def normalise_show_name(show_name):
    return re.sub(r'[.\-_]+', ' ', show_name).strip().title()


class SeriesIDs(object):
    default_location = '/home/user/Done/tv_ids.yaml'

    def __init__(self, yaml_location=None):
        self._yaml_location = yaml_location if yaml_location is not None else self.default_location
        self.id_data = dict()

    def __enter__(self):
        if os.path.exists(self._yaml_location):
            with open(self._yaml_location) as fh:
                self.id_data = yaml.load(fh)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with open(self._yaml_location, 'wb') as fh:
            fh.write(yaml.safe_dump(self.id_data, indent=4, default_flow_style=False))

    def __iter__(self):
        for seriesname in self.id_data.keys():
            yield seriesname

    def __getitem__(self, seriesname):
        return self.id_data[seriesname]

    def __setitem__(self, seriesname, tvdb_id):
        self.id_data[seriesname] = tvdb_id


def get_tvdb_id(seriesname):
    with SeriesIDs() as tvdb_ids:
        if seriesname not in tvdb_ids:
            try:
                show = tvdb_api.Tvdb()[seriesname]
                tvdb_ids[seriesname] = int(show.data['id'])
                logger.warn("TVDB found '{}' -> {} ({})".format(seriesname, show.data['seriesname'], show.data['id']))
            except tvdb_api.tvdb_shownotfound:
                logger.warn("Show not found in TVDB for: '{}'".format(seriesname))
                raise IdentificationError('Show not found in TVDB: {}'.format(seriesname))
        return tvdb_ids[seriesname]


class IdentificationError(Exception):
    pass


def name_via_tvdb(fyle):

    try:
        episode_details = validate_episode_filename(fyle.filename, fyle.source_directory)
    except RuntimeError:
        logger.warn('Unable parse: {}'.format(fyle.filename))
        raise IdentificationError('Unable parse: {}'.format(fyle.filename))

    show_id = get_tvdb_id(episode_details['seriesname'])

    try:
        show = tvdb_api.Tvdb()[show_id]
    except tvdb_api.tvdb_error as e:
        raise IdentificationError(e.message)

    try:
        episode = show[episode_details['season']][episode_details['episode']]
    except tvdb_api.tvdb_episodenotfound:
        logger.warn('Unable to find show for: {}'.format(fyle.filename))
        raise IdentificationError('Unable to find show for: {}'.format(fyle.filename))

    episode_details['seriesname'] = show.data['seriesname']
    episode_details['episodename'] = episode['episodename']

    internal_directory = '{seriesname}/Season {season}'.format(**episode_details)
    filename = '{seriesname} - S{season:02d}E{episode:02d} - {episodename}.{extension}'.format(**episode_details)
    return internal_directory, filename


@handler(r'\.([rR][aA][rR])|([rR]\d\d)$')
def unrar(fyle):
    if re.search(r'\.[Rr]\d\d$', fyle.source_filename):
        logger.info("Ignoring: {}".format(fyle.source_filename))
        raise StopIteration

    try:
        validate_rar(fyle.source_filename)
        logger.info("RAR verified: {}".format(fyle.source_filename))
    except subprocess.CalledProcessError:
        logger.info("RAR verify failed: {}".format(fyle.source_filename))
        raise StopIteration

    for archived_file, size in rar_contents(fyle.source_filename):
        sub_fyle = File(source_filename=os.path.join(fyle.source_directory, archived_file))
        if not os.path.exists(sub_fyle.source_filename):
            try:
                extract_rar(archive=fyle.source_filename, archived_file=archived_file,
                            destination_directory=fyle.source_directory)
            except subprocess.CalledProcessError:
                logger.error("Could not extract '{}' from '{}'".format(archived_file, fyle.source_filename))
                continue
        yield sub_fyle


def rar_contents(archive):
    try:
        output = run_rar(['l', archive])
    except subprocess.CalledProcessError:
        logger.error('unable to get archive contents')
        raise StopIteration

    file_list_section = False
    for line in output.split('\n'):
        if line.startswith("----------- ---------  ---------- -----  ----"):
            file_list_section = not file_list_section
            continue
        if not file_list_section:
            continue
        size, name = re.search(r'[^\s]+\s+([^\s]+)\s+[^\s]+\s+[^\s]+\s+(.*)', line).groups()
        yield name.strip(), int(size)


def extract_rar(archive, archived_file, destination_directory):
    return run_rar(['x', archive, archived_file, destination_directory])


def validate_rar(rar_filename):
    return run_rar(['t', rar_filename])


def run_rar(options):
    return run_command(['/usr/bin/unrar'] + options )


def run_command(command):
    if not isinstance(command, list):
        command = command.split(' ')
    logger.info("Running: '{}'".format(' '.join(command)))
    return subprocess.check_output(command)


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


def process_files(torrent):
    for fyle in torrent:
        handle_file(fyle)


@handler(r'.*')
def no_op(fyle):
    logger.info('no action taken for {}:{}'.format(fyle.category, fyle.source_filename))
    if False:
        # Turn this function into a generator
        yield None
    raise StopIteration


def main():
    environment_vars = get_environment_variables()
    client = transmissionrpc.Client(user='transremote', password='W@t3rm3l0n')
    torrent = TorrentAdapter(client.get_torrent(environment_vars.id))
    process_files(torrent)


if __name__ == '__main__':
    main()
