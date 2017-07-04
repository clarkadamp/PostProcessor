from __future__ import absolute_import
import logging
import os
import re
import tvdb_api
import yaml

from .exceptions import IdentificationError

logger = logging.getLogger(__name__)


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
