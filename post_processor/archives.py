from __future__ import absolute_import
import logging
import os
import re
import subprocess
from .framework import handler
from .filesystem import File

logger = logging.getLogger(__name__)


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
    return run_command(['/usr/bin/unrar'] + options)


def run_command(command):
    if not isinstance(command, list):
        command = command.split(' ')
    logger.info("Running: '{}'".format(' '.join(command)))
    return subprocess.check_output(command)
