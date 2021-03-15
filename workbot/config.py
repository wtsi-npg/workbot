# -*- coding: utf-8 -*-
#
# Copyright © 2020 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# @author Keith James <kdj@sanger.ac.uk>

import configparser
import importlib
import logging
import os
import pwd
from pathlib import Path
from typing import List

from workbot.utilities import parse_qualified_class_name

log = logging.getLogger(__package__)


def get_config_paths() -> List[str]:
    """Returns a list of paths to be searched for a workbot.ini
    configuration file. These are, in order of priority:

    1. If the environment variable WORKBOT_CONFIG is set, the path
    specified by ${WORKBOT_CONFIG}

    2. In the current working directory  ${CWD}/workbot.ini

    3. ${HOME}/.workbot/workbot.ini

    4. If the environment variable XDG_CONFIG_HOME is set, the path
    specified by ${XDG_CONFIG_HOME}/workbot/workbot.ini

    5. If the environment variable XDG_CONFIG_HOME is not set, the path
    ${HOME}/.config/workbot/workbot.ini

    """
    config_file = "workbot.ini"
    config_dir = "workbot"
    dot_config_dir = "." + config_dir

    user = pwd.getpwuid(os.getuid()).pw_name
    home = os.getenv("HOME", os.path.join("home", user))
    xdg_data_home = os.getenv("XDG_DATA_HOME",
                              os.path.join(home, ".local", "share"))

    override_path = os.environ.get("WORKBOT_CONFIG")

    paths = []
    if override_path:
        paths.append(override_path)

    paths.append(os.path.join(os.getcwd(), config_file))

    if xdg_data_home:
        paths.append(os.path.join(xdg_data_home, config_dir, config_file))
    if home:
        paths.append(os.path.join(home, dot_config_dir, config_file))

    return paths


def read_config() -> configparser.ConfigParser:
    """Searches for the first config file available from the list of paths
    returned by get_config_paths() and reads it, returning the
    configuration. Raises an error if no file is found.

    Returns: configparser.ConfigParser
    """
    search = get_config_paths()

    for p in search:
        f = Path(p)
        if f.is_file():
            conf = configparser.ConfigParser()
            conf.read(f)
            return conf

    raise FileNotFoundError("No configuration file found "
                            "in: {}".format(search))


def load_classes_from_config(conf: configparser.ConfigParser):
    """Loads any classes mentioned under the 'class' keys in the supplied
    configuration.

    Args: configparser.ConfigParser
    """
    for sec in conf.sections():
        for key, value in conf.items(sec):
            if key == "class":
                module_name, class_name, = parse_qualified_class_name(value)
                log.debug("Loading {} {}".format(module_name, class_name))

                module = importlib.import_module(module_name)
                _ = getattr(module, class_name)
