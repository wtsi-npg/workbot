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
import os
import pwd
from pathlib import Path

def get_config_paths():
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


def config():
    search = get_config_paths()

    for p in search:
        f = Path(p)
        if f.is_file():
            conf = configparser.ConfigParser()
            conf.read(f)
            return conf

    raise FileNotFoundError("No configuration file found "
                            "in: {}".format(search))
