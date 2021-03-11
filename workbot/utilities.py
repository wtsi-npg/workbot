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

from argparse import ArgumentTypeError
from datetime import datetime

from workbot.enums import WorkType


def valid_iso_date(s: str):
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        raise ArgumentTypeError("Invalid date: '{}'.".format(s))


def valid_work_type(s: str):
    try:
        wt = WorkType[s]
    except KeyError:
        avail = [wt.name for wt in WorkType]
        raise ArgumentTypeError("Unknown work type: '{}'. "
                                "Available types are: {}".format(s, avail))

    return wt
