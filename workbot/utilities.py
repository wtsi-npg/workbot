# -*- coding: utf-8 -*-
#
# Copyright © 2020, 2021 Genome Research Ltd. All rights reserved.
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
from typing import Tuple

from workbot.enums import WorkType


def is_builtins_module(module_name: str) -> bool:
    """Returns true if the named module is the builtins module."""
    # Refer to the builtin object class
    return module_name == object.__class__.__module__


def parse_qualified_class_name(name: str) -> Tuple[str, str]:
    """Parses a qualified class name into its module name and unqualified
    class name parts, which it returns.

    Args:
        name: A qualified class name.

    Returns: Tuple[str, str]
    """
    module_parts = name.split(".")[:-1]
    module_name = ".".join(module_parts)
    class_name = name.split(".")[-1]

    return module_name, class_name


def qualified_class_name(cls: type) -> str:
    """Returns the qualified class name of a class."""
    module = cls.__module__
    name = cls.__name__

    if module is None:
        return name

    if is_builtins_module(module):
        return name

    return ".".join([module, name])


def valid_iso_date(s: str) -> datetime:
    """Parse an ISO date or raise an ArgumentTypeError.

    Args:
        s: date time.

    Returns: datetime.
    """
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        raise ArgumentTypeError("Invalid date: '{}'.".format(s))


def valid_work_type(s: str) -> WorkType:
    """Parse a work type or  raise an ArgumentTypeError.

    Args:
        s: A work type name.

    Returns: a WorkType enum member.
    """
    try:
        wt = WorkType[s]
    except KeyError:
        avail = [wt.name for wt in WorkType]
        raise ArgumentTypeError("Unknown work type: '{}'. "
                                "Available types are: {}".format(s, avail))

    return wt
