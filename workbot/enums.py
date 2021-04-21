# -*- coding: utf-8 -*-
#
# Copyright © 2021 Genome Research Ltd. All rights reserved.
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

from enum import Enum, unique


@unique
class WorkType(Enum):
    EMPTY = "A null pipeline, does nothing"
    ARTICNextflow = "ARTIC Nextflow pipeline for ONT run data",
    ONTRunMetadataUpdate = "ONT Run Metadata update"


@unique
class WorkState(Enum):
    PENDING = "Pending any action"
    STAGED = "Work data have been staged"

    STARTED = "Work has started"
    SUCCEEDED = "Work was done successfully"
    ARCHIVED = "Work data have been archived"
    ANNOTATED = "Work data have been annotated"
    UNSTAGED = "Work data have been unstaged"
    COMPLETED = "All actions are complete"

    FAILED = "Work has failed"
    CANCELLED = "Work has been cancelled"
