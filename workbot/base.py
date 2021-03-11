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

import functools
import logging
import os
import re
import shutil
import subprocess
import tempfile
from abc import ABCMeta, abstractmethod
from pathlib import Path, PurePath
from typing import FrozenSet, List, Union

from sqlalchemy.orm import Session

from workbot import irods
from workbot.config import config
from workbot.enums import WorkState, WorkType
from workbot.irods import AVU, BatonClient, BatonError, Collection, \
    DataObject, RodsError
from workbot.schema import State, WorkInstance, find_state

log = logging.getLogger(__name__)


class WorkBotError(Exception):
    """Exception raised for general WorkBot errors."""
    pass


class AnalysisError(WorkBotError):
    """Exception raised for errors during the analysis process."""
    pass


class AnnotationMixin(object, metaclass=ABCMeta):
    @abstractmethod
    def add_metadata(self, session: Session, wi: WorkInstance, **kwargs):
        """Adds metadata for this work instance to the WorkBot database.

        Args:
            session: An open Session.
            wi: A WorkInstance to annotate.
        """
        pass


# The following decorators handle all of the database updates that happen
# when WorkInstances move from one state to another.
def stage_op(method):
    """Decorator which handles WorkInstance state update for the stage
    operation."""

    @functools.wraps(method)
    def inner(ref, session: Session, wi: WorkInstance, **kwargs):
        if wi.is_pending():
            result = method(ref, session, wi, **kwargs)
            wi.staged(session)
            session.commit()
            return result

    return inner


def analyse_op(method):
    """Decorator which handles WorkInstance state update for the analysis
    operation."""

    @functools.wraps(method)
    def inner(ref, session: Session, wi: WorkInstance, **kwargs):
        if wi.is_staged():
            wi.started(session)
            session.commit()

            try:
                result = method(ref, session, wi, **kwargs)
                wi.succeeded(session)
                session.commit()
            except AnalysisError as e:
                log.error(e)
                wi.failed(session)
                session.commit()
                raise

            return result

    return inner


def archive_op(method):
    """Decorator which handles WorkInstance state update for the archive
    operation."""

    @functools.wraps(method)
    def inner(ref, session: Session, wi: WorkInstance, **kwargs):
        if wi.is_succeeded():
            result = method(ref, session, wi, **kwargs)
            wi.archived(session)
            session.commit()
            return result

    return inner


def annotate_op(method):
    """Decorator which handles WorkInstance state update for the annotate
    operation"""

    @functools.wraps(method)
    def inner(ref, session: Session, wi: WorkInstance, **kwargs):
        if wi.is_archived():
            result = method(ref, session, wi, **kwargs)
            wi.annotated(session)
            session.commit()
            return result

    return inner


def unstage_op(method):
    """Decorator which handles WorkInstance state update for the unstage
    operation"""

    @functools.wraps(method)
    def inner(ref, session: Session, wi: WorkInstance, **kwargs):
        if wi.is_annotated():
            result = method(ref, session, wi, **kwargs)
            wi.unstaged(session)
            session.commit()
            return result

    return inner


def complete_op(method):
    """Decorator which handles WorkInstance state update for the complete
    operation."""

    @functools.wraps(method)
    def inner(ref, session: Session, wi: WorkInstance, **kwargs):
        if wi.is_unstaged():
            result = method(ref, session, wi, **kwargs)
            wi.completed(session)
            session.commit()
            return result

    return inner


def cancel_op(method):
    """Decorator which handles WorkInstance state update for the cancel
    operation."""

    @functools.wraps(method)
    def inner(ref, session: Session, wi: WorkInstance, **kwargs):
        result = method(ref, session, wi, **kwargs)
        wi.cancelled(session)
        session.commit()
        return result

    return inner


class RodsHandler(object):
    """A handler for carrying out iRODS operations. WorkBots delegate to one of
    these."""
    client: BatonClient

    def __init__(self):
        self.client = BatonClient()

    def is_input_path_present(self, wi: WorkInstance) -> bool:
        """Returns true if the input data path exists.

        Args:
            wi: A WorkInstance.

        Returns: bool

        """
        log.debug("Finding input data for {}".format(wi))

        try:
            coll = Collection(self.client, wi.input_path)
            exists = coll.exists()

            if not exists:
                log.info("Input collection {} for "
                         "{} does not exist".format(coll, wi))
        except BatonError as e:
            log.error("Failed to find input data for {}: {}".format(wi, e))
            raise

        return exists

    def is_input_data_complete(self, wi: WorkInstance):
        """Returns true if the input data is complete and ready to process.

        Args:
            wi: A WorkInstance.

        Returns: bool
        """
        return True

    def collection(self, path) -> Collection:
        """Returns a new Collection for path."""
        return Collection(self.client, path)

    def data_object(self, path) -> DataObject:
        """Returns a new DataObject for path."""
        return DataObject(self.client, path)

    def meta_query(self, avus: List[AVU], zone=None,
                   collection=False, data_object=False):
        return self.client. \
            meta_query(avus, zone=zone,
                       collection=collection, data_object=data_object)

    def iget(self, remote_path, local_path, **kwargs):
        irods.iget(remote_path, local_path, **kwargs)

    def imkdir(self, remote_path, **kwargs):
        irods.imkdir(remote_path, **kwargs)

    def iput(self, local_path, remote_path, **kwargs):
        irods.iput(local_path, remote_path, **kwargs)


workbot_registry = {}


def register(cls):
    """Class decorator to register Workbot classes for the make_workbot
     factory function."""
    class_name = cls.__name__
    if class_name in workbot_registry:
        raise WorkBotError("WorkBot {} is already "
                           "registered".format(class_name))

    workbot_registry[class_name] = cls
    return cls


@register
class WorkBot(object):
    """A WorkBot is an extract, transform, load (ETL,
     https://en.wikipedia.org/wiki/Extract,_transform,_load) agent.

    It provides methods to get some data from an archive (iRODS), stage it to a
    temporary filesystem, run a particular type of analysis on it and return
    the results to an archive, annotated with metadata about the process.
    """

    config = config()
    """The configuration read from workbot.ini when the class is loaded."""

    def __compatible_work_types(self):
        compat = set()

        name = self.__class__.__name__
        for sec in WorkBot.config.sections():
            for key, value in WorkBot.config.items(sec):
                if key == "class" and value.lower() == name.lower():
                    compat.add(sec)

        if not compat:
            raise WorkBotError("Configuration file did not declare any "
                               "compatible work types for {}".format(name))

        return frozenset(compat)

    archive_root: str
    """The root collection under which work results will be archived. Data
    should not be placed in the archive root, but in a subdirectory under
    it."""

    staging_root: str
    """The root of the local directory where data will be staged during work.
    Data should not be placed in the staging root, but in a subdirectory under
    it."""

    work_type: str
    """The type of work done. This allows the WorkBot to recognise suitable 
    work in the database"""

    end_state: List[str]
    """The work states for are considered end points for all work on a given 
    data set. When an end state is reached, no further work of this instance's
    type can be queued for that data set."""

    rods_handler: RodsHandler

    def __init__(self, work_type: str,
                 archive_root: Union[PurePath, str] = None,
                 staging_root: Union[Path, str] = None,
                 rods_handler: RodsHandler = None,
                 end_states: List[WorkState] = None):
        """
        Args:
            work_type: The name of the type of work to be done. This can be
                       any string naming a type work that is compatible with
                       the workbot class. Compatibility is declared in the
                       workbot.ini file by using the "class" property to name
                       the compatible WorkBot class.
            archive_root: The iRODS collection root under which to store
                          any results. Optional, defaults to the current
                          working directory.
            staging_root: The directory root under which to store any working
                          data during processing. Optional, defaults to a
                          temporary directory created with tempfile.mkdtemp.
            rods_handler: An iRODS handler. Optional, defaults to a basic
                          handler.
        """

        if work_type is None:
            raise ValueError("work_type must be defined")
        if not re.match(r'[A-Za-z0-9_-]+$', work_type):
            raise ValueError("invalid work_type '{}' did "
                             "not match [A-Za-z0-9_-]+$".format(work_type))

        if work_type not in self.compatible_work_types():
            raise ValueError(
                    "invalid work type; '{}' was not one of the compatible "
                    "work types {}".format(work_type,
                                           self.compatible_work_types()))
        self.work_type = work_type

        aroot, sroot = archive_root, staging_root
        if aroot is None:
            aroot = os.getcwd()
            log.info("Defaulting to archive root {}".format(aroot))
        if sroot is None:
            prefix = "workbot.{}.".format(self.work_type)
            sroot = tempfile.mkdtemp(prefix=prefix)

        aroot, sroot = str(aroot), str(sroot)
        if not aroot.strip():
            raise ValueError("archive_root must not be empty")
        if not sroot.strip():
            raise ValueError("staging_root must not be empty")

        self.archive_root = aroot
        self.staging_root = sroot

        if rods_handler is None:
            rods_handler = RodsHandler()
        self.rods_handler = rods_handler

        if end_states is None:
            end_states = [WorkState.CANCELLED, WorkState.COMPLETED]
        self.end_states = end_states

    def compatible_work_types(self) -> FrozenSet[str]:
        """Returns the set of work types supported by this class."""

        return self.__compatible_work_types()

    def find_work(self,
                  session: Session,
                  input_path: Union[Path, str],
                  states=None,
                  not_states=None) -> List[WorkInstance]:
        """Finds work instances in the WorkBot database.

        Finds work instances in the WorkBot database, optionally limited to
        those in specified states.

        Args:
            session: An open Session.
            input_path: The iRODS collection where the initial data are
                        located.
            states: A list of states the analyses must have.
            not_states: A list of states the analyses must not have.

        Returns: List[WorkInstance]
        """

        q = session.query(WorkInstance). \
            join(State). \
            filter(WorkInstance.input_path == os.fspath(input_path)). \
            filter(WorkInstance.work_type == self.work_type)

        if states:
            q = q.filter(State.name.in_(states))

        if not_states:
            q = q.filter(State.name.notin_(not_states))

        return q.all()

    def add_work(self,
                 session: Session,
                 input_path: Union[Path, str]) -> Union[None, WorkInstance]:
        """Adds a new analysis (work instance) to the WorkBot database.

        Adds a new analysis and sets its state to Pending. If analyses exist
        for these data in states other than Cancelled or Failed, raises an
        error.

        Args:
            session: An open Session.
            input_path: The iRODS collection where the initial data are
                        located.

        Returns: WorkInstance or None.

        Raises:
            AnalysisError: An error occurred adding the analysis.
        """

        ended = self.find_work(session, input_path, states=self.end_states)
        if ended:
            raise AnalysisError("An error occurred adding the analysis: "
                                "analyses already "
                                "exist for input {}: {}".format(input_path,
                                                                ended))

        incomplete = self.find_work(session, input_path,
                                    not_states={*self.end_states,
                                                WorkState.CANCELLED,
                                                WorkState.COMPLETED})
        if incomplete:
            log.info("No new analysis added. Incomplete analyses already "
                     "exist for input {}: {}".format(input_path, incomplete))
            return None

        pending = find_state(session, WorkState.PENDING)

        wi = WorkInstance(input_path, self.work_type, pending)
        log.info("Adding work {}".format(wi))
        session.add(wi)
        session.commit()

        return wi

    def archive_path(self, wi: WorkInstance) -> PurePath:
        """Returns the iRODS collection where the work instance will store its
        results.

        Args:
            wi: A WorkInstance.

        Returns: str

        """

        return PurePath(self.archive_root, str(wi.id))

    def staging_path(self, wi: WorkInstance) -> Path:
        """Returns the local directory where the work instance will stage its
        files temporarily.

        Args:
            wi: A WorkInstance.

        Returns: str

        """

        return Path(self.staging_root, str(wi.id))

    def staging_input_path(self, wi: WorkInstance) -> Path:
        """Returns the local directory in the staging area where data for
        analysis will be staged. This is typically a directory named 'input'.

        Args:
            wi: A WorkInstance.

        Returns: str

        """

        return Path(self.staging_path(wi), "input")

    def staging_output_path(self, wi: WorkInstance) -> Path:
        """Returns the local directory in the staging area where data for
        archiving will be staged. This is typically a directory named 'output'.

        Args:
            wi: A WorkInstance.

        Returns: str

        """

        return Path(self.staging_path(wi), "output")

    def is_input_path_present(self, wi: WorkInstance):
        return self.rods_handler.is_input_path_present(wi)

    def is_input_data_complete(self, wi: WorkInstance):
        return self.rods_handler.is_input_data_complete(wi)

    @stage_op
    def stage_input_data(self, session: Session, wi: WorkInstance, **kwargs):
        """Stages input data from the archive to the local working directory if
        the input data are complete.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """

        if self.is_input_data_complete(wi):
            log.info("Staging input data for {}".format(wi))

            src = wi.input_path
            dst = self.staging_path(wi)

            dst.mkdir(parents=True, exist_ok=True)
            try:
                self.rods_handler.iget(src, dst, force=True,
                                       verify_checksum=True, recurse=True)

                # The leaf element of the archive input path will become a
                # new directory within the staging path. We need to rename
                # it to the generic input path name
                d = src.name
                tmp_staged = Path(dst, d)
                staged = self.staging_input_path(wi)

                log.debug("Moving staged input data into position "
                          "from {} to {}".format(tmp_staged, staged))
                shutil.move(tmp_staged, staged)
            except RodsError as e:
                log.error("Failed to stage input data for {} "
                          "from {} to {}: {}".format(wi, src, dst, e))
                raise

        return

    @analyse_op
    def run_analysis(self, session: Session, wi: WorkInstance, **kwargs):
        """Runs the analysis (work instance) if the data have been staged to
        the local working directory.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """

        log.info("Starting analysis for {}".format(wi))

        cmd_str = WorkBot.config.get(self.work_type, "command",
                                     fallback=None)
        if cmd_str is None:
            raise AnalysisError("Failed to find a 'command' value in the "
                                "'{}' section of the configuration "
                                "file".format(self.work_type))
        cmd_str = Path(cmd_str).resolve().as_posix()

        cmd = cmd_str.split()
        # These are the two parameters required to be supported by any script
        # we run to perform the work.
        cmd += ["-i", self.staging_input_path(wi),
                "-o", self.staging_output_path(wi),
                "-v"]

        dst = self.staging_output_path(wi)
        dst.mkdir(parents=True, exist_ok=True)

        log.info("Running {} for {} ".format(cmd, wi))
        completed = subprocess.run(cmd, capture_output=True,
                                   cwd=self.staging_output_path(wi))
        if completed.returncode != 0:
            rc = completed.returncode
            err = completed.stderr.decode("utf-8").rstrip()
            raise AnalysisError("Running {} for {} failed with "
                                "exit code: {}: {}".format(cmd, wi, rc, err))

        return

    @archive_op
    def archive_output_data(self, session: Session, wi: WorkInstance,
                            **kwargs):
        """Archives the analysis (work instance) results if the analysis
        completed successfully.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """

        log.info("Archiving output data for {}".format(wi))

        src = self.staging_output_path(wi)
        dst = self.archive_path(wi)

        try:
            coll = self.rods_handler.collection(dst)
            if not coll.exists():
                self.rods_handler.imkdir(dst, make_parents=True)

            self.rods_handler.iput(src, dst, force=True, verify_checksum=True,
                                   recurse=True)
        except RodsError as e:
            log.error("Failed to archive data for {} "
                      "from {} to {}: {}".format(wi, src, dst, e))
            raise

        return

    @annotate_op
    def annotate_output_data(self, session: Session, wi: WorkInstance,
                             **kwargs):
        """Annotates the archived analysis results if archiving is complete,
        possibly using platform-specific metadata.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """

        pass

    @unstage_op
    def unstage_input_data(self, session: Session, wi: WorkInstance, **kwargs):
        """Unstages the input data from the temporary local directory by
        deletion if the analysis results are archived and annotated.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """

        log.info("Unstaging input data for {}".format(wi))
        shutil.rmtree(self.staging_path(wi), ignore_errors=True)
        return

    @complete_op
    def complete_analysis(self, session: Session, wi: WorkInstance, **kwargs):
        """Marks the analysis as complete in the database. No further action
        will be taken on it.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """

        log.info("Completed analysis for {}".format(wi))
        return

    @cancel_op
    def cancel_analysis(self, session: Session, wi: WorkInstance, **kwargs):
        """Marks the analysis as cancelled in the database. No further action
        will be taken on it.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """

        if wi.is_staged() or wi.is_annotated():
            self.unstage_input_data(session, wi)

        log.info("Cancelled analysis for {}".format(wi))
        return

    def run(self, session: Session, wi: WorkInstance, **kwargs):
        self.stage_input_data(session, wi, **kwargs)
        self.run_analysis(session, wi, **kwargs)
        self.archive_output_data(session, wi, **kwargs)
        self.annotate_output_data(session, wi, **kwargs)
        self.unstage_input_data(session, wi, **kwargs)
        self.complete_analysis(session, wi, **kwargs)


def make_workbot(work_type: WorkType, **kwargs):
    """Returns a WorkBot instance suitable for running a work type.

    Args:
        work_type: A work type from the controlled vocabulary of work types.
        kwargs: Additional keyword arguments passed to the constructor of
        the WorkBot.

    Returns: WorkBot
    """

    compatible_classes = {}
    for worktype_section in WorkBot.config.sections():
        for key, value in WorkBot.config.items(worktype_section):
            if key == "class" and value is not None:
                compatible_classes[worktype_section] = value

    key = work_type.name
    if key not in compatible_classes:
        raise WorkBotError("Configuration file did not declare "
                           "any compatible WorkBot "
                           "for {}".format(key))

    class_name = compatible_classes[key]
    if class_name not in workbot_registry:
        raise WorkBotError("WorkBot class {} is not known".format(class_name))

    cls = workbot_registry[class_name]
    return cls(key, **kwargs)


class WorkBroker(object, metaclass=ABCMeta):
    """A broker responsible for finding work to be done and queueing it in the
    WorkBot database.
    """

    @abstractmethod
    def request_work(self, session: Session, **kwargs) -> int:
        """Queues new work in the WorkBot database and returns the number of
        work instances added, which may be zero if all work is already queued.

        Args:
            session: An open Session.

        Returns: int
        """
        pass
