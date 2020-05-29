import logging
import os
import re
import shutil
import subprocess
from typing import List, Tuple

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import workbot
from workbot.config import ARTIC_NEXTFLOW_WORKTYPE, \
    PENDING_STATE, CANCELLED_STATE, COMPLETED_STATE, FAILED_STATE
from workbot.irods import BatonClient, Collection, BatonError, RodsError, \
    iget, iput
from workbot.schema import WorkInstance, State, find_work_type, find_state, \
    WorkType, ONTMeta

log = logging.getLogger(__name__)


class AnalysisError(Exception):
    """Exception raised for errors during the analysis process."""
    pass


class WorkBot(object):
    """A WorkBot is an extract, transform, load (ETL,
     https://en.wikipedia.org/wiki/Extract,_transform,_load) agent.

    It provides methods to get some data from an archive, stage it to a
    temporary filesystem, run a particular type of analysis on it and return
    the results to an archive, annotated with metadata about the process."""

    def __init__(self, archive_root: str, staging_root: str):
        """

        Args:
            archive_root: The iRODS collection root under which to store
                          results.
            staging_root: The directory root under which to store working data
                          during processing.
        """
        self.archive_root = archive_root
        """The root collection under which work results will be archived"""
        self.staging_root = staging_root
        """The root of the local directory where data will be staged during
        work"""

        self.work_type = ARTIC_NEXTFLOW_WORKTYPE
        """The type of work done. This allows the WorkBot to recognise
        suitable work in the database"""

        self.client = BatonClient()  # Starts on demand
        """The baton client for interaction with iRODS"""

    def find_analyses(self,
                      session: Session,
                      input_path: str,
                      states=None,
                      not_states=None) -> List[workbot.schema.WorkInstance]:
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
        q = session.query(WorkInstance).\
            join(State).\
            join(WorkType).\
            filter(WorkInstance.input_path == input_path).\
            filter(WorkType.name == self.work_type)

        if states:
            q = q.filter(State.name.in_(states))

        if not_states:
            q = q.filter(State.name.notin_(not_states))

        return q.all()

    def add_analysis(self,
                     session: Session,
                     input_path: str) -> workbot.schema.WorkInstance:

        """Adds a new analysis to the WorkBot database.

        Adds a new analysis and sets its state to Pending. If analyses exist
        for these data in states other than Cancelled or Failed, raises an
        error.

        Args:
            session: An open Session.
            input_path: The iRODS collection where the initial data are
                        located.

        Returns: WorkInstance

        Raises:
            AnalysisError: An error occurred adding the analysis.
        """

        ignore_states = [CANCELLED_STATE, COMPLETED_STATE]

        existing = self.find_analyses(session, input_path,
                                      not_states=ignore_states)
        if existing:
            tmpl = "An error occurred adding the analysis: " \
                   "analyses already exist for " \
                   "input {}, not in states {}"
            raise AnalysisError(tmpl.format(input_path, ignore_states))

        wtype = find_work_type(session, self.work_type)
        pending = find_state(session, PENDING_STATE)

        wi = WorkInstance(input_path, wtype, pending)
        log.info("Adding work {}".format(wi))
        session.add(wi)

        return wi

    @staticmethod
    def add_metadata(session: Session, wi: WorkInstance,
                     experiment_name: str, instrument_position: int):
        """Adds metadata for this work instance to the WorkBot database.

        Args:
            session: An open Session.
            wi: A WorkInstance to annotate.
            experiment_name: An ONT experiment name.
            instrument_position: An ONT instrument position
        """
        session.add(ONTMeta(wi, experiment_name, instrument_position))

    def archive_path(self, wi: WorkInstance) -> str:
        """Returns the iRODS collection where the work instance will store its
        results.

        Args:
            wi: A WorkInstance.

        Returns: str

        """
        return os.path.join(self.archive_root, str(wi.id))

    def staging_path(self, wi: WorkInstance) -> str:
        """Returns the local directory where the work instance will stage its
        files temporarily.

        Args:
            wi: A WorkInstance.

        Returns: str

        """
        return os.path.join(self.staging_root, str(wi.id))

    def is_input_path_present(self, wi: WorkInstance) -> bool:
        """Returns true if the input data path exists.

        Args:
            wi: A WorkInstance.

        Returns: bool

        """
        log.debug("Finding input data for {}".format(wi))

        exists = False
        try:
            coll = Collection(self.client, wi.input_path)
            exists = coll.exists()

            if not exists:
                log.info("Input collection {} does not exist".format(coll))
        except BatonError as e:
            log.error("Failed to find input data: {}".format(e))
            raise

        return exists

    def is_input_data_complete(self, wi: WorkInstance):
        """Returns true if the input data is complete and ready to process.

        Args:
            wi: A WorkInstance.

        Returns: bool

        """
        complete = False

        if self.is_input_path_present(wi):
            log.info("Checking for complete input data for {}".format(wi))
            # If a file named .*final_report.txt.gz is present, the run is
            # complete

            try:
                coll = Collection(self.client,  wi.input_path)
                contents = coll.list(contents=True)
                matches = list(filter(lambda x:
                                      re.search(r'final_report.txt.gz$', x),
                                      contents))
                if list(matches):
                    log.info("Found final report matches: {}".format(matches))
                    complete = True
            except BatonError as e:
                log.error("Failed to check input data: {}".format(e))
                raise

        return complete

    def stage_input_data(self, session: Session, wi: WorkInstance):
        """Stages input data from the archive to the local working directory if
        the input data are complete.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """
        if wi.is_pending():
            if self.is_input_data_complete(wi):
                log.info("Staging input data for {}".format(wi))

                src = wi.input_path
                dst = self.staging_path(wi)
                try:
                    iget(src, dst, force=True, verify_checksum=True,
                         recurse=True)
                    wi.staged(session)
                    session.commit()
                except RodsError as e:
                    log.error("Failed to stage input data "
                              "from {} to {}: {}".format(src, dst, e))
                    raise

    def run_analysis(self, session: Session, wi: WorkInstance):
        """Runs the analysis if the data have been staged to the local working
        directory.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """
        if wi.is_staged():
            log.info("Starting analysis for {}".format(wi))
            wi.started(session)
            session.commit()

            cmd = ["ls", "-l"]  # FIXME -- lookup the real command to run
            log.info("Running {} for {} ".format(cmd, wi))
            completed = subprocess.run(cmd, capture_output=True)
            if completed.returncode == 0:
                wi.succeeded(session)
                session.commit()
            else:
                raise AnalysisError("Running {} for {} failed with "
                                    "exit code: {}: {}".format(
                                     cmd, wi, completed.returncode,
                                     completed.stderr))

    def archive_output_data(self, session: Session, wi: WorkInstance):
        """Archives the analysis results if the analysis completed
        successfully.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """
        if wi.is_succeeded():
            log.info("Archiving output data for {}".format(wi))

            src = self.staging_path(wi)
            dst = self.archive_path(wi)
            try:
                iput(src, dst, force=True, verify_checksum=True,
                     recurse=True)
                wi.archived(session)
                session.commit()
            except RodsError as e:
                log.error("Failed to archive data "
                          "from {} to {}: {}".format(src, dst, e))
                raise

    def annotate_output_data(self, session: Session, wi: WorkInstance):
        """Annotates the archived analysis results if archiving is complete,
        possibly using platform-specific metadata.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """
        if wi.is_archived():
            log.info("Annotating output data for {}".format(wi))

            meta = session.query(ONTMeta).filter(ONTMeta.workinstance == wi).all()
            log.info("Got metadata: {}".format(meta))

            dst = self.archive_path(wi)
            coll = Collection(self.client, dst)

            for m in meta:
                coll.meta_add({"attribute": "experiment_name",
                               "value": m.experiment_name},
                              {"attribute": "instrument_slot",
                               "value": str(m.instrument_slot)})
            wi.annotated(session)
            session.commit()

    def unstage_input_data(self, session: Session, wi: WorkInstance):
        """Unstages the input data from the temporary local directory by
        deletion if the analysis results are archived and annotated.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """
        if wi.is_annotated():
            log.info("Unstaging input data for {}".format(wi))
            shutil.rmtree(wi.input_path, ignore_errors=True)
            wi.unstaged(session)
            session.commit()

    def complete_analysis(self, session: Session, wi: WorkInstance):
        """Marks the analysis as complete in the database. No further action
        will be taken on it.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """
        if wi.is_unstaged():
            log.info("Completed analysis for {}".format(wi))
            wi.completed(session)
            session.commit()


def add_ont_analyses(session: Session, baton: BatonClient,
                     experiment_slots: List[Tuple]) -> int:
    num_added = 0

    try:
        for experiment_name, instrument_slot in experiment_slots:
            log.info("Experiment {}, instrument slot {}".format(
                experiment_name, instrument_slot))

            # We should find in iRODS a single collection for a given
            # experiment and slot. However, if there's more then we can still
            # analyse each one.
            found = baton.meta_query([{"attribute": "experiment_name",
                                      "value": experiment_name},
                                      {"attribute": "instrument_slot",
                                       "value": str(instrument_slot)}],
                                     collection=True)

            log.debug("For expt: {} pos: {} found: {} "
                      "collections in iRODS".format(experiment_name,
                                                    instrument_slot, found))

            if not found:
                continue

            input_path = found[0]

            wb = WorkBot("", "")  # FIXME - Currently, just one kind of WorkBot
            analyses = wb.find_analyses(session, input_path,
                                        not_states=[FAILED_STATE,
                                                    CANCELLED_STATE])
            if analyses:
                log.info("Analyses exist for experiment {}, "
                         "instrument slot {}, skipping: {}".format(
                          experiment_name, instrument_slot, analyses))
            else:
                log.info("Adding an analysis for experiment {}, "
                         "instrument slot {}".format(
                          experiment_name, instrument_slot))

                wi = wb.add_analysis(session, input_path)
                wb.add_metadata(session, wi, experiment_name, instrument_slot)

                session.commit()
                num_added += 1
    except SQLAlchemyError:
        session.rollback()
        raise

    return num_added
