import logging
import os
import re
import subprocess
from random import randint
from time import sleep
from typing import List

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import workbot
from workbot.config import ARTIC_NEXTFLOW_WORKTYPE, \
    PENDING_STATE, CANCELLED_STATE, COMPLETED_STATE
from workbot.irods import BatonClient, Collection, iget, BatonError, RodsError, \
    iput
from workbot.schema import WorkInstance, State, find_work_type, find_state, \
    WorkType

log = logging.getLogger(__name__)


class AnalysisError(Exception):
    pass


class WorkBot(object):
    def __init__(self, archive_root: str, staging_root: str):
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

    """Finds analyses in the WorkBot database.
    
    Finds analyses in the WorkBot database, optionally limited to those in 
    specified states.
    
    Args:
        session: An open SQL session.
        states: A list of states the analyses must have.
        not_states: A list of states the analyses must not have.
    
    Returns:
        List of matching analyses
    """
    def find_analyses(self,
                      session: Session,
                      input_path: str,
                      states=None,
                      not_states=None) -> List[workbot.schema.WorkInstance]:

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

    """Adds a new analysis to the WorkBot database.
     
    Adds a new analysis and sets its state to Pending. If analyses exist for
    these data that are in states other than Cancelled or Failed, raises an
    error.
    
    Args:
        session: An open SQL session.
        
    Returns:
        A new work instance.
        
    Raises:
        AnalysisError: An error occurred adding the analysis.
    """
    def add_analysis(self,
                     session: Session,
                     input_path: str) -> workbot.schema.WorkInstance:
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

        work = WorkInstance(input_path, wtype, pending)
        log.info("Adding work {}".format(work))

        session.add(work)
        session.flush()
        return work

    def archive_path(self, wi: WorkInstance):
        return os.path.join(self.archive_root, str(wi.id))

    def staging_path(self, wi: WorkInstance):
        return os.path.join(self.staging_root, str(wi.id))

    def find_input_data(self, wi: WorkInstance):
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

    def has_complete_input_data(self, wi: WorkInstance):
        complete = False

        if self.find_input_data(wi):
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
        if wi.is_pending():
            if self.has_complete_input_data(wi):
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
        if wi.is_staged():
            log.info("Starting analysis for {}".format(wi))
            wi.started(session)
            session.commit()

            log.info("Running analysis for {}".format(wi.id))
            sleep(randint(10, 30))
            # FIXME

            wi.succeeded(session)
            session.commit()

    def archive_output_data(self, session: Session, wi: WorkInstance):
        if wi.is_succeeded():
            log.info("Archiving output data for {}".format(wi))

            src = self.staging_path(wi)
            dst = self.archive_root
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
        if wi.is_archived():
            log.info("Annotating output data for {}".format(wi))
            # FIXME
            wi.annotated(session)
            session.commit()

    def unstage_input_data(self, session: Session, wi: WorkInstance):
        if wi.is_annotated():
            log.info("Unstaging input data for {}".format(wi))
            # FIXME
            wi.unstaged(session)
            session.commit()

    def complete_analysis(self, session: Session, wi: WorkInstance):
        if wi.is_unstaged():
            log.info("Completed analysis for {}".format(wi))
            wi.completed(session)
            session.commit()


def add_new_analyses(session: Session, baton: BatonClient, expts) -> int:
    num_added = 0

    try:
        for expt, pos in expts:
            log.info("Experiment {}, Position {}".format(expt, pos))

            # We should find in iRODS a single collection for a given
            # experiment and slot. However, if there's more then we can still
            # analyse each one.
            found = baton.meta_query([{"attribute": "experiment_name",
                                      "value": expt},
                                      {"attribute": "instrument_slot",
                                       "value": str(pos)}], collection=True)

            log.debug("For expt: {} pos: {} found: {} "
                      "collections in iRODS".format(expt, pos, found))

            if not found:
                continue

            input_path = found[0]

            wb = WorkBot("", "")  # FIXME - Currently, just one kind of WorkBot
            analyses = wb.find_analyses(session, input_path)
            if analyses:
                log.info("Analyses exist for experiment {}, position {}, "
                         "skipping: {}".format(expt, pos, analyses))
            else:
                log.info("Adding an analysis for experiment {}, "
                         "position {}".format(expt, pos))
                wb.add_analysis(session, input_path)
                session.flush()
                num_added += 1

        session.commit()
    except SQLAlchemyError:
        session.rollback()
        raise

    return num_added
