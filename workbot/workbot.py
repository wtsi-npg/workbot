import logging
from typing import List

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import workbot
from workbot.config import ARTIC_NEXTFLOW_WORKTYPE, \
    PENDING_STATE, FAILED_STATE, CANCELLED_STATE
from workbot.irods import BatonClient
from workbot.schema import WorkInstance, State, find_work_type, find_state

log = logging.getLogger(__name__)


class AnalysisError(Exception):
    pass


class WorkBot(object):
    def __init__(self, input_path: str):
        self.input_path = input_path

    """Returns the identifier of an analysis suitable for the data"""
    @staticmethod
    def choose_analysis() -> str:
        return ARTIC_NEXTFLOW_WORKTYPE

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
                      states=None,
                      not_states=None) -> List[workbot.schema.WorkInstance]:

        q = session.query(WorkInstance).\
            join(State).\
            filter(WorkInstance.input_path == self.input_path)

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
                     session: Session) -> workbot.schema.WorkInstance:
        ignore_states = [CANCELLED_STATE, FAILED_STATE]

        existing = self.find_analyses(session, not_states=ignore_states)
        if existing:
            tmpl = "An error occurred adding the analysis: " \
                   "analyses already exist for " \
                   "input {}, not in states {}"
            raise AnalysisError(tmpl.format(self.input_path, ignore_states))

        wtype = find_work_type(session, self.choose_analysis())
        pending = find_state(session, PENDING_STATE)

        work = WorkInstance(self.input_path, wtype, pending)
        log.info("Adding work {}".format(work))

        session.add(work)
        session.flush()
        return work

    def find_input_data(self):
        raise NotImplementedError

    def has_complete_input_data(self):
        raise NotImplementedError

    def stage_input_data(self):
        raise NotImplementedError

    def run_analysis(self):
        raise NotImplementedError

    def store_output_data(self):
        raise NotImplementedError

    def unstage_input_data(self):
        raise NotImplementedError

    def annotate_output_data(self):
        raise NotImplementedError


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

            wb = WorkBot(input_path)
            analyses = wb.find_analyses(session)
            if analyses:
                log.info("Analyses exist for experiment {}, position {}, "
                         "skipping: {}".format(expt, pos, analyses))
            else:
                log.info("Adding an analysis for experiment {}, "
                         "position {}".format(expt, pos))
                wb.add_analysis(session)
                session.flush()
                num_added += 1

        session.commit()
    except SQLAlchemyError:
        session.rollback()
        raise

    return num_added
