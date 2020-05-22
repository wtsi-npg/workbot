import logging
from typing import List

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import workbot
from workbot.config import ARTIC_NEXTFLOW_WORKTYPE, OXFORD_NANOPORE, \
    FAILED_STATE, CANCELLED_STATE, PENDING_STATE
from workbot.schema import WorkInstance, State, \
    find_instrument_type, find_work_type, find_state

log = logging.getLogger(__name__)


class AnalysisError(Exception):
    pass


class WorkBotBase(object):
    def __init__(self,
                 inst_manufacturer: str,
                 inst_model: str,
                 inst_position: int,
                 expt_name: str):
        self.instrument_manufacturer = inst_manufacturer
        self.instrument_model = inst_model
        self.instrument_position = inst_position
        self.experiment_name = expt_name


class ONTWorkBot(WorkBotBase):
    def __init__(self,
                 inst_model: str,
                 inst_position: int,
                 expt_name: str):
        super().__init__(OXFORD_NANOPORE, inst_model, inst_position, expt_name)

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
        itype = find_instrument_type(session,
                                     self.instrument_manufacturer,
                                     self.instrument_model)

        q = session.query(WorkInstance).\
            join(State).\
            filter(WorkInstance.instrument_type == itype,
                   WorkInstance.experiment_name == self.experiment_name,
                   WorkInstance.instrument_position == self.instrument_position)

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
                   "{} experiment {} position {}, not in states {}"
            raise AnalysisError(tmpl.format(self.instrument_model,
                                            self.experiment_name,
                                            self.instrument_position,
                                            ignore_states))

        itype = find_instrument_type(session,
                                     self.instrument_manufacturer,
                                     self.instrument_model)
        wtype = find_work_type(session, self.choose_analysis())
        pending = find_state(session, PENDING_STATE)

        work = WorkInstance(itype,
                            self.instrument_position,
                            self.experiment_name, wtype, pending)
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


def add_ont_analyses(session: Session, inst_model: str, expts) -> int:
    num_added = 0

    try:
        for expt, pos in expts:
            log.info("Experiment {}, Position {}".format(expt, pos))

            wb = ONTWorkBot(inst_model, pos, expt)
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