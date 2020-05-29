from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func

from workbot.config import PENDING_STATE, STARTED_STATE, STAGED_STATE, \
    SUCCEEDED_STATE, FAILED_STATE, \
    UNSTAGED_STATE, COMPLETED_STATE, CANCELLED_STATE, \
    ARTIC_NEXTFLOW_WORKTYPE, ARCHIVED_STATE, ANNOTATED_STATE

WorkBotDBBase = declarative_base()


class StateTransitionError(Exception):
    def __init__(self,
                 current: str,
                 new: str):
        self.current = current
        self.new = new

        tmpl = "An error occurred changing state: " \
               "invalid transition from {} to {}"
        self.message = tmpl.format(self.current, self.new)

    def __repr__(self):
        return "<StateTransitionError: {}>".format(self.message)


class State(WorkBotDBBase):
    __tablename__ = 'state'

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(128), nullable=False)
    desc = Column(String(1024), nullable=False)

    def __init__(self,
                 name: str,
                 desc: str):
        self.name = name
        self.desc = desc

    def __repr__(self):
        return "<State: {}>".format(self.name)


class WorkType(WorkBotDBBase):
    __tablename__ = 'worktype'

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(128), nullable=False)
    desc = Column(String(1024), nullable=False)

    def __init__(self,
                 name: str,
                 desc: str):
        self.name = name
        self.desc = desc

    def __repr__(self):
        return "<WorkType: name={}, desc={}>".format(self.name, self.desc)


class WorkInstance(WorkBotDBBase):
    __tablename__ = 'workinstance'

    id = Column(Integer, autoincrement=True, primary_key=True)
    input_path = Column(String(2048), nullable=False)
    # output_path = Column(String(2048), nullable=True)

    type_id = Column(Integer, ForeignKey('worktype.id'), nullable=False)
    work_type = relationship("WorkType")

    state_id = Column(Integer, ForeignKey('state.id'), nullable=False)
    state = relationship("State")

    created = Column(DateTime(timezone=True), nullable=False,
                     default=func.now())
    last_updated = Column(DateTime(timezone=True), nullable=False,
                          default=func.now())



    def __init__(self,
                 input_path: str,
                 work_type: WorkType,
                 state: State):
        self.input_path = input_path
        self.work_type = work_type
        self.state = state

    def __repr__(self):
        tmpl = "<WorkInstance: id={}, input={}, type={}, state={} " \
               "created={} updated={}>"
        return tmpl.format(self.id, self.input_path,  self.work_type.name,
                           self.state, self.created, self.last_updated)

    """Changes the current state to Staged.

    Changes the state, if the current state is Pending.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def staged(self, session: Session):
        if self.state.name != PENDING_STATE:
            raise StateTransitionError(self.state.name, STAGED_STATE)

        self._update_state(session, STAGED_STATE)

    """Changes the current state to Started.

    Changes state to Started, if the current state is Staged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def started(self, session: Session):
        if self.state.name != STAGED_STATE:
            raise StateTransitionError(self.state.name, STARTED_STATE)

        self._update_state(session, STARTED_STATE)

    """Changes the current state to Succeeded.

    Changes state to Succeeded, if the current state is Started.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def succeeded(self, session: Session):
        if self.state.name != STARTED_STATE:
            raise StateTransitionError(self.state.name, SUCCEEDED_STATE)

        self._update_state(session, SUCCEEDED_STATE)

    """Changes the current state to Archived.

    Changes state to Archived, if the current state is Succeeded.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def archived(self, session: Session):
        if self.state.name != SUCCEEDED_STATE:
            raise StateTransitionError(self.state.name, ARCHIVED_STATE)

        self._update_state(session, ARCHIVED_STATE)

    """Changes the current state to Annotated.

    Changes state to Annotated, if the current state is Archived.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def annotated(self, session: Session):
        if self.state.name != ARCHIVED_STATE:
            raise StateTransitionError(self.state.name, ANNOTATED_STATE)

        self._update_state(session, ANNOTATED_STATE)

    """Changes the current state to Unstaged.

    Changes state to Unstaged, if the current state is Staged or Annotated.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def unstaged(self, session):
        if self.state.name not in [STAGED_STATE, ANNOTATED_STATE]:
            raise StateTransitionError(self.state.name, UNSTAGED_STATE)

        self._update_state(session, UNSTAGED_STATE)

    """Changes the current state to Completed.

    Changes state to Completed, if the current state is Unstaged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def completed(self, session: Session):
        if self.state.name != UNSTAGED_STATE:
            raise StateTransitionError(self.state.name, COMPLETED_STATE)

        self._update_state(session, COMPLETED_STATE)

    """Changes the current state to Failed.

    Changes state to Failed, if the current state is Started. Failed is an
    end state and data will remain staged for inspection until cleaned up.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def failed(self, session: Session):
        if self.state.name != STARTED_STATE:
            raise StateTransitionError(self.state.name, FAILED_STATE)

        self._update_state(session, FAILED_STATE)

    """Changes the current state to Cancelled.

    Changes state to Cancelled. This can be done from any state.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def cancelled(self, session: Session):
        self._update_state(session, CANCELLED_STATE)

    def is_pending(self):
        return self.state.name == PENDING_STATE

    def is_staged(self):
        return self.state.name == STAGED_STATE

    def is_started(self):
        return self.state.name == STARTED_STATE

    def is_succeeded(self):
        return self.state.name == SUCCEEDED_STATE

    def is_archived(self):
        return self.state.name == ARCHIVED_STATE

    def is_annotated(self):
        return self.state.name == ANNOTATED_STATE

    def is_unstaged(self):
        return self.state.name == UNSTAGED_STATE

    def is_completed(self):
        return self.state.name == COMPLETED_STATE

    def is_failed(self):
        return self.state.name == FAILED_STATE

    def is_cancelled(self):
        return self.state.name == CANCELLED_STATE

    def _update_state(self, session: Session, name: str):
        s = session.query(State).filter(State.name == name).one()
        self.state = s
        self.last_updated = func.now()
        session.flush()


class ONTMeta(WorkBotDBBase):
    """Oxford Nanopore-specific metadata."""
    __tablename__ = 'ontmeta'

    def __init__(self, wi: WorkInstance, experiment_name: str,
                 instrument_slot: int):
        """Create new ONTMeta metadata.

        Args:
            wi: A WorkInstance to which the metadata applies.
            experiment_name: ONT experiment name.
            instrument_slot: ONT instrument slot.
        """
        self.workinstance = wi
        self.experiment_name = experiment_name
        self.instrument_slot = instrument_slot

    id = Column(Integer, autoincrement=True, primary_key=True)
    workinstance_id = Column(Integer, ForeignKey('workinstance.id'),
                             nullable=False)
    workinstance = relationship("WorkInstance")
    experiment_name = Column(String(255), nullable=False)
    instrument_slot = Column(Integer, nullable=False)

    def __repr__(self):
        return "<ONTMeta: experiment: {}, position: {}>".format(
            self.experiment_name, self.instrument_slot)


def find_state(session: Session, name: str):
    return session.query(State).filter(State.name == name).one()


def find_work_type(session: Session, name: str):
    return session.query(WorkType).filter(WorkType.name == name).one()


def initialize_database(session: Session):
    """Initializes the database dictionary tables

    Inserts values into the dictionary tables for work types and work
    states."""
    _initialize_worktypes(session)
    _initialize_states(session)
    session.commit()


def _initialize_states(session):
    states = [
        State(name=PENDING_STATE, desc="Pending any action"),
        State(name=STAGED_STATE, desc="The work data are staged"),
        State(name=STARTED_STATE, desc="Work started"),
        State(name=SUCCEEDED_STATE, desc="Work was done successfully"),
        State(name=FAILED_STATE, desc="Work has failed"),
        State(name=ARCHIVED_STATE, desc="Work has been archived"),
        State(name=ANNOTATED_STATE, desc="Work has been annotated"),
        State(name=UNSTAGED_STATE, desc="The work data were unstaged"),
        State(name=COMPLETED_STATE, desc="All actions are complete"),
        State(name=CANCELLED_STATE, desc="Work was cancelled"),
    ]
    session.add_all(states)


def _initialize_worktypes(session):
    types = [
        WorkType(name=ARTIC_NEXTFLOW_WORKTYPE, desc="ARTIC NextFlow pipeline")
    ]
    session.add_all(types)
