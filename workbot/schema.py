import operator

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import func

from workbot.config import PENDING_STATE, STARTED_STATE, STAGED_STATE, \
    FAILED_STAGING_STATE, SUCCEEDED_STATE, FAILED_STATE, \
    FAILED_UNSTAGING_STATE, UNSTAGED_STATE, \
    COMPLETED_STATE, CANCELLED_STATE, ARTIC_NEXTFLOW_WORKTYPE, OXFORD_NANOPORE, \
    GRIDION_MODEL, PROMETHION_MODEL

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


class InstrumentType(WorkBotDBBase):
    __tablename__ = 'instrumenttype'

    id = Column(Integer, autoincrement=True, primary_key=True)
    manufacturer = Column(String(128), nullable=False)
    model = Column(String(128), nullable=False)

    def __init__(self,
                 manufacturer: str,
                 model: str):
        self.manufacturer = manufacturer
        self.model = model

    def __repr__(self):
        tmpl = "<InstrumentType: manuf={}, model={}>"
        return tmpl.format(self.manufacturer, self.model)


class WorkInstance(WorkBotDBBase):
    __tablename__ = 'workinstance'

    id = Column(Integer, autoincrement=True, primary_key=True)
    experiment_name = Column(String(1024), nullable=False,
                             comment="name of the experiment or run")
    input_manifest = Column(String(2048), nullable=True)
    output_manifest = Column(String(2048), nullable=True)

    type_id = Column(Integer, ForeignKey('worktype.id'), nullable=False)
    work_type = relationship("WorkType")

    instrument_id = Column(Integer, ForeignKey('instrumenttype.id'),
                           nullable=False)
    instrument_type = relationship("InstrumentType")
    instrument_position = Column(Integer, nullable=False)

    state_id = Column(Integer, ForeignKey('state.id'), nullable=False)
    state = relationship("State")

    created = Column(DateTime(timezone=True), nullable=False,
                     default=func.now())
    last_updated = Column(DateTime(timezone=True), nullable=False,
                          default=func.now())

    def __init__(self,
                 inst_type: InstrumentType,
                 inst_position: int,
                 expt_name: str,
                 work_type: WorkType,
                 state: State):
        self.instrument_type = inst_type
        self.instrument_position = inst_position
        self.experiment_name = expt_name
        self.work_type = work_type
        self.state = state

    def __repr__(self):
        tmpl = "<WorkInstance: id={}, instr={}, type={}, state={} " \
               "created={} updated={}>"
        return tmpl.format(self.id, self.instrument_type, self.work_type.name,
                           self.state, self.created, self.last_updated)

    """Changes the current state to Staged.

    Changes the state, if the current state is Pending.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def staged(self, session: Session):
        if self.state is None or self.state.name != PENDING_STATE:
            raise StateTransitionError(self.state.name, STAGED_STATE)

        self.update_state(session, STAGED_STATE)

    """Changes the current state to Failed Staging.

    Changes state to Failed Staging, if the current state is Pending.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def failed_staging(self, session: Session):
        if self.state is None or self.state.name != PENDING_STATE:
            raise StateTransitionError(self.state.name, FAILED_STAGING_STATE)

        self.update_state(session, FAILED_STAGING_STATE)

    """Changes the current state to Started.

    Changes state to Started, if the current state is Staged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def started(self, session: Session):
        if self.state is None or self.state.name != STAGED_STATE:
            raise StateTransitionError(self.state.name, STARTED_STATE)

        self.update_state(session, STARTED_STATE)

    """Changes the current state to Succeeded.

    Changes state to Succeeded, if the current state is Started.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def succeeded(self, session: Session):
        if self.state is None or self.state.name != STARTED_STATE:
            raise StateTransitionError(self.state.name, SUCCEEDED_STATE)

        self.update_state(session, SUCCEEDED_STATE)

    """Changes the current state to Failed.

    Changes state to Failed, if the current state is Started.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def failed(self, session: Session):
        if self.state is None:
            raise StateTransitionError(None, UNSTAGED_STATE)

        if self.state.name != STARTED_STATE:
            raise StateTransitionError(self.state.name, FAILED_STATE)

        self.update_state(session, FAILED_STATE)

    """Changes the current state to Cancelled.

    Changes state to Cancelled. This can be done from any state.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def cancelled(self, session: Session):
        self.update_state(session, CANCELLED_STATE)

    """Changes the current state to Unstaged.

    Changes state to Unstaged, if the current state is Staged, Failed Staging,
    Succeeded or Failed.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def unstaged(self, session):
        if self.state is None or \
                self.state.name not in [STAGED_STATE,
                                        FAILED_STAGING_STATE,
                                        SUCCEEDED_STATE,
                                        FAILED_STATE,
                                        FAILED_UNSTAGING_STATE]:
            raise StateTransitionError(self.state.name, UNSTAGED_STATE)

        self.update_state(session, UNSTAGED_STATE)

    """Changes the current state to Failed Unstaging.

    Changes state to Failed Unstaging, if the current state is Staged,
    Succeeded or Failed.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def failed_unstaging(self, session: Session):
        if self.state is None or self.state.name not in [STAGED_STATE,
                                                         SUCCEEDED_STATE,
                                                         FAILED_STATE]:
            raise StateTransitionError(self.state.name,
                                       FAILED_UNSTAGING_STATE)

        self.update_state(session, FAILED_UNSTAGING_STATE)

    """Changes the current state to Completed.

    Changes state to Completed, if the current state is Unstaged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def completed(self, session: Session):
        if self.state and self.state.name != UNSTAGED_STATE:
            raise StateTransitionError(self.state.name, COMPLETED_STATE)

        self.update_state(session, COMPLETED_STATE)

    def update_state(self, session: Session, name: str):
        s = session.query(State).filter(State.name == name).one()
        self.state = s
        self.last_updated = func.now()
        session.flush()


def find_state(session: Session, name: str):
    return session.query(State).filter(State.name == name).one()


def find_work_type(session: Session, name: str):
    return session.query(WorkType).filter(WorkType.name == name).one()


def find_instrument_type(session: Session,
                         inst_manufacturer: str,
                         inst_model: str) -> InstrumentType:
    return session.query(InstrumentType).filter(
            InstrumentType.manufacturer == inst_manufacturer,
            InstrumentType.model == inst_model).one()


def initialize_database(session: Session):
    """Initializes the database dictionary tables

    Inserts values into the dictionary tables for instrument types, work
    types and work states."""
    _initialize_instruments(session)
    _initialize_worktypes(session)
    _initialize_states(session)
    session.commit()


def _initialize_states(session):
    states = [
        State(name=PENDING_STATE, desc="Pending any action"),
        State(name=STAGED_STATE, desc="The work data are staged"),
        State(name=FAILED_STAGING_STATE, desc="Staging has failed"),
        State(name=STARTED_STATE, desc="Work started"),
        State(name=SUCCEEDED_STATE, desc="Work was done successfully"),
        State(name=FAILED_STATE, desc="Work has failed"),
        State(name=CANCELLED_STATE, desc="Work was cancelled"),
        State(name=UNSTAGED_STATE, desc="The work data were unstaged"),
        State(name=FAILED_UNSTAGING_STATE, desc="Unstaging has failed"),
        State(name=COMPLETED_STATE, desc="All actions are complete")
    ]
    session.add_all(states)


def _initialize_worktypes(session):
    types = [
        WorkType(name=ARTIC_NEXTFLOW_WORKTYPE, desc="ARTIC NextFlow pipeline")
    ]
    session.add_all(types)


def _initialize_instruments(session):
    types = [
        InstrumentType(manufacturer=OXFORD_NANOPORE, model=GRIDION_MODEL),
        InstrumentType(manufacturer=OXFORD_NANOPORE, model=PROMETHION_MODEL)
    ]
    session.add_all(types)
