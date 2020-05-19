import operator

from sqlalchemy import Column, ForeignKey, create_engine
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session, sessionmaker
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
        return "<State: name={}>".format(self.name)


def find_state(session: Session, name: str):
    return session.query(State).filter(State.name == name).one()


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


def find_work_type(session: Session, name: str):
    return session.query(WorkType).filter(WorkType.name == name).one()


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
        tmpl = "<InstrumentType: manufacturer={}, model={}>"
        return tmpl.format(self.manufacturer, self.model)


def find_instrument_type(session: Session,
                         inst_manufacturer: str,
                         inst_model: str) -> InstrumentType:
    return session.query(InstrumentType).filter(
            InstrumentType.manufacturer == inst_manufacturer,
            InstrumentType.model == inst_model).one()


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

    instance_states = relationship("InstanceState", backref="instance",
                                   lazy="joined", cascade="all, delete-orphan")
    states = association_proxy('instance_states', 'state')

    def __init__(self,
                 inst_type: InstrumentType,
                 inst_position: int,
                 expt_name: str,
                 work_type: WorkType):
        self.instrument_type = inst_type
        self.instrument_position = inst_position
        self.experiment_name = expt_name
        self.work_type = work_type

    def __repr__(self):
        tmpl = "<WorkInstance: id={}, instrument={}, work_type={}, state={}>"
        return tmpl.format(self.id, self.instrument_type, self.work_type.name,
                           self.state())

    """Return the current state."""
    def state(self) -> State:
        if not self.states:
            return None

        # There may be a better way of doing this. As state associations are
        # ordered by their rank on the link instance_states table, the
        # latest i.e. current state of an instance, is the one that sorts last
        # by rank.
        istates = self.instance_states
        istates.sort(key=operator.attrgetter('rank'), reverse=True)
        return istates[0].state

    """Changes the current state to Pending.
    
    Changes state to Pending, if the current state is None.
    
    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def pending(self, session: Session):
        if self.state() is not None:
            raise StateTransitionError(self.state().name, PENDING_STATE)

        self.update_state(session, PENDING_STATE)

    """Changes the current state to Staged.

    Changes the state, if the current state is Pending.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def staged(self, session: Session):
        if self.state() is None or self.state().name != PENDING_STATE:
            raise StateTransitionError(self.state().name, STAGED_STATE)

        self.update_state(session, STAGED_STATE)

    """Changes the current state to Failed Staging.

    Changes state to Failed Staging, if the current state is Pending.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def failed_staging(self, session: Session):
        if self.state() is None or self.state().name != PENDING_STATE:
            raise StateTransitionError(self.state().name, FAILED_STAGING_STATE)

        self.update_state(session, FAILED_STAGING_STATE)

    """Changes the current state to Started.

    Changes state to Started, if the current state is Staged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def started(self, session: Session):
        if self.state() is None or self.state().name != STAGED_STATE:
            raise StateTransitionError(self.state().name, STARTED_STATE)

        self.update_state(session, STARTED_STATE)

    """Changes the current state to Succeeded.

    Changes state to Succeeded, if the current state is Started.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def succeeded(self, session: Session):
        if self.state() is None or self.state().name != STARTED_STATE:
            raise StateTransitionError(self.state().name, SUCCEEDED_STATE)

        self.update_state(session, SUCCEEDED_STATE)

    """Changes the current state to Failed.

    Changes state to Failed, if the current state is Started.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def failed(self, session: Session):
        if self.state() is None:
            raise StateTransitionError(None, UNSTAGED_STATE)

        if self.state().name != STARTED_STATE:
            raise StateTransitionError(self.state().name, FAILED_STATE)

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
        if self.state() is None or \
                self.state().name not in [STAGED_STATE,
                                          FAILED_STAGING_STATE,
                                          SUCCEEDED_STATE,
                                          FAILED_STATE,
                                          FAILED_UNSTAGING_STATE]:
            raise StateTransitionError(self.state().name, UNSTAGED_STATE)

        self.update_state(session, UNSTAGED_STATE)

    """Changes the current state to Failed Unstaging.

    Changes state to Failed Unstaging, if the current state is Staged,
    Succeeded or Failed.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def failed_unstaging(self, session: Session):
        if self.state() is None or self.state().name not in [STAGED_STATE,
                                                             SUCCEEDED_STATE,
                                                             FAILED_STATE]:
            raise StateTransitionError(self.state().name,
                                       FAILED_UNSTAGING_STATE)

        self.update_state(session, FAILED_UNSTAGING_STATE)

    """Changes the current state to Completed.

    Changes state to Completed, if the current state is Unstaged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """
    def completed(self, session: Session):
        if self.state() and self.state().name != UNSTAGED_STATE:
            raise StateTransitionError(self.state().name, COMPLETED_STATE)

        self.update_state(session, COMPLETED_STATE)

    def update_state(self, session: Session, name: str):
        s = session.query(State).filter(State.name == name).one()
        self.states.append(s)
        session.flush()


class InstanceState(WorkBotDBBase):
    __tablename__ = 'instance_state'

    rank = Column(Integer, autoincrement=True, primary_key=True)
    time = Column(DateTime(timezone=True), nullable=False, default=func.now())

    instance_id = Column(Integer, ForeignKey('workinstance.id'),
                         nullable=False)

    state_id = Column(Integer, ForeignKey('state.id'), nullable=False)
    state = relationship("State", lazy="joined")

    def __init__(self,
                 state: State):
        self.state = state

    def __repr__(self):
        tmpl = "<InstanceState: rank={} instance={}, state={}, time={}>"
        return tmpl.format(self.rank, self.instance_id, self.state, self.time)


def initialize_database(*args, **kwargs):
    engine = create_engine(*args, **kwargs)
    WorkBotDBBase.metadata.create_all(engine)

    session_maker = sessionmaker(bind=engine)
    sess = session_maker()

    initialize_instruments(sess)
    initialize_worktypes(sess)
    initialize_states(sess)

    sess.commit()
    sess.close()

    return True


def initialize_states(session):
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


def initialize_worktypes(session):
    types = [
        WorkType(name=ARTIC_NEXTFLOW_WORKTYPE, desc="ARTIC NextFlow pipeline")
    ]
    session.add_all(types)


def initialize_instruments(session):
    types = [
        InstrumentType(manufacturer=OXFORD_NANOPORE, model=GRIDION_MODEL),
        InstrumentType(manufacturer=OXFORD_NANOPORE, model=PROMETHION_MODEL)
    ]
    session.add_all(types)
