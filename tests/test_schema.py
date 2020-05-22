import pytest
from pytest import mark as m

from tests.schema_fixture import wb_session
from workbot.config import OXFORD_NANOPORE, GRIDION_MODEL, \
    ARTIC_NEXTFLOW_WORKTYPE, STAGED_STATE, FAILED_STAGING_STATE, \
    CANCELLED_STATE, STARTED_STATE, UNSTAGED_STATE, FAILED_UNSTAGING_STATE, \
    SUCCEEDED_STATE, FAILED_STATE, COMPLETED_STATE, PENDING_STATE
from workbot.schema import StateTransitionError, WorkInstance, \
    find_instrument_type, find_work_type, find_state

_ = wb_session

# @pytest.fixture(scope="module")
# def initialized_db_session():
#     sess = None
#
#     def _make_session_and_initialize(uri):
#         nonlocal sess
#
#         engine = create_engine(uri, echo=False)
#         Base.metadata.create_all(engine)
#
#         session_maker = sessionmaker(bind=engine)
#         sess = session_maker()
#
#         initialize_database(uri, echo=False)
#
#         return sess
#
#     yield _make_session_and_initialize
#
#     sess.close()


@m.describe("WorkInstance state transitions")
@m.context("When pending")
@m.it("Can be staged")
def test_pending_to_staged(wb_session):
    wi = make_instance(wb_session)
    wi.staged(wb_session)
    assert wi.state.name == STAGED_STATE


@m.it("Can fail staging")
def test_pending_to_failed_staging(wb_session):
    wi = make_instance(wb_session)
    wi.failed_staging(wb_session)
    assert wi.state.name == FAILED_STAGING_STATE


@m.it("Can be cancelled")
def test_pending_to_cancelled(wb_session):
    wi = make_instance(wb_session)
    wi.cancelled(wb_session)
    assert wi.state.name == CANCELLED_STATE


@m.it("Raises exceptions on invalid transitions")
def test_pending_transition_except(wb_session):
    wi = make_instance(wb_session)

    with pytest.raises(StateTransitionError):
        wi.started(wb_session)

    with pytest.raises(StateTransitionError):
        wi.succeeded(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.unstaged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed_unstaging(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When staged")
@m.it("Can be started")
def test_staged_to_started(wb_session):
    wi = make_staged(wb_session)
    wi.started(wb_session)
    assert wi.state.name == STARTED_STATE


@m.it("Can be unstaged")
def test_staged_to_unstaged(wb_session):
    wi = make_staged(wb_session)
    wi.unstaged(wb_session)
    assert wi.state.name == UNSTAGED_STATE


@m.it("Can fail unstaging")
def test_staged_to_failed_unstaging(wb_session):
    wi = make_staged(wb_session)
    wi.failed_unstaging(wb_session)
    assert wi.state.name == FAILED_UNSTAGING_STATE


@m.it("Raises exceptions on invalid transitions from staged")
def test_staged_transition_except(wb_session):
    wi = make_staged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.succeeded(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When started")
@m.it("Can succeed")
def test_started_to_succeeded(wb_session):
    wi = make_started(wb_session)
    wi.succeeded(wb_session)
    assert wi.state.name == SUCCEEDED_STATE


@m.it("Can fail")
def test_started_to_failed(wb_session):
    wi = make_started(wb_session)
    wi.failed(wb_session)
    assert wi.state.name == FAILED_STATE


@m.it("Can be cancelled")
def test_started_to_cancelled(wb_session):
    wi = make_started(wb_session)
    wi.cancelled(wb_session)
    assert wi.state.name == CANCELLED_STATE


@m.it("Raises exceptions on invalid transitions from started")
def test_started_transition_except(wb_session):
    wi = make_started(wb_session)

    with pytest.raises(StateTransitionError):
        wi.started(wb_session)

    with pytest.raises(StateTransitionError):
        wi.unstaged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed_unstaging(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When it has succeeded")
@m.it("Can be unstaged")
def test_succeeded_to_unstaged(wb_session):
    wi = make_succeeded(wb_session)
    wi.unstaged(wb_session)
    assert wi.state.name == UNSTAGED_STATE


@m.it("Can fail unstaging")
def test_succeeded_to_failed_unstaging(wb_session):
    wi = make_succeeded(wb_session)
    wi.failed_unstaging(wb_session)
    assert wi.state.name == FAILED_UNSTAGING_STATE


@m.it("Raises exceptions on invalid transitions from succeeded")
def test_succeeded_transition_except(wb_session):
    wi = make_succeeded(wb_session)

    with pytest.raises(StateTransitionError):
        wi.started(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When it has failed")
@m.it("Can be unstaged")
def test_failed_to_unstaged(wb_session):
    wi = make_failed(wb_session)
    wi.unstaged(wb_session)
    assert wi.state.name == UNSTAGED_STATE


@m.it("Can fail unstaging")
def test_failed_to_failed_unstaging(wb_session):
    wi = make_failed(wb_session)
    wi.failed_unstaging(wb_session)
    assert wi.state.name == FAILED_UNSTAGING_STATE


@m.it("Can be cancelled")
def test_failed_to_cancelled(wb_session):
    wi = make_failed(wb_session)
    wi.cancelled(wb_session)
    assert wi.state.name == CANCELLED_STATE


@m.it("Raises exceptions on invalid transitions from failed")
def test_failed_transition_except(wb_session):
    wi = make_failed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.started(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When it has been unstaged")
@m.it("Can be completed")
def test_unstaged_to_completed(wb_session):
    wi = make_unstaged(wb_session)
    wi.completed(wb_session)
    assert wi.state.name == COMPLETED_STATE


@m.context("When it has failed unstaging")
@m.it("Can fail unstaging again")
def test_failed_unstaging_to_unstaged(wb_session):
    wi = make_failed_unstaging(wb_session)
    assert wi.state.name == FAILED_UNSTAGING_STATE
    wi.unstaged(wb_session)
    assert wi.state.name == UNSTAGED_STATE


def make_instance(session):
    gridion = find_instrument_type(session, OXFORD_NANOPORE, GRIDION_MODEL)
    expt_name = "my_experiment"
    inst_position = 1

    artic_nf = find_work_type(session, ARTIC_NEXTFLOW_WORKTYPE)
    pending = find_state(session, PENDING_STATE)
    wi = WorkInstance(gridion, inst_position, expt_name, artic_nf, pending)
    session.add(wi)
    return wi

def make_staged(session):
    wi = make_instance(session)
    wi.staged(session)
    session.commit()
    return wi


def make_failed_staging(session):
    wi = make_staged(session)
    wi.failed_staging(session)
    session.commit()
    return wi


def make_started(session):
    wi = make_staged(session)
    wi.started(session)
    session.commit()
    return wi


def make_succeeded(session):
    wi = make_started(session)
    wi.succeeded(session)
    session.commit()
    return wi


def make_failed(session):
    wi = make_started(session)
    wi.failed(session)
    session.commit()
    return wi


def make_unstaged(session):
    wi = make_succeeded(session)
    wi.unstaged(session)
    session.commit()
    return wi


def make_failed_unstaging(session):
    wi = make_succeeded(session)
    wi.failed_unstaging(session)
    session.commit()
    return wi


def make_completed(session):
    wi = make_unstaged(session)
    wi.completed(session)
    session.commit()
    return wi
