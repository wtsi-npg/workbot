import pytest
from pytest import mark as m

from tests.schema_fixture import wb_session
from workbot.config import ARTIC_NEXTFLOW_WORKTYPE, PENDING_STATE, \
    STAGED_STATE, \
    STARTED_STATE, UNSTAGED_STATE, \
    SUCCEEDED_STATE, FAILED_STATE, COMPLETED_STATE, CANCELLED_STATE, \
    ARCHIVED_STATE, ANNOTATED_STATE
from workbot.schema import StateTransitionError, WorkInstance, \
    find_work_type, find_state

#  Stop IDEs "optimizing" away these imports
_ = wb_session


@m.describe("WorkInstance state transitions")
@m.context("When pending")
@m.it("Can be staged")
def test_pending_to_staged(wb_session):
    wi = make_instance(wb_session)
    wi.staged(wb_session)
    assert wi.state.name == STAGED_STATE


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
        wi.archived(wb_session)

    with pytest.raises(StateTransitionError):
        wi.annotated(wb_session)

    with pytest.raises(StateTransitionError):
        wi.unstaged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed(wb_session)


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


@m.it("Raises exceptions on invalid transitions from staged")
def test_staged_transition_except(wb_session):
    wi = make_staged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.succeeded(wb_session)

    with pytest.raises(StateTransitionError):
        wi.archived(wb_session)

    with pytest.raises(StateTransitionError):
        wi.annotated(wb_session)

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


@m.it("Can be failed")
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
        wi.archived(wb_session)

    with pytest.raises(StateTransitionError):
        wi.annotated(wb_session)

    with pytest.raises(StateTransitionError):
        wi.unstaged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When succeeded")
@m.it("Can be archived")
def test_succeeded_to_archived(wb_session):
    wi = make_succeeded(wb_session)
    wi.archived(wb_session)
    assert wi.state.name == ARCHIVED_STATE


@m.it("Raises exceptions on invalid transitions from succeeded")
def test_succeeded_transition_except(wb_session):
    wi = make_succeeded(wb_session)

    with pytest.raises(StateTransitionError):
        wi.started(wb_session)

    with pytest.raises(StateTransitionError):
        wi.annotated(wb_session)

    with pytest.raises(StateTransitionError):
        wi.unstaged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When archived")
@m.it("Can be annotated")
def test_archived_to_annotated(wb_session):
    wi = make_archived(wb_session)
    wi.annotated(wb_session)
    assert wi.state.name == ANNOTATED_STATE


@m.it("Raises exceptions on invalid transitions from archived")
def test_succeeded_transition_except(wb_session):
    wi = make_archived(wb_session)

    with pytest.raises(StateTransitionError):
        wi.started(wb_session)

    with pytest.raises(StateTransitionError):
        wi.unstaged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.failed(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


@m.context("When it has been annotated")
@m.it("Can be unstaged")
def test_annotated_to_unstaged(wb_session):
    wi = make_annotated(wb_session)
    wi.unstaged(wb_session)
    assert wi.state.name == UNSTAGED_STATE


@m.context("When it has been unstaged")
@m.it("Can be completed")
def test_unstaged_to_completed(wb_session):
    wi = make_unstaged(wb_session)
    wi.completed(wb_session)
    assert wi.state.name == COMPLETED_STATE


@m.context("When it has failed")
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
        wi.unstaged(wb_session)

    with pytest.raises(StateTransitionError):
        wi.completed(wb_session)


def make_instance(session):
    artic_nf = find_work_type(session, ARTIC_NEXTFLOW_WORKTYPE)
    pending = find_state(session, PENDING_STATE)
    input_path = "/seq/ont/gridion/experiment_1"
    wi = WorkInstance(input_path, artic_nf, pending)
    session.add(wi)
    return wi


def make_staged(session):
    wi = make_instance(session)
    wi.staged(session)
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


def make_archived(session):
    wi = make_succeeded(session)
    wi.archived(session)
    session.commit()
    return wi


def make_annotated(session):
    wi = make_archived(session)
    wi.annotated(session)
    session.commit()
    return wi


def make_unstaged(session):
    wi = make_annotated(session)
    wi.unstaged(session)
    session.commit()
    return wi


def make_failed(session):
    wi = make_started(session)
    wi.failed(session)
    session.commit()
    return wi


def make_completed(session):
    wi = make_unstaged(session)
    wi.completed(session)
    session.commit()
    return wi
