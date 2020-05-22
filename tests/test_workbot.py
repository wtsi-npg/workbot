from datetime import datetime

import pytest
from pytest import mark as m

from tests.ml_warehouse_fixture import mlwh_session
from tests.schema_fixture import wb_session
from workbot.config import GRIDION_MODEL, PENDING_STATE, CANCELLED_STATE
from workbot.ml_warehouse_schema import find_recent_experiment_pos
from workbot.schema import WorkInstance, State
from workbot.workbot import ONTWorkBot, AnalysisError, add_ont_analyses

#  Stop IDEs "optimizing" away these imports
_ = mlwh_session
_ = wb_session


@m.describe("Finding analyses")
@m.context("When specific states are included")
@m.it("Includes the analysis")
def test_find_analyses_include(wb_session):
    wb = ONTWorkBot(GRIDION_MODEL, 1, 'multiplexed_experiment_001')
    wi = wb.add_analysis(wb_session)
    wb_session.commit()

    assert wb.find_analyses(wb_session, states=[PENDING_STATE]) == [wi]


@m.context("When specific states are excluded")
@m.it("Excludes the analysis")
def test_find_analyses_exclude(wb_session):
    wb = ONTWorkBot(GRIDION_MODEL, 1, 'multiplexed_experiment_001')
    _ = wb.add_analysis(wb_session)
    wb_session.commit()

    assert wb.find_analyses(wb_session, not_states=[PENDING_STATE]) == []


@m.describe("Adding analyses")
@m.context("When there is no existing analysis")
@m.it("Can be added")
def test_add_analysis(wb_session):
    wb = ONTWorkBot(GRIDION_MODEL, 1, 'multiplexed_experiment_001')
    analyses = wb.find_analyses(wb_session)
    assert analyses == []

    wi = wb.add_analysis(wb_session)
    assert wi.experiment_name == 'multiplexed_experiment_001'
    assert wi.state.name == PENDING_STATE

    analyses = wb.find_analyses(wb_session)
    assert len(analyses) == 1
    analysis = analyses[0]
    assert analysis.state.name == PENDING_STATE
    assert analysis.experiment_name == 'multiplexed_experiment_001'
    assert analysis.instrument_position == 1


@m.context("When there is an existing analysis")
@m.it("Cannot be added")
def test_add_analysis_existing(wb_session):
    wb = ONTWorkBot(GRIDION_MODEL, 1, 'multiplexed_experiment_001')
    wb.add_analysis(wb_session)

    with pytest.raises(AnalysisError, match="analyses already exist"):
        wb.add_analysis(wb_session)


@m.context("When an existing analysis is cancelled")
@m.it("Can be added")
def test_add_analysis_cancelled(wb_session):
    wb = ONTWorkBot(GRIDION_MODEL, 1, 'multiplexed_experiment_001')
    wi1 = wb.add_analysis(wb_session)
    wi1.cancelled(wb_session)
    wb_session.commit()

    analyses = wb.find_analyses(wb_session, not_states=[CANCELLED_STATE])
    assert analyses == []

    wi2 = wb.add_analysis(wb_session)
    assert wi2.experiment_name == 'multiplexed_experiment_001'
    assert wi2.state.name == PENDING_STATE


@m.context("When ONT experiments are found")
@m.it("Adds analyses for new ones")
def test_add_new_analyses(mlwh_session, wb_session):
    start_date = datetime.fromisoformat("2020-06-16")

    expts = find_recent_experiment_pos(mlwh_session, start_date)
    assert expts == [('multiplexed_experiment_001', 1),
                     ('multiplexed_experiment_001', 3),
                     ('multiplexed_experiment_001', 5),
                     ('multiplexed_experiment_003', 1),
                     ('multiplexed_experiment_003', 3),
                     ('multiplexed_experiment_003', 5)]

    num_added = add_ont_analyses(wb_session, GRIDION_MODEL, expts)
    assert num_added == len(expts)

    for expt, pos in expts:
        q = wb_session.query(WorkInstance).\
            filter(WorkInstance.experiment_name == expt,
                   WorkInstance.instrument_position == pos).all()
        assert len(q) == 1
        wi = q[0]
        assert wi.experiment_name == expt
        assert wi.instrument_position == pos
        assert wi.state.name == PENDING_STATE

    num_added = add_ont_analyses(wb_session, GRIDION_MODEL, expts)
    assert num_added == 0
