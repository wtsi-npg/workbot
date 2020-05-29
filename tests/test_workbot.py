import os
from datetime import datetime

import pytest
from pytest import mark as m

from tests.irods_fixture import irods_tmp_coll, baton_session
from tests.ml_warehouse_fixture import mlwh_session
from tests.schema_fixture import wb_session
from workbot.config import PENDING_STATE, CANCELLED_STATE
from workbot.irods import Collection
from workbot.ml_warehouse_schema import find_recent_experiment_pos
from workbot.schema import WorkInstance, State
from workbot.workbot import WorkBot, AnalysisError, add_new_analyses

#  Stop IDEs "optimizing" away these imports
_ = mlwh_session
_ = wb_session

_ = irods_tmp_coll
_ = baton_session


@m.describe("Finding analyses")
@m.context("When specific states are included")
@m.it("Includes the analysis")
def test_find_analyses_include(wb_session):
    input_path = "/seq/ont/gridion/experiment_01"
    archive_root = "/dummy"
    staging_root = "/dummy"

    wb = WorkBot(archive_root, staging_root)
    wi = wb.add_analysis(wb_session, input_path)
    wb_session.commit()

    assert wb.find_analyses(wb_session, input_path,
                            states=[PENDING_STATE]) == [wi]


@m.context("When specific states are excluded")
@m.it("Excludes the analysis")
def test_find_analyses_exclude(wb_session):
    input_path = "/seq/ont/gridion/experiment_01"
    archive_root = "/dummy"
    staging_root = "/dummy"

    wb = WorkBot(archive_root, staging_root)
    _ = wb.add_analysis(wb_session, input_path)
    wb_session.commit()

    assert wb.find_analyses(wb_session, input_path,
                            not_states=[PENDING_STATE]) == []


@m.describe("Adding analyses")
@m.context("When there is no existing analysis")
@m.it("Can be added")
def test_add_analysis(wb_session):
    input_path = "/seq/ont/gridion/experiment_01"
    archive_root = "/dummy"
    staging_root = "/dummy"

    wb = WorkBot(archive_root, staging_root)
    analyses = wb.find_analyses(wb_session, input_path)
    assert analyses == []

    wi = wb.add_analysis(wb_session, input_path)
    assert wi.input_path == input_path
    assert wi.state.name == PENDING_STATE

    analyses = wb.find_analyses(wb_session, input_path)
    assert len(analyses) == 1
    analysis = analyses[0]
    assert analysis.state.name == PENDING_STATE
    assert analysis.input_path == input_path


@m.context("When there is an existing analysis")
@m.it("Cannot be added")
def test_add_analysis_existing(wb_session):
    input_path = "/seq/ont/gridion/experiment_01"
    archive_root = "/dummy"
    staging_root = "/dummy"

    wb = WorkBot(archive_root, staging_root)
    wb.add_analysis(wb_session, input_path)

    with pytest.raises(AnalysisError, match="analyses already exist"):
        wb.add_analysis(wb_session, input_path)


@m.context("When an existing analysis is cancelled")
@m.it("Can be added")
def test_add_analysis_cancelled(wb_session):
    input_path = "/seq/ont/gridion/experiment_01"
    archive_root = "/dummy"
    staging_root = "/dummy"

    wb = WorkBot(archive_root, staging_root)
    wi1 = wb.add_analysis(wb_session, input_path)
    wi1.cancelled(wb_session)
    wb_session.commit()

    analyses = wb.find_analyses(wb_session, input_path,
                                not_states=[CANCELLED_STATE])
    assert analyses == []

    wi2 = wb.add_analysis(wb_session, input_path)
    assert wi2.input_path == input_path
    assert wi2.state.name == PENDING_STATE


@m.context("When ONT experiments are found")
@m.it("Adds analyses for new ones")
def test_add_new_analyses(mlwh_session, wb_session,
                          irods_tmp_coll, baton_session):
    start_date = datetime.fromisoformat("2020-06-16")

    p = os.path.join(irods_tmp_coll, "gridion/66/DN585561I_A1/"
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    coll = Collection(baton_session, p)
    coll.meta_add({"attribute": "experiment_name",
                   "value": "multiplexed_experiment_001"},
                  {"attribute": "instrument_slot",
                   "value": "1"})

    expts = find_recent_experiment_pos(mlwh_session, start_date)
    assert expts == [('multiplexed_experiment_001', 1),
                     ('multiplexed_experiment_001', 3),
                     ('multiplexed_experiment_001', 5),
                     ('multiplexed_experiment_003', 1),
                     ('multiplexed_experiment_003', 3),
                     ('multiplexed_experiment_003', 5)]

    num_added = add_new_analyses(wb_session, baton_session, expts)
    assert num_added == 1  # Only one experiment has reached iRODS

    q = wb_session.query(WorkInstance).\
        filter(WorkInstance.input_path == p).all()
    assert len(q) == 1
    wi = q[0]
    assert wi.input_path == p
    assert wi.state.name == PENDING_STATE

    num_added = add_new_analyses(wb_session, baton_session, expts)
    # One analysis exists, so another should not be added
    assert num_added == 0
