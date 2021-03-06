import os
import unittest
from datetime import datetime

import pytest
from pytest import mark as m

from tests.irods_fixture import irods_tmp_coll, baton_session
from tests.ml_warehouse_fixture import mlwh_session
from tests.schema_fixture import wb_session
from workbot.config import PENDING_STATE, CANCELLED_STATE
from workbot.irods import Collection, imkdir, iput
from workbot.ml_warehouse_schema import find_recent_experiment_pos
from workbot.schema import WorkInstance
from workbot.workbot import WorkBot, AnalysisError, add_ont_analyses

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
@m.it("Adds analyses for new ones in a PENDING state")
def test_add_new_analyses(mlwh_session, wb_session,
                          irods_tmp_coll, baton_session):
    start_date = datetime.fromisoformat("2020-06-16")

    p = os.path.join(irods_tmp_coll, "gridion", "66", "DN585561I_A1",
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

    num_added = add_ont_analyses(wb_session, baton_session, expts)
    assert num_added == 1  # Only one experiment has reached iRODS

    q = wb_session.query(WorkInstance).\
        filter(WorkInstance.input_path == p).all()
    assert len(q) == 1
    wi = q[0]
    assert wi.input_path == p
    assert wi.state.name == PENDING_STATE

    num_added = add_ont_analyses(wb_session, baton_session, expts)
    # One analysis exists, so another should not be added
    assert num_added == 0


@m.describe("Pre-analysis")
@m.context("When an analysis input is present")
@m.it("Is detected")
def test_is_input_path_present(wb_session, irods_tmp_coll):
    archive_root = "/dummy"
    staging_root = "/dummy"
    p = os.path.join(irods_tmp_coll, "dummy_input")

    wb = WorkBot(archive_root, staging_root)
    wi = wb.add_analysis(wb_session, p)

    assert not wb.is_input_path_present(wi)
    imkdir(p, make_parents=True)
    assert wb.is_input_path_present(wi)


@m.context("When analysis input data are complete")
@m.it("Is detected")
def test_is_input_data_complete(wb_session, irods_tmp_coll):
    archive_root = "/dummy"
    staging_root = "/dummy"
    p = os.path.join(irods_tmp_coll, "dummy_input")

    wb = WorkBot(archive_root, staging_root)
    wi = wb.add_analysis(wb_session, p)

    assert not wb.is_input_data_complete(wi)

    imkdir(p, make_parents=True)
    assert not wb.is_input_data_complete(wi)

    iput("tests/data/gridion/66/DN585561I_A1/"
         "20190904_1514_GA20000_FAL01979_43578c8f/final_report.txt.gz",
         os.path.join(p, "final_report.txt.gz"))
    assert wb.is_input_data_complete(wi)


@m.context("When analysis input data are staged")
@m.it("Is present in the staging input directory")
def test_stage_input_data(wb_session, irods_tmp_coll, tmp_path):
    archive_root = os.path.join(irods_tmp_coll, "archive")
    imkdir(archive_root, make_parents=True)
    staging_root = tmp_path / "staging"

    wb = WorkBot(archive_root, staging_root.as_posix())
    p = os.path.join(irods_tmp_coll, "gridion", "66", "DN585561I_A1",
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    wi = wb.add_analysis(wb_session, p)

    assert not wi.is_staged()
    wb.stage_input_data(wb_session, wi)
    assert wi.is_staged()

    # The run collection 20190904_1514_GA20000_FAL01979_43578c8f is the one
    # annotated with metadata in iRODS, so is the collection that gets staged
    staging_in_path = wb.staging_input_path(wi)

    assert staging_in_path == os.path.join(staging_root.as_posix(),
                                           str(wi.id),
                                           "input")
    expected_files = ["duty_time.csv",
                      "fast5_fail",
                      "fast5_pass",
                      "fastq_fail",
                      "fastq_pass",
                      "final_report.txt.gz",
                      "final_summary.txt",
                      "GXB02004_20190904_151413_FAL01979_gridion_"
                      "sequencing_run_DN585561I_A1_sequencing_summary.txt",
                      "report.md",
                      "report.pdf",
                      "throughput.csv"]
    for f in expected_files:
        assert os.path.exists(os.path.join(staging_in_path, f))


@m.describe("Running analyses")
@m.context("When an analysis is run")
@m.it("Writes to the staging output directory")
def test_run_analysis(wb_session, irods_tmp_coll, tmp_path):
    archive_root = os.path.join(irods_tmp_coll, "archive")
    imkdir(archive_root, make_parents=True)
    staging_root = tmp_path / "staging"

    wb = WorkBot(archive_root, staging_root.as_posix())
    p = os.path.join(irods_tmp_coll, "gridion", "66", "DN585561I_A1",
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    wi = wb.add_analysis(wb_session, p)
    wb.stage_input_data(wb_session, wi)

    assert not wi.is_succeeded()
    wb.run_analysis(wb_session, wi)
    assert wi.is_succeeded()

    staging_out_path = wb.staging_output_path(wi)
    assert staging_out_path == os.path.join(staging_root.as_posix(),
                                            str(wi.id),
                                            "output")

    expected_files = ["ncov2019-artic-nf-done"]
    for f in expected_files:
        assert os.path.exists(os.path.join(staging_out_path, f))


@m.describe("Post-analysis")
@m.context("When an analysis is archived")
@m.it("Writes to the archive collection")
def test_archive_output_data(wb_session, irods_tmp_coll, tmp_path):
    archive_root = os.path.join(irods_tmp_coll, "archive")
    imkdir(archive_root, make_parents=True)
    staging_root = tmp_path / "staging"

    wb = WorkBot(archive_root, staging_root.as_posix())
    p = os.path.join(irods_tmp_coll, "gridion", "66", "DN585561I_A1",
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    wi = wb.add_analysis(wb_session, p)
    wb.add_metadata(wb_session, wi, "experiment_01", 1)

    wb.stage_input_data(wb_session, wi)
    wb.run_analysis(wb_session, wi)

    assert not wi.is_archived()
    wb.archive_output_data(wb_session, wi)
    assert wi.is_archived()


@m.context("When an analysis is annotated")
@m.it("Adds metadata to the archive collection")
def test_annotate_output_data(wb_session, irods_tmp_coll, tmp_path,
                              baton_session):
    archive_root = os.path.join(irods_tmp_coll, "archive")
    imkdir(archive_root, make_parents=True)
    staging_root = tmp_path / "staging"

    wb = WorkBot(archive_root, staging_root.as_posix())
    p = os.path.join(irods_tmp_coll, "gridion", "66", "DN585561I_A1",
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    wi = wb.add_analysis(wb_session, p)

    expt = "experiment_01"
    pos = 1

    wb.add_metadata(wb_session, wi, expt, pos)
    wb.stage_input_data(wb_session, wi)
    wb.run_analysis(wb_session, wi)
    wb.archive_output_data(wb_session, wi)

    assert not wi.is_annotated()
    wb.annotate_output_data(wb_session, wi)
    assert wi.is_annotated()

    archive_path = wb.archive_path(wi)
    coll = Collection(baton_session, archive_path)
    assert {"attribute": "experiment_name",
            "value": expt} in coll.metadata()
    assert {"attribute": "instrument_slot",
            "value": str(pos)} in coll.metadata()


@m.context("When an analysis is unstaged")
@m.it("Removes the local staging directory")
def test_unstage_input_data(wb_session, irods_tmp_coll, tmp_path,
                            baton_session):
    archive_root = os.path.join(irods_tmp_coll, "archive")
    imkdir(archive_root, make_parents=True)
    staging_root = tmp_path / "staging"

    wb = WorkBot(archive_root, staging_root.as_posix())
    p = os.path.join(irods_tmp_coll, "gridion", "66", "DN585561I_A1",
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    wi = wb.add_analysis(wb_session, p)

    wb.stage_input_data(wb_session, wi)
    wb.run_analysis(wb_session, wi)
    wb.archive_output_data(wb_session, wi)
    wb.annotate_output_data(wb_session, wi)

    assert os.path.exists(wb.staging_path(wi))
    assert not wi.is_unstaged()
    wb.unstage_input_data(wb_session, wi)
    assert wi.is_unstaged()
    assert not os.path.exists(wb.staging_path(wi))


@m.it("Can be completed")
def test_complete_analysis(wb_session, irods_tmp_coll, tmp_path,
                           baton_session):
    archive_root = os.path.join(irods_tmp_coll, "archive")
    imkdir(archive_root, make_parents=True)
    staging_root = tmp_path / "staging"

    wb = WorkBot(archive_root, staging_root.as_posix())
    p = os.path.join(irods_tmp_coll, "gridion", "66", "DN585561I_A1",
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    wi = wb.add_analysis(wb_session, p)

    wb.stage_input_data(wb_session, wi)
    wb.run_analysis(wb_session, wi)
    wb.archive_output_data(wb_session, wi)
    wb.annotate_output_data(wb_session, wi)
    wb.unstage_input_data(wb_session, wi)

    assert not wi.is_completed()
    wb.complete_analysis(wb_session, wi)
    assert wi.is_completed()

