import os
from datetime import datetime
from pathlib import Path, PurePath

import pytest
from pytest import mark as m

from tests.irods_fixture import baton_session, irods_gridion, irods_synthetic
from tests.ml_warehouse_fixture import mlwh_session
from tests.schema_fixture import wb_session
from workbot.base import AnalysisError
from workbot.enums import WorkState, WorkType
from workbot.irods import AVU, Collection, imkdir, iput
from workbot.ml_warehouse_schema import find_recent_ont_pos
from workbot.ont import ONTRunDataWorkBot, ONTWorkBroker
from workbot.schema import WorkInstance

#  Stop IDEs "optimizing" away these imports
_ = mlwh_session
_ = wb_session

_ = irods_gridion
_ = irods_synthetic
_ = baton_session


@m.describe("ONTRunDataWorkBot")
class TestONTRunDataWorkBot(object):
    @m.context("When created")
    @m.it("Has the correct work type")
    def test_make_workbot_ont_run_data_worktype(self):
        work_type = WorkType.ARTICNextflow.name
        assert ONTRunDataWorkBot(work_type).work_type == work_type

    @m.context("When a work type is set")
    @m.it("Appears in its compatible worktypes")
    def test_make_compatible_worktypes(self):
        work_type = WorkType.ARTICNextflow.name
        assert work_type in ONTRunDataWorkBot(work_type). \
            compatible_work_types()

    @m.it("Can be requeued until cancelled or completed")
    def test_make_workbot_ont_run_data_endstate(self):
        work_type = WorkType.ARTICNextflow.name
        assert ONTRunDataWorkBot(work_type). \
                   end_states == [WorkState.CANCELLED, WorkState.COMPLETED]

    @m.context("When ONT experiments are found")
    @m.it("Adds analyses for new ones in a PENDING state")
    def test_add_new_analyses(self, mlwh_session, wb_session,
                              irods_synthetic, baton_session):
        start_date = datetime.fromisoformat("2020-06-16")

        p = PurePath(irods_synthetic, "multiplexed_experiment_001",
                     "20190904_1514_GA10000_flowcell101_cf751ba1")

        # Check precondition of test
        expts = find_recent_ont_pos(mlwh_session, start_date)
        assert expts == [('multiplexed_experiment_001', 1),
                         ('multiplexed_experiment_001', 3),
                         ('multiplexed_experiment_001', 5),
                         ('multiplexed_experiment_003', 1),
                         ('multiplexed_experiment_003', 3),
                         ('multiplexed_experiment_003', 5)]

        br = ONTWorkBroker(ONTRunDataWorkBot(WorkType.ARTICNextflow.name))
        num_added = br.request_work(wb_session=wb_session,
                                    mlwh_session=mlwh_session,
                                    start_date=start_date)
        assert num_added == 1  # Only one experiment has reached iRODS

        wis = wb_session.query(WorkInstance).all()
        assert len(wis) == 1
        assert wis[0].input_path == p
        assert wis[0].state.name == WorkState.PENDING

        # One analysis exists, so another should not be added
        num_added = br.request_work(wb_session=wb_session,
                                    mlwh_session=mlwh_session,
                                    start_date=start_date)
        assert num_added == 0

    @m.context("When ONT analysis input data are complete")
    @m.it("Is detected")
    def test_is_ont_input_data_complete(self, wb_session, irods_gridion):
        archive_root = "/dummy"
        staging_root = "/dummy"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        p = Path(irods_gridion, "dummy_input")
        wi = wb.add_work(wb_session, p)

        assert not wb.is_input_data_complete(wi)
        imkdir(p, make_parents=True)
        assert not wb.is_input_data_complete(wi)

        iput("tests/data/gridion/66/DN585561I_A1/"
             "20190904_1514_GA20000_FAL01979_43578c8f/final_report.txt.gz",
             Path(p, "final_report.txt.gz"))
        assert wb.is_input_data_complete(wi)

    @m.context("When analysis input data are staged")
    @m.it("Is present in the staging input directory")
    def test_stage_input_data(self, wb_session, irods_gridion, tmp_path):
        archive_root = Path(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        p = Path(irods_gridion, "66", "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)

        assert not wi.is_staged()
        wb.stage_input_data(wb_session, wi)
        assert wi.is_staged()

        # The run collection 20190904_1514_GA20000_FAL01979_43578c8f is the one
        # annotated with metadata in iRODS, so is the collection that gets
        # staged
        staging_in_path = wb.staging_input_path(wi)
        assert staging_in_path == Path(staging_root, str(wi.id), "input")

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
            assert Path(staging_in_path, f).exists()

    @m.describe("Running analyses")
    @m.context("When an ONT analysis is run")
    @m.it("Writes to the staging output directory")
    def test_run_analysis(self, wb_session, irods_gridion, tmp_path):
        archive_root = Path(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        p = Path(irods_gridion, "66", "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)
        wb.stage_input_data(wb_session, wi)

        assert not wi.is_succeeded()
        wb.run_analysis(wb_session, wi)
        assert wi.is_succeeded()

        staging_out_path = wb.staging_output_path(wi)
        assert staging_out_path == Path(staging_root, str(wi.id), "output")

        expected_files = ["ncov2019-artic-nf-done"]
        for f in expected_files:
            assert Path(os.path.join(staging_out_path, f)).exists()

    @m.describe("Post-analysis")
    @m.context("When an ONT analysis is archived")
    @m.it("Writes to the archive collection")
    def test_archive_output_data(self, wb_session, irods_gridion, tmp_path):
        archive_root = Path(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        p = Path(irods_gridion, "66", "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)
        wb.add_metadata(wb_session, wi,
                        experiment_name="experiment_01",
                        instrument_position=1)

        wb.stage_input_data(wb_session, wi)
        wb.run_analysis(wb_session, wi)

        assert not wi.is_archived()
        wb.archive_output_data(wb_session, wi)
        assert wi.is_archived()

    @m.context("When an ONT analysis is annotated")
    @m.it("Adds metadata to the archive collection")
    def test_annotate_output_data(self, wb_session, irods_gridion, tmp_path,
                                  baton_session):
        archive_root = Path(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        expt = "66"
        pos = 2

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        p = Path(irods_gridion, expt, "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)
        wb.add_metadata(wb_session, wi,
                        experiment_name=expt,
                        instrument_position=pos)
        wb.stage_input_data(wb_session, wi)
        wb.run_analysis(wb_session, wi)
        wb.archive_output_data(wb_session, wi)

        assert not wi.is_annotated()
        wb.annotate_output_data(wb_session, wi)
        assert wi.is_annotated()

        archive_path = wb.archive_path(wi)
        coll = Collection(baton_session, archive_path)
        assert AVU("experiment_name", expt, namespace="ont") in coll.metadata()
        assert AVU("instrument_slot", pos, namespace="ont") in coll.metadata()

    @m.context("When an ONT analysis is unstaged")
    @m.it("Removes the local staging directory")
    def test_unstage_input_data(self, wb_session, irods_gridion, tmp_path,
                                baton_session):
        archive_root = Path(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        p = Path(irods_gridion, "66", "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)

        wb.stage_input_data(wb_session, wi)
        wb.run_analysis(wb_session, wi)
        wb.archive_output_data(wb_session, wi)
        wb.annotate_output_data(wb_session, wi)

        assert wb.staging_path(wi).exists()
        assert not wi.is_unstaged()
        wb.unstage_input_data(wb_session, wi)
        assert wi.is_unstaged()
        assert not wb.staging_path(wi).exists()

    @m.it("Can be completed")
    def test_complete_analysis(self, wb_session, irods_gridion, tmp_path,
                               baton_session):
        archive_root = os.path.join(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        p = Path(irods_gridion, "66", "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)

        wb.stage_input_data(wb_session, wi)
        wb.run_analysis(wb_session, wi)
        wb.archive_output_data(wb_session, wi)
        wb.annotate_output_data(wb_session, wi)
        wb.unstage_input_data(wb_session, wi)

        assert not wi.is_completed()
        wb.complete_analysis(wb_session, wi)
        assert wi.is_completed()

    @m.context("When an ONT analysis is completed")
    @m.it("Cannot be re-run")
    def test_rerun_completed_analysis(self, wb_session, irods_gridion,
                                      tmp_path, baton_session):
        archive_root = os.path.join(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        assert wb.end_states == [WorkState.CANCELLED, WorkState.COMPLETED]

        p = Path(irods_gridion, "66", "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)
        wb.run(wb_session, wi)

        assert wi.is_completed()
        with pytest.raises(AnalysisError, match="analyses already exist"):
            wb.add_work(wb_session, p)

    @m.context("When an ONT analysis is cancelled")
    @m.it("Cannot be re-run")
    def test_rerun_cancelled_analysis(self, wb_session, irods_gridion,
                                      tmp_path, baton_session):
        archive_root = os.path.join(irods_gridion, "archive")
        imkdir(archive_root, make_parents=True)
        staging_root = tmp_path / "staging"

        wb = ONTRunDataWorkBot(WorkType.ARTICNextflow.name,
                               archive_root=archive_root,
                               staging_root=staging_root)
        assert wb.end_states == [WorkState.CANCELLED, WorkState.COMPLETED]

        p = Path(irods_gridion, "66", "DN585561I_A1",
                 "20190904_1514_GA20000_FAL01979_43578c8f")
        wi = wb.add_work(wb_session, p)
        wb.cancel_analysis(wb_session, wi)

        assert wi.is_cancelled()
        with pytest.raises(AnalysisError, match="analyses already exist"):
            wb.add_work(wb_session, p)
