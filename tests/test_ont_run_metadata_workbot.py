from datetime import datetime
from pathlib import PurePath

import pytest
from pytest import mark as m

from tests.irods_fixture import baton_session, irods_synthetic
from tests.ml_warehouse_fixture import mlwh_session
from tests.schema_fixture import wb_session
from workbot.base import AnalysisError
from workbot.enums import WorkState, WorkType
from workbot.irods import AVU, Collection
from workbot.ml_warehouse_schema import find_recent_ont_pos
from workbot.ont import ONTRunMetadataWorkBot, ONTWorkBroker
from workbot.schema import WorkInstance

#  Stop IDEs "optimizing" away these imports
_ = mlwh_session
_ = wb_session

_ = irods_synthetic
_ = baton_session


@m.describe("ONTRunMetadataWorkBot")
class TestONTRunMetadataWorkBot(object):
    @m.context("When created")
    @m.it("Can be re-queued until cancelled")
    def test_make_workbot_ont_run_data_endstate(self):
        work_type = WorkType.ONTRunMetadataUpdate.name
        assert ONTRunMetadataWorkBot(work_type). \
            end_states == [WorkState.CANCELLED]

    @m.context("When a work type is set")
    @m.it("Appears in its compatible worktypes")
    def test_make_compatible_worktypes(self):
        work_type = WorkType.ONTRunMetadataUpdate.name
        assert work_type in ONTRunMetadataWorkBot(work_type). \
            compatible_work_types()

    @m.context("When ONT experiments are found")
    @m.it("Adds analyses for new ones in a PENDING state")
    def test_add_new_updates(self, mlwh_session, wb_session,
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

        wt = WorkType.ONTRunMetadataUpdate.name
        br = ONTWorkBroker(ONTRunMetadataWorkBot(wt))
        num_added = br.request_work(wb_session=wb_session,
                                    mlwh_session=mlwh_session,
                                    start_date=start_date)
        assert num_added == 1  # Only one experiment has reached iRODS

        wis = wb_session.query(WorkInstance).all()
        assert len(wis) == 1
        assert wis[0].input_path == p
        assert wis[0].state.name == WorkState.PENDING

        # One update exists, so another should not be added
        num_added = br.request_work(wb_session=wb_session,
                                    mlwh_session=mlwh_session,
                                    start_date=start_date)
        assert num_added == 0

    @m.context("When an ONT experiment collection is annotated")
    @m.context("When the experiment is single-sample")
    @m.it("Adds sample and study metadata to the run-folder collection")
    def test_add_new_sample_metadata(self, mlwh_session, wb_session,
                                     irods_synthetic, baton_session):
        expt = "simple_experiment_001"
        pos = 1

        wt = WorkType.ONTRunMetadataUpdate.name
        wb = ONTRunMetadataWorkBot(wt)
        p = PurePath(irods_synthetic, expt,
                     "20190904_1514_GA10000_flowcell011_69126024")
        wi = wb.add_work(wb_session, p)
        wb.add_metadata(wb_session, wi,
                        experiment_name=expt,
                        instrument_position=pos)

        wb.stage_input_data(wb_session, wi)
        wb.run_analysis(wb_session, wi)
        wb.archive_output_data(wb_session, wi)
        wb.annotate_output_data(wb_session, wi, mlwh_session=mlwh_session)

        coll = Collection(baton_session, PurePath(wi.input_path))
        assert AVU("sample", "sample 1") in coll.metadata()
        assert AVU("study_id", "study_02") in coll.metadata()
        assert AVU("study", "Study Y") in coll.metadata()

    @m.context("When the experiment is multiplexed")
    @m.it("Adds {tag_index => <n>} metadata to barcode<0n> sub-collections")
    def test_add_new_plex_metadata(self, mlwh_session, wb_session,
                                   irods_synthetic, baton_session):
        expt = "multiplexed_experiment_001"
        pos = 1

        wt = WorkType.ONTRunMetadataUpdate.name
        wb = ONTRunMetadataWorkBot(wt)
        p = PurePath(irods_synthetic, expt,
                     "20190904_1514_GA10000_flowcell101_cf751ba1")
        wi = wb.add_work(wb_session, p)
        wb.add_metadata(wb_session, wi,
                        experiment_name=expt,
                        instrument_position=pos)

        wb.stage_input_data(wb_session, wi)
        wb.run_analysis(wb_session, wi)
        wb.archive_output_data(wb_session, wi)

        assert not wi.is_annotated()
        wb.annotate_output_data(wb_session, wi, mlwh_session=mlwh_session)
        assert wi.is_annotated()

        for tag_index in range(1, 12):
            bc_dir = "barcode{}".format(str(tag_index).zfill(2))
            bc_coll = Collection(baton_session,
                                 PurePath(wi.input_path, bc_dir))

            assert AVU("tag_index", tag_index) in bc_coll.metadata()

    @m.it("Adds sample and study metadata to barcode<0n> sub-collections")
    def test_add_new_plex_sample_metadata(self, mlwh_session, wb_session,
                                          irods_synthetic, baton_session):
        expt = "multiplexed_experiment_001"
        pos = 1

        wt = WorkType.ONTRunMetadataUpdate.name
        wb = ONTRunMetadataWorkBot(wt)
        p = PurePath(irods_synthetic, expt,
                     "20190904_1514_GA10000_flowcell101_cf751ba1")
        wi = wb.add_work(wb_session, p)
        wb.add_metadata(wb_session, wi,
                        experiment_name=expt,
                        instrument_position=pos)

        wb.stage_input_data(wb_session, wi)
        wb.run_analysis(wb_session, wi)
        wb.archive_output_data(wb_session, wi)
        wb.annotate_output_data(wb_session, wi, mlwh_session=mlwh_session)

        for tag_index in range(1, 12):
            bc_dir = "barcode{}".format(str(tag_index).zfill(2))
            bc_coll = Collection(baton_session,
                                 PurePath(wi.input_path, bc_dir))

            sid = "sample {}".format(tag_index)
            assert AVU("sample", sid) in bc_coll.metadata()
            assert AVU("study_id", "study_03") in bc_coll.metadata()
            assert AVU("study", "Study Z") in bc_coll.metadata()

    @m.context("When completed")
    @m.it("Can be re-run")
    def test_rerun_completed(self, mlwh_session, wb_session,
                             irods_synthetic, baton_session):
        expt = "multiplexed_experiment_001"
        pos = 1

        wt = WorkType.ONTRunMetadataUpdate.name
        wb = ONTRunMetadataWorkBot(wt)
        p = PurePath(irods_synthetic, expt,
                     "20190904_1514_GA10000_flowcell101_cf751ba1")
        wi = wb.add_work(wb_session, p)
        wb.add_metadata(wb_session, wi,
                        experiment_name=expt,
                        instrument_position=pos)

        wb.run(wb_session, wi, mlwh_session=mlwh_session)
        assert wi.is_completed()

        wi = wb.add_work(wb_session, p)
        assert wi is not None
        assert wi.is_pending()

    @m.context("When cancelled")
    @m.it("Cannot be re-run")
    def test_rerun_cancelled(self, mlwh_session, wb_session,
                             irods_synthetic, baton_session):
        expt = "multiplexed_experiment_001"
        pos = 1

        wt = WorkType.ONTRunMetadataUpdate.name
        wb = ONTRunMetadataWorkBot(wt)
        p = PurePath(irods_synthetic, expt,
                     "20190904_1514_GA10000_flowcell101_cf751ba1")
        wi = wb.add_work(wb_session, p)
        wb.add_metadata(wb_session, wi,
                        experiment_name=expt,
                        instrument_position=pos)

        wb.cancel_analysis(wb_session, wi)
        assert wi.is_cancelled()

        with pytest.raises(AnalysisError, match="analyses already exist"):
            wb.add_work(wb_session, p)
