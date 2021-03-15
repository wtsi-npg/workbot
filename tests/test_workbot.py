from pathlib import Path, PurePath

import pytest
from pytest import mark as m

from tests.irods_fixture import baton_session, irods_gridion
from tests.ml_warehouse_fixture import mlwh_session
from tests.schema_fixture import wb_session
from workbot.base import AnalysisError, WorkBot, make_workbot
from workbot.enums import WorkState, WorkType
from workbot.irods import imkdir
from workbot.ont import ONTRunDataWorkBot, ONTRunMetadataWorkBot

#  Stop IDEs "optimizing" away these imports
_ = mlwh_session
_ = wb_session

_ = irods_gridion
_ = baton_session


@m.describe("WorkBot")
class TestWorkbot(object):
    @m.context("When created")
    @m.it("Has must have a work type")
    def test_make_workbot_no_worktype(self):
        with pytest.raises(TypeError):
            WorkBot()

    @m.context("When a work type is set")
    @m.it("Appears in its compatible worktypes")
    def test_make_compatible_worktype(self):
        work_type = WorkType.EMPTY.name
        assert work_type in WorkBot(work_type).compatible_work_types()

        with pytest.raises(ValueError, match="invalid work type"):
            WorkBot("no_such_work_type")

    @m.it("Has the correct default root paths")
    def test_make_workbot_ont_run_data_paths(self):
        wb = WorkBot(WorkType.EMPTY.name)
        assert wb.archive_root is not None
        assert wb.staging_root is not None


@m.describe("WorkBot factory")
class TestWorkBotFactory(object):
    @m.context("When a work type is specified")
    @m.it("Creates the correct type of Workbot")
    def test_workbot_factory(self):
        pairs = [(WorkType.EMPTY, WorkBot),
                 (WorkType.ARTICNextflow, ONTRunDataWorkBot),
                 (WorkType.ONTRunMetadataUpdate, ONTRunMetadataWorkBot)]

        for worktype_name, cls in pairs:
            assert make_workbot(worktype_name).__class__ == cls

    @m.it("Passes kwargs to the constructor")
    def test_workbot_factory_kwargs(self):
        archive_root = PurePath("/dummy")
        staging_root = Path("/dummy")
        wb = make_workbot(WorkType.EMPTY,
                          archive_root=archive_root,
                          staging_root=staging_root)
        assert wb.archive_root == archive_root.as_posix()
        assert wb.staging_root == staging_root.as_posix()


@m.describe("Finding analyses")
class TestFindingAnalyses(object):
    @m.context("When specific states are included")
    @m.it("Includes the analysis")
    def test_find_analyses_include(self, wb_session):
        input_path = PurePath("/seq/ont/gridion/experiment_01")
        archive_root = PurePath("/dummy")
        staging_root = Path("/dummy")

        wb = WorkBot(WorkType.EMPTY.name,
                     archive_root=archive_root,
                     staging_root=staging_root)
        wi = wb.add_work(wb_session, input_path)

        assert wb.find_work(wb_session, input_path,
                            states=[WorkState.PENDING]) == [wi]

    @m.context("When specific states are excluded")
    @m.it("Excludes the analysis")
    def test_find_analyses_exclude(self, wb_session):
        input_path = "/seq/ont/gridion/experiment_01"
        archive_root = "/dummy"
        staging_root = "/dummy"

        wb = WorkBot(WorkType.EMPTY.name,
                     archive_root=archive_root,
                     staging_root=staging_root)
        _ = wb.add_work(wb_session, input_path)

        assert wb.find_work(wb_session, input_path,
                            not_states=[WorkState.PENDING]) == []


@m.describe("Adding analyses")
class TestAddingAnalyses(object):
    @m.context("When there is no existing analysis")
    @m.it("Can be added")
    def test_add_analysis(self, wb_session):
        input_path = PurePath("/seq/ont/gridion/experiment_01")
        archive_root = "/dummy"
        staging_root = "/dummy"

        wb = WorkBot(WorkType.EMPTY.name, archive_root, staging_root)
        assert wb.find_work(wb_session, input_path) == []

        wi = wb.add_work(wb_session, input_path)
        assert wi.input_path == input_path
        assert wi.state.name == WorkState.PENDING

        assert wb.find_work(wb_session, input_path) == [wi]

    @m.context("When there is an existing analysis")
    @m.it("Cannot be added")
    def test_add_analysis_existing(self, wb_session):
        input_path = PurePath("/seq/ont/gridion/experiment_01")
        archive_root = "/dummy"
        staging_root = "/dummy"

        wb = WorkBot(WorkType.EMPTY.name, archive_root, staging_root)
        wi = wb.add_work(wb_session, input_path)
        assert wb.find_work(wb_session, input_path,
                            states=[WorkState.PENDING]) == [wi]

        assert wb.add_work(wb_session, input_path) is None

        assert wb.find_work(wb_session, input_path,
                            states=[WorkState.PENDING]) == [wi]

    @m.context("When an existing analysis is cancelled")
    @m.it("Raises an exception")
    def test_add_analysis_cancelled(self, wb_session):
        input_path = PurePath("/seq/ont/gridion/experiment_01")
        archive_root = PurePath("/dummy")
        staging_root = Path("/dummy")

        wb = WorkBot(WorkType.EMPTY.name, archive_root, staging_root)
        wi = wb.add_work(wb_session, input_path)
        wi.cancelled(wb_session)
        wb_session.commit()

        assert wb.find_work(wb_session, input_path,
                            states=[WorkState.CANCELLED]) == [wi]
        assert wb.find_work(wb_session, input_path,
                            not_states=[WorkState.CANCELLED]) == []

        with pytest.raises(AnalysisError, match="analyses already exist"):
            wb.add_work(wb_session, input_path)


@m.describe("Pre-analysis")
class TestPreAnalysis(object):
    @m.context("When an analysis input is present")
    @m.it("Is detected")
    def test_is_input_path_present(self, wb_session, irods_gridion):
        archive_root = PurePath("/dummy")
        staging_root = Path("/dummy")

        wb = WorkBot(WorkType.EMPTY.name, archive_root, staging_root)
        p = Path(irods_gridion, "dummy_input")
        wi = wb.add_work(wb_session, p)

        assert not wb.is_input_path_present(wi)
        imkdir(p, make_parents=True)
        assert wb.is_input_path_present(wi)
