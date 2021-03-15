from datetime import timedelta

from pytest import mark as m

from tests.ml_warehouse_fixture import EARLY, LATE, LATEST, mlwh_session
from workbot.ml_warehouse_schema import find_recent_ont_expt, \
    find_recent_ont_pos

# Stop IDEs "optimizing" away this import
_ = mlwh_session


@m.describe("Finding updated experiments by datetime")
class TestMLWarehouseQueries(object):
    @m.context("When a query date is provided")
    @m.it("Finds the correct experiments")
    def test_find_recent_experiments(self, mlwh_session):
        all_expts = ['simple_experiment_001',
                     'simple_experiment_002',
                     'simple_experiment_003',
                     'simple_experiment_004',
                     'simple_experiment_005',
                     'multiplexed_experiment_001',
                     'multiplexed_experiment_002',
                     'multiplexed_experiment_003']
        assert find_recent_ont_expt(mlwh_session, EARLY) == all_expts

        # Odd-numbered experiments were done late or latest
        before_late = LATE - timedelta(days=1)
        odd_expts = ['simple_experiment_001',
                     'simple_experiment_003',
                     'simple_experiment_005',
                     'multiplexed_experiment_001',
                     'multiplexed_experiment_003']
        assert find_recent_ont_expt(mlwh_session, before_late) == odd_expts

        after_latest = LATEST + timedelta(days=1)
        none = find_recent_ont_expt(mlwh_session, after_latest)
        assert none == []

    @m.describe("Finding updated experiments and positions by datetime")
    @m.context("When a query date is provided")
    @m.it("Finds the correct experiment, position tuples")
    def test_find_recent_experiment_pos(self, mlwh_session):
        before_late = LATE - timedelta(days=1)
        odd_expts = [('multiplexed_experiment_001', 1),
                     ('multiplexed_experiment_001', 2),
                     ('multiplexed_experiment_001', 3),
                     ('multiplexed_experiment_001', 4),
                     ('multiplexed_experiment_001', 5),
                     ('multiplexed_experiment_003', 1),
                     ('multiplexed_experiment_003', 2),
                     ('multiplexed_experiment_003', 3),
                     ('multiplexed_experiment_003', 4),
                     ('multiplexed_experiment_003', 5),
                     ('simple_experiment_001', 1),
                     ('simple_experiment_001', 2),
                     ('simple_experiment_001', 3),
                     ('simple_experiment_001', 4),
                     ('simple_experiment_001', 5),
                     ('simple_experiment_003', 1),
                     ('simple_experiment_003', 2),
                     ('simple_experiment_003', 3),
                     ('simple_experiment_003', 4),
                     ('simple_experiment_003', 5),
                     ('simple_experiment_005', 1),
                     ('simple_experiment_005', 2),
                     ('simple_experiment_005', 3),
                     ('simple_experiment_005', 4),
                     ('simple_experiment_005', 5)]
        assert find_recent_ont_pos(mlwh_session, before_late) == odd_expts

        before_latest = LATEST - timedelta(days=1)
        odd_positions = [('multiplexed_experiment_001', 1),
                         ('multiplexed_experiment_001', 3),
                         ('multiplexed_experiment_001', 5),
                         ('multiplexed_experiment_003', 1),
                         ('multiplexed_experiment_003', 3),
                         ('multiplexed_experiment_003', 5)]
        assert find_recent_ont_pos(mlwh_session,
                                   before_latest) == odd_positions

        after_latest = LATEST + timedelta(days=1)
        assert find_recent_ont_pos(mlwh_session, after_latest) == []
