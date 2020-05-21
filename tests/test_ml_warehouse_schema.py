from datetime import datetime, timedelta

import pytest
from pytest import mark as m
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from workbot.ml_warehouse_schema import MLWHBase, Sample, Study, \
    OseqFlowcell, find_recent_experiments, find_recent_experiment_pos

early = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0)
late = datetime(year=2020, month=6, day=14, hour=0, minute=0, second=0)
latest = datetime(year=2020, month=6, day=30, hour=0, minute=0, second=0)


@pytest.fixture(scope="function")
def mlwh_session(tmp_path) -> Session:
    p = tmp_path / "mlwh"
    uri = 'sqlite:///{}'.format(p)

    engine = create_engine(uri, echo=False)
    MLWHBase.metadata.create_all(engine)

    session_maker = sessionmaker(bind=engine)
    sess = session_maker()

    initialize_mlwh(sess)

    yield sess
    sess.close()


@m.describe("Finding updated experiments by datetime")
@m.context("When a query date is provided")
@m.it("Finds the correct experiments")
def test_find_recent_experiments(mlwh_session):
    all_expts = ['simple_experiment_001',
                 'simple_experiment_002',
                 'simple_experiment_003',
                 'simple_experiment_004',
                 'simple_experiment_005',
                 'multiplexed_experiment_001',
                 'multiplexed_experiment_002',
                 'multiplexed_experiment_003']
    assert find_recent_experiments(mlwh_session, early) == all_expts

    # Odd-numbered experiments were done late or latest
    before_late = late - timedelta(days=1)
    odd_expts = ['simple_experiment_001',
                 'simple_experiment_003',
                 'simple_experiment_005',
                 'multiplexed_experiment_001',
                 'multiplexed_experiment_003']
    assert find_recent_experiments(mlwh_session, before_late) == odd_expts

    after_latest = latest + timedelta(days=1)
    none = find_recent_experiments(mlwh_session, after_latest)
    assert none == []


@m.describe("Finding updated experiments and positions by datetime")
@m.context("When a query date is provided")
@m.it("Finds the correct experiment, position tuples")
def test_find_recent_experiment_pos(mlwh_session):
    before_late = late - timedelta(days=1)
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
    assert find_recent_experiment_pos(mlwh_session, before_late) == odd_expts

    before_latest = latest - timedelta(days=1)
    odd_positions = [('multiplexed_experiment_001', 1),
                     ('multiplexed_experiment_001', 3),
                     ('multiplexed_experiment_001', 5),
                     ('multiplexed_experiment_003', 1),
                     ('multiplexed_experiment_003', 3),
                     ('multiplexed_experiment_003', 5)]
    assert find_recent_experiment_pos(mlwh_session,
                                      before_latest) == odd_positions

    after_latest = latest + timedelta(days=1)
    assert find_recent_experiment_pos(mlwh_session, after_latest) == []


def initialize_mlwh(session: Session):
    instrument_name = "instrument_01"
    pipeline_id_lims = "Ligation"
    req_data_type = "Basecalls and raw data"

    study_x = Study(id_lims="LIMS_01", id_study_lims="study_01",
                    name="Study X")
    study_y = Study(id_lims="LIMS_01", id_study_lims="study_02",
                    name="Study Y")
    study_z = Study(id_lims="LIMS_01", id_study_lims="study_03",
                    name="Study Z")
    session.add_all([study_x, study_y, study_z])
    session.flush()

    samples = []
    flowcells = []

    num_samples = 200
    for s in range(1, num_samples + 1):
        sid = "sample{}".format(s)
        name = "sample {}".format(s)
        samples.append(
            Sample(id_lims="LIMS_01", id_sample_lims=sid, name=name))
    session.add_all(samples)
    session.flush()

    num_simple_expts = 5
    num_instrument_pos = 5
    sample_idx = 0
    for expt in range(1, num_simple_expts + 1):
        for pos in range(1, num_instrument_pos + 1):
            expt_name = "simple_experiment_{:03}".format(expt)
            id_flowcell = "flowcell {:03}".format(pos + 10)

            # All the even experiments have the early datetime
            # All the odd experiments have the late datetime
            when_expt = early if expt % 2 == 0 else late

            flowcells.append(OseqFlowcell(sample=samples[sample_idx],
                                          study=study_y,
                                          instrument_name=instrument_name,
                                          instrument_slot=pos,
                                          experiment_name=expt_name,
                                          id_flowcell_lims=id_flowcell,
                                          pipeline_id_lims=pipeline_id_lims,
                                          requested_data_type=req_data_type,
                                          last_updated=when_expt))
            sample_idx += 1

    num_multiplexed_expts = 3
    num_instrument_pos = 5
    barcodes = ["CACAAAGACACCGACAACTTTCTT",
                "ACAGACGACTACAAACGGAATCGA",
                "CCTGGTAACTGGGACACAAGACTC",
                "TAGGGAAACACGATAGAATCCGAA",
                "AAGGTTACACAAACCCTGGACAAG",
                "GACTACTTTCTGCCTTTGCGAGAA",

                "AAGGATTCATTCCCACGGTAACAC",
                "ACGTAACTTGGTTTGTTCCCTGAA",
                "AACCAAGACTCGCTGTGCCTAGTT",
                "GAGAGGACAAAGGTTTCAACGCTT",
                "TCCATTCCCTCCGATAGATGAAAC",
                "TCCGATTCTGCTTCTTTCTACCTG"]

    msample_idx = 0
    for expt in range(1, num_multiplexed_expts + 1):
        for pos in range(1, num_instrument_pos + 1):
            expt_name = "multiplexed_experiment_{:03}".format(expt)
            id_flowcell = "flowcell {:03}".format(pos + 100)

            # All the even experiments have the early datetime
            when = early

            # All the odd experiments have the late datetime
            if expt % 2 == 1:
                when = late
                # Or latest if they have an odd instrument position
                if pos % 2 == 1:
                    when = latest

            for barcode_idx, barcode in enumerate(barcodes):
                flowcells.append(OseqFlowcell(
                    sample=samples[msample_idx],
                    study=study_z,
                    instrument_name=instrument_name,
                    instrument_slot=pos,
                    experiment_name=expt_name,
                    id_flowcell_lims=id_flowcell,
                    tag_set_id_lims="ONT_12",
                    tag_set_name="ONT library barcodes x12",
                    tag_sequence=barcode,
                    tag_identifier=barcode_idx + 1,
                    pipeline_id_lims=pipeline_id_lims,
                    requested_data_type=req_data_type,
                    last_updated=when))
                msample_idx += 1

    session.add_all(flowcells)
    session.commit()
