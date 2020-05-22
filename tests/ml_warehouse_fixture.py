from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from workbot.ml_warehouse_schema import Study, Sample, OseqFlowcell, MLWHBase

EARLY = datetime(year=2020, month=6, day=1, hour=0, minute=0, second=0)
LATE = datetime(year=2020, month=6, day=14, hour=0, minute=0, second=0)
LATEST = datetime(year=2020, month=6, day=30, hour=0, minute=0, second=0)


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
            when_expt = EARLY if expt % 2 == 0 else LATE

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
            when = EARLY

            # All the odd experiments have the late datetime
            if expt % 2 == 1:
                when = LATE
                # Or latest if they have an odd instrument position
                if pos % 2 == 1:
                    when = LATEST

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

