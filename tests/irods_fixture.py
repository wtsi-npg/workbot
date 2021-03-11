import os
from pathlib import PurePath

import pytest

from workbot.irods import AVU, BatonClient, Collection, imkdir, iput, irm
from workbot.metadata import ONTMetadata


@pytest.fixture(scope="function")
def irods_gridion(tmp_path):
    root_path = "/testZone/home/irods/test"
    parts = list(tmp_path.parts[1:])
    rods_path = os.path.join(root_path, *parts)
    imkdir(rods_path, make_parents=True)

    iput("./tests/data/gridion", rods_path, recurse=True)
    expt_root = os.path.join(rods_path, "gridion")

    try:
        yield expt_root
    finally:
        irm(root_path, force=True, recurse=True)


@pytest.fixture(scope="function")
def irods_synthetic(tmp_path, baton_session):
    root_path = "/testZone/home/irods/test"
    parts = list(tmp_path.parts[1:])
    rods_path = PurePath(root_path, *parts)
    imkdir(rods_path, make_parents=True)

    iput("./tests/data/synthetic", rods_path, recurse=True)
    expt_root = PurePath(rods_path, "synthetic")

    avus = [avu.with_namespace(ONTMetadata.namespace) for avu in
            [AVU(ONTMetadata.EXPERIMENT_NAME.value,
                 "simple_experiment_001"),
             AVU(ONTMetadata.INSTRUMENT_SLOT.value,
                 "1")]]

    Collection(baton_session,
               PurePath(expt_root,
                        "simple_experiment_001",
                        "20190904_1514_GA10000_flowcell011_69126024")). \
        meta_add(*avus)

    avus = [avu.with_namespace(ONTMetadata.namespace) for avu in
            [AVU(ONTMetadata.EXPERIMENT_NAME.value,
                 "multiplexed_experiment_001"),
             AVU(ONTMetadata.INSTRUMENT_SLOT.value,
                 "1")]]

    Collection(baton_session,
               PurePath(expt_root, "multiplexed_experiment_001",
                        "20190904_1514_GA10000_flowcell101_cf751ba1")). \
        meta_add(*avus)

    try:
        yield expt_root
    finally:
        irm(root_path, force=True, recurse=True)


@pytest.fixture(scope="function")
def baton_session():
    client = BatonClient()
    client.start()

    try:
        yield client
    finally:
        client.stop()
