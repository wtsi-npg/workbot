import os

import pytest

from workbot.irods import imkdir, irm, iput, BatonClient


@pytest.fixture(scope="function")
def irods_tmp_coll(tmp_path):

    root_path = "/testZone/home/irods/test"
    parts = list(tmp_path.parts[1:])
    rods_path = os.path.join(root_path, *parts)
    imkdir(rods_path, make_parents=True)

    _initialize_contents(rods_path)

    yield rods_path

    irm(root_path, force=True, recurse=True)


@pytest.fixture(scope="function")
def baton_session():
    client = BatonClient()
    client.start()

    yield client

    client.stop()


def _initialize_contents(path):

    iput("./tests/data/gridion", path, recurse=True)
