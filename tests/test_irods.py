import os

import pytest
from pytest import mark as m

from tests.irods_fixture import irods_tmp_coll, baton_session
from workbot.irods import BatonClient, Collection, RodsError, DataObject

#  Stop IDEs "optimizing" away these imports
_ = irods_tmp_coll
_ = baton_session


@m.describe("BatonClient")
@m.context("When created")
@m.it("Is not running")
def test_create_baton_client():
    client = BatonClient()
    assert not client.is_running()


@m.it("Can be started and stopped")
def test_start_baton_client():
    client = BatonClient()
    client.start()
    assert client.is_running()
    client.stop()
    assert not client.is_running()


@m.context("When stopped")
@m.it("Can be re-started")
def test_restart_baton_client(irods_tmp_coll):
    client = BatonClient()
    client.start()
    assert client.is_running()
    client.stop()
    assert not client.is_running()
    # Re-start
    client.start()
    assert client.is_running()
    # Try an operation
    coll = Collection(client, irods_tmp_coll)
    assert coll.exists()
    client.stop()


@m.context("When running")
@m.it("Can list a collection (non-recursively)")
def test_list_collection(irods_tmp_coll, baton_session):
    coll = Collection(baton_session, irods_tmp_coll)
    assert coll.list() == irods_tmp_coll

    coll = Collection(baton_session, "/no/such/collection")
    with pytest.raises(RodsError, match="does not exist"):
        coll.list()


@m.it("Can list collection contents")
def test_list_collection_contents(irods_tmp_coll, baton_session):
    p = os.path.join(irods_tmp_coll, "gridion/66/DN585561I_A1/"
                                     "20190904_1514_GA20000_FAL01979_43578c8f/")

    coll = Collection(baton_session, p)
    contents = coll.list(contents=True)
    assert len(contents) == 10


@m.it("Can list a data object")
def test_list_data_object(irods_tmp_coll, baton_session):
    p = os.path.join(irods_tmp_coll, "gridion/66/DN585561I_A1/"
                                     "20190904_1514_GA20000_FAL01979_43578c8f/"
                                     "final_summary.txt")

    obj = DataObject(baton_session, p)
    assert obj.list() == p

    obj = DataObject(baton_session, "/no/such/data_object.txt")
    with pytest.raises(RodsError, match="does not exist"):
        obj.list()


@m.it("Can test existence of a collection")
def test_exists_collection(irods_tmp_coll, baton_session):
    coll = Collection(baton_session, irods_tmp_coll)
    assert coll.exists()

    coll = Collection(baton_session, "/no/such/collection")
    assert not coll.exists()


@m.it("Can test existence of a data object")
def test_exists_data_object(irods_tmp_coll, baton_session):
    p = os.path.join(irods_tmp_coll, "gridion/66/DN585561I_A1/"
                                     "20190904_1514_GA20000_FAL01979_43578c8f/"
                                     "final_summary.txt")

    obj = DataObject(baton_session, p)
    assert obj.exists()

    obj = DataObject(baton_session, "/no/such/data_object.txt")
    assert not obj.exists()


@m.it("Can add metadata to a collection")
def test_meta_add_collection(irods_tmp_coll, baton_session):
    p = os.path.join(irods_tmp_coll, "gridion/66/DN585561I_A1/"
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    coll = Collection(baton_session, p)
    assert coll.metadata() == []

    avu1 = {"attribute": "abcde", "value": "12345"}
    avu2 = {"attribute": "vwxyz", "value": "67890"}
    coll.meta_add(avu1, avu2)
    assert avu1 in coll.metadata()
    assert avu2 in coll.metadata()

    assert coll.meta_add(avu1, avu2) == 0, \
        "adding collection metadata is idempotent"


@m.it("Can add metadata to a data object")
def test_meta_add_data_object(irods_tmp_coll, baton_session):
    p = os.path.join(irods_tmp_coll, "gridion/66/DN585561I_A1/"
                                     "20190904_1514_GA20000_FAL01979_43578c8f/"
                                     "final_summary.txt")
    obj = DataObject(baton_session, p)
    assert obj.metadata() == []

    avu1 = {"attribute": "abcde", "value": "12345"}
    avu2 = {"attribute": "vwxyz", "value": "67890"}
    obj.meta_add(avu1, avu2)
    assert avu1 in obj.metadata()
    assert avu2 in obj.metadata()

    assert obj.meta_add(avu1, avu2) == 0, \
        "adding data object metadata is idempotent"


@m.it("Can find a collection by its metadata")
def test_meta_query_collection(irods_tmp_coll, baton_session):
    p = os.path.join(irods_tmp_coll, "gridion/66/DN585561I_A1/"
                                     "20190904_1514_GA20000_FAL01979_43578c8f")
    coll = Collection(baton_session, p)
    avu = {"attribute": "abcde", "value": "12345"}
    coll.meta_add(avu)
    assert coll.metadata() == [avu]

    found = baton_session.meta_query([avu], collection=True,
                                     zone=irods_tmp_coll)
    assert found == [p]

