from pathlib import PurePath

import pytest
from pytest import mark as m

from tests.irods_fixture import baton_session, irods_gridion
from workbot.irods import AVU, BatonClient, Collection, DataObject, RodsError

#  Stop IDEs "optimizing" away these imports
_ = irods_gridion
_ = baton_session


@m.describe("BatonClient")
class TestBatonClient(object):
    @m.context("When created")
    @m.it("Is not running")
    def test_create_baton_client(self):
        client = BatonClient()
        assert not client.is_running()

    @m.it("Can be started and stopped")
    def test_start_baton_client(self):
        client = BatonClient()
        client.start()
        assert client.is_running()
        client.stop()
        assert not client.is_running()

    @m.context("When stopped")
    @m.it("Can be re-started")
    def test_restart_baton_client(self, irods_gridion):
        client = BatonClient()
        client.start()
        assert client.is_running()
        client.stop()
        assert not client.is_running()
        # Re-start
        client.start()
        assert client.is_running()
        # Try an operation
        coll = Collection(client, irods_gridion)
        assert coll.exists()
        client.stop()

    @m.context("When running")
    @m.it("Can list a collection (non-recursively)")
    def test_list_collection(self, irods_gridion, baton_session):
        coll = Collection(baton_session, irods_gridion)
        assert coll.list() == Collection(baton_session, irods_gridion)

        coll = Collection(baton_session, "/no/such/collection")
        with pytest.raises(RodsError, match="does not exist"):
            coll.list()

    @m.it("Can list collection contents")
    def test_list_collection_contents(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f")

        coll = Collection(baton_session, p)
        contents = coll.contents()
        assert len(contents) == 11

    @m.it("Can list a data object")
    def test_list_data_object(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f",
                     "final_summary.txt")

        obj = DataObject(baton_session, p)
        assert obj.list() == DataObject(baton_session, p)

        obj = DataObject(baton_session, "/no/such/data_object.txt")
        with pytest.raises(RodsError, match="does not exist"):
            obj.list()

    @m.it("Can test existence of a collection")
    def test_exists_collection(self, irods_gridion, baton_session):
        coll = Collection(baton_session, irods_gridion)
        assert coll.exists()

        coll = Collection(baton_session, "/no/such/collection")
        assert not coll.exists()

    @m.it("Can test existence of a data object")
    def test_exists_data_object(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f",
                     "final_summary.txt")

        obj = DataObject(baton_session, p)
        assert obj.exists()

        obj = DataObject(baton_session, "/no/such/data_object.txt")
        assert not obj.exists()

    @m.it("Can add metadata to a collection")
    def test_meta_add_collection(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f")
        coll = Collection(baton_session, p)
        assert coll.metadata() == []

        avu1 = AVU("abcde", "12345")
        avu2 = AVU("vwxyz", "567890")

        assert coll.meta_add(avu1, avu2) == 2
        assert avu1 in coll.metadata()
        assert avu2 in coll.metadata()

        assert coll.meta_add(avu1, avu2) == 0, \
            "adding collection metadata is idempotent"

    @m.it("Can remove metadata from a collection")
    def test_meta_rem_collection(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f")
        coll = Collection(baton_session, p)
        assert coll.metadata() == []

        avu1 = AVU("abcde", "12345")
        avu2 = AVU("vwxyz", "567890")
        coll.meta_add(avu1, avu2)

        assert coll.meta_remove(avu1, avu2) == 2
        assert avu1 not in coll.metadata()
        assert avu2 not in coll.metadata()
        assert coll.meta_remove(avu1, avu2) == 0, \
            "removing collection metadata is idempotent"

    @m.it("Can add metadata to a data object")
    def test_meta_add_data_object(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f",
                     "final_summary.txt")
        obj = DataObject(baton_session, p)
        assert obj.metadata() == []

        avu1 = AVU("abcde", "12345")
        avu2 = AVU("vwxyz", "567890")

        obj.meta_add(avu1, avu2)
        assert avu1 in obj.metadata()
        assert avu2 in obj.metadata()

        assert obj.meta_add(avu1, avu2) == 0, \
            "adding data object metadata is idempotent"

    @m.it("Can remove metadata from a data object")
    def test_meta_rem_data_object(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f",
                     "final_summary.txt")
        obj = DataObject(baton_session, p)
        assert obj.metadata() == []

        avu1 = AVU("abcde", "12345")
        avu2 = AVU("vwxyz", "567890")
        obj.meta_add(avu1, avu2)

        assert obj.meta_remove(avu1, avu2) == 2
        assert avu1 not in obj.metadata()
        assert avu2 not in obj.metadata()
        assert obj.meta_remove(avu1, avu2) == 0, \
            "removing data object metadata is idempotent"

    @m.it("Can replace metadata on a data object")
    def test_meta_rep_data_object(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f",
                     "final_summary.txt")
        obj = DataObject(baton_session, p)
        assert obj.metadata() == []

        avu1 = AVU("abcde", "12345")
        avu2 = AVU("vwxyz", "567890")
        obj.meta_add(avu1, avu2)

        assert obj.meta_supersede(avu1, avu2) == (0, 0), \
            "nothing is replaced when new all AVUs == all old AVUs"
        assert obj.metadata() == [avu1, avu2]

        assert obj.meta_supersede(avu1) == (0, 0), \
            "nothing is replaced when one new AVU is in the AVUs"
        assert obj.metadata() == [avu1, avu2]

        avu3 = AVU("abcde", "88888")
        avu4 = AVU("abcde", "99999")
        avu5 = AVU("abcde", "00000")
        obj.meta_add(avu3)

        assert obj.meta_supersede(avu4, avu5) == (2, 2), \
            "AVUs sharing an attribute with a new AVU are replaced"

        expected = [avu2, avu4, avu5]
        expected.sort()
        assert obj.metadata() == expected

    @m.it("Can find a collection by its metadata")
    def test_meta_query_collection(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f")
        coll = Collection(baton_session, p)

        avu = AVU("abcde", "12345")
        coll.meta_add(avu)
        assert coll.metadata() == [avu]

        found = baton_session.meta_query([avu], collection=True,
                                         zone=irods_gridion)
        assert found == [Collection(baton_session, p)]


@m.describe("AVU")
class TestAVU(object):
    @m.describe("Comparison")
    def test_compare_avus_equal(self):
        assert AVU("a", 1) == AVU("a", 1)
        assert AVU("a", 1, "mm") == AVU("a", 1, "mm")

        assert AVU("a", 1) != AVU("a", 1, "mm")

        assert AVU("a", 1).with_namespace("x") == \
               AVU("a", 1).with_namespace("x")

        assert AVU("a", 1).with_namespace("x") != \
               AVU("a", 1).with_namespace("y")

    def test_compare_avus_lt(self):
        assert AVU("a", 1) < AVU("b", 1)
        assert AVU("a", 1) < AVU("a", 2)

        assert AVU("a", 1, "mm") < AVU("a", 1)
        assert AVU("a", 1, "mm") < AVU("a", 2, "mm")
        assert AVU("a", 1, "cm") < AVU("a", 1, "mm")

        assert AVU("a", 1).with_namespace("x") < AVU("a", 1)
        assert AVU("z", 99).with_namespace("x") < AVU("a", 1)

        assert AVU("a", 1).with_namespace("x") < \
               AVU("a", 1).with_namespace("y")

    def test_compare_avus_sort(self):
        x = [AVU("z", 1), AVU("y", 1), AVU("x", 1)]
        x.sort()
        assert x == [AVU("x", 1), AVU("y", 1), AVU("z", 1)]

        y = [AVU("x", 2), AVU("x", 3), AVU("x", 1)]
        y.sort()
        assert y == [AVU("x", 1), AVU("x", 2), AVU("x", 3)]

    def test_compare_avus_sort_ns(self):
        x = [AVU("z", 1).with_namespace("a"), AVU("y", 1), AVU("x", 1)]
        x.sort()

        assert x == [AVU("z", 1).with_namespace("a"),
                     AVU("x", 1), AVU("y", 1)]

    def test_compare_avus_sort_units(self):
        x = [AVU("x", 1, "mm"), AVU("x", 1, "cm"), AVU("x", 1, "km")]
        x.sort()

        assert x == [AVU("x", 1, "cm"), AVU("x", 1, "km"), AVU("x", 1, "mm")]


@m.describe("Collection")
class TestCollection(object):
    @m.describe("Support for str path")
    @m.context("When a Collection is made from a str path")
    @m.it("Can be created")
    def test_make_collection_str(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion)
        coll = Collection(baton_session, p.as_posix())

        assert coll.exists()
        assert coll.path == p

    @m.describe("Support for pathlib.Path")
    @m.context("When a Collection is made from a pathlib.Path")
    @m.it("Can be created")
    def test_make_collection_pathlib(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion)
        coll = Collection(baton_session, p)

        assert coll.exists()
        assert coll.path == p


@m.describe("DataObject")
class TestDataObject(object):
    @m.context("When a DataObject is made from a str path")
    @m.it("Can be created")
    def test_make_data_object_str(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f",
                     "final_summary.txt")
        obj = DataObject(baton_session, p.as_posix())

        assert obj.exists()
        assert obj.path == p.parent
        assert obj.name == p.name

    @m.context("When a DataObject is made from a pathlib.Path")
    @m.it("Can be created")
    def test_make_data_object_pathlib(self, irods_gridion, baton_session):
        p = PurePath(irods_gridion, "66", "DN585561I_A1",
                     "20190904_1514_GA20000_FAL01979_43578c8f",
                     "final_summary.txt")
        obj = DataObject(baton_session, p)

        assert obj.exists()
        assert obj.path == p.parent
        assert obj.name == p.name
