# -*- coding: utf-8 -*-
#
# Copyright © 2020, 2021 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# @author Keith James <kdj@sanger.ac.uk>

from __future__ import annotations  # Will not be needed in Python 3.10

import json
import logging
import subprocess
from abc import abstractmethod
from datetime import datetime
from enum import Enum, unique
from functools import total_ordering
from os import PathLike
from pathlib import PurePath
from typing import Any, Dict, List, Tuple, Union

log = logging.getLogger(__package__)

"""This module provides a basic API for accessing iRODS using the native
iRODS client 'baton' (https://github.com/wtsi-npg/baton).

NB: We do not use iRODS' Python interface which misses features present in 
baton, including:

- No parallel transfer for puts and gets
  https://github.com/irods/python-irodsclient/issues/235

However, we should consider to it once it is more feature-complete and has 
demonstrated a period of stability.
"""


class RodsError(Exception):
    """Exception wrapping an error raised by the iRODS server."""

    def __init__(self, message: str, code: int):
        """Create a new exception.

        Args:
            message: The iRODS error message
            code: The iRODS error code
        """
        self.message = message
        self.code = code

    def __repr__(self):
        return str(self.code)

    def __str__(self):
        return "<RodsError: {} - {}>".format(self.code, self.message)


class BatonError(Exception):
    pass


@unique
class Permission(Enum):
    NULL = "null"
    OWN = "own",
    READ = "read",
    WRITE = "write",


@total_ordering
class AC(object):
    """AC is an iRODS access control."""

    SEPARATOR = "#"

    def __init__(self, user: str, perm: Permission, zone=None):
        if user is None:
            raise ValueError("user may not be None")

        if user.find(AC.SEPARATOR) >= 0:
            raise ValueError("User '{}' should not contain a zone suffix. "
                             "Please use the zone= keyword argument to set "
                             "a zone".format(user))

        if zone:
            if zone.find(AC.SEPARATOR) >= 0:
                raise ValueError("Zone '{}' "
                                 "contained '{}'".format(zone, AC.SEPARATOR))
        self.user = user
        self.zone = zone
        self.perm = perm

    def __hash__(self):
        return hash(self.user) + hash(self.zone) + hash(self.perm)

    def __eq__(self, other):
        return isinstance(other, AC) and \
               self.user == other.user and \
               self.zone == other.zone and \
               self.perm == other.perm

    def __lt__(self, other):
        if self.zone is not None and other.zone is None:
            return True

        if self.zone is None and other.zone is not None:
            return True

        if self.zone is not None and other.zone is not None:
            if self.zone < other.zone:
                return True

        if self.zone == other.zone:
            if self.user < other.user:
                return True

            if self.user == other.user:
                return self.perm.name < other.perm.name

        return False

    def __repr__(self):
        z = AC.SEPARATOR + self.zone if self.zone else ""
        return "{}{}:{}".format(self.user, z, self.perm.name.lower())


@total_ordering
class AVU(object):
    """AVU is an iRODS attribute, value, units tuple.

    AVUs may be sorted, where they will sorted lexically, first by
    namespace (if present), then by attribute, then by value and finally by
    units (if present).
    """

    SEPARATOR = ":"
    """The attribute namespace separator"""

    HISTORY_SUFFIX = "_history"
    """The attribute history suffix"""

    def __init__(self, attribute: str, value: Any, units=None, namespace=None):
        if namespace:
            if namespace.find(AVU.SEPARATOR) >= 0:
                raise ValueError("AVU namespace '{}' "
                                 "contained '{}'".format(namespace,
                                                         AVU.SEPARATOR))
        if attribute is None:
            raise ValueError("AVU attribute may not be None")
        if value is None:
            raise ValueError("AVU value may not be None")

        self._namespace = namespace
        self._attribute = str(attribute)
        self._value = str(value)
        self._units = units

    @classmethod
    def collate(cls, *avus) -> Dict[str: List[AVU]]:
        """Collates AVUs by attribute (including namespace, if any) and
        returns a dict mapping the attribute to a list of AVUs with that
        attribute.

        Returns: Dict[str: List[AVU]]
        """
        collated = {}

        for avu in avus:
            if avu.attribute not in collated:
                collated[avu.attribute] = []
            collated[avu.attribute].append(avu)

        return collated

    @classmethod
    def history(cls, *avus, history_date=None) -> AVU:
        """Returns a history AVU describing the argument AVUs. A history AVU is
        sometimes added to an iRODS path to describe AVUs that were once
        present, but have been removed. Adding a history AVU can act as a poor
        man's audit trail and it used because iRODS does not have native
        history support.

        Args:
            avus: AVUs removed, which must share the same attribute
            and namespace (if any).
            history_date: A datetime to be embedded as part of the history
            AVU value.

        Returns: AVU
        """
        if history_date is None:
            history_date = datetime.utcnow()
        date = history_date.isoformat(timespec="seconds")

        # Check that the AVUs have the same namespace and attribute and that
        # none are history attributes (we don't do meta-history!)
        namespaces = set()
        attributes = set()
        values = set()
        for avu in avus:
            if avu.is_history():
                raise ValueError("Cannot create a history of "
                                 "a history AVU: {}".format(avu))
            namespaces.add(avu.namespace)
            attributes.add(avu.without_namespace)
            values.add(avu.value)

        if len(namespaces) > 1:
            raise ValueError("Cannot create a history for AVUs with a "
                             "mixture of namespaces: {}".format(namespaces))
        if len(attributes) > 1:
            raise ValueError("Cannot create a history for AVUs with a "
                             "mixture of attributes: {}".format(attributes))

        history_namespace = namespaces.pop()
        history_attribute = attributes.pop() + AVU.HISTORY_SUFFIX
        history_value = "[{}] {}".format(date, ",".join(sorted(list(values))))

        return AVU(history_attribute, history_value,
                   namespace=history_namespace)

    @property
    def namespace(self):
        return self._namespace

    @property
    def without_namespace(self):
        """The attribute without namespace."""

        return self._attribute

    @property
    def attribute(self):
        if self._namespace:
            return "{}{}{}".format(self._namespace, AVU.SEPARATOR,
                                   self._attribute)
        else:
            return self.without_namespace

    @property
    def value(self):
        return self._value

    @property
    def units(self):
        return self._units

    def with_namespace(self, namespace: str):
        return AVU(self._attribute,
                   self._value,
                   self._units,
                   namespace=namespace)

    def is_history(self) -> bool:
        """Return true if this is a history AVU."""
        return self._attribute.endswith(AVU.HISTORY_SUFFIX)

    def __hash__(self):
        return hash(self.attribute) + hash(self.value) + hash(self.units)

    def __eq__(self, other):
        if not isinstance(other, AVU):
            return False

        return self.attribute == other.attribute and \
            self.value == other.value and \
            self.units == other.units

    def __lt__(self, other):
        if self.namespace is not None and other.namespace is None:
            return True
        if self.namespace is None and other.namespace is not None:
            return False

        if self.namespace is not None and other.namespace is not None:
            if self.namespace < other.namespace:
                return True

        if self.namespace == other.namespace:
            if self.attribute < other.attribute:
                return True

            if self.attribute == other.attribute:
                if self.value < other.value:
                    return True

                if self.value == other.value:
                    if self.units is not None and other.units is None:
                        return True
                    if self.units is None and other.units is not None:
                        return False
                    if self.units is None and other.units is None:
                        return False

                    return self.units < other.units

        return False

    def __repr__(self):
        u = " " + self.units if self._units else ""
        return "{}={}{}".format(self.attribute, self.value, u)

    def __str__(self):
        u = " " + self.units if self._units else ""
        return "<AVU '{}' = '{}'{}>".format(self.attribute, self.value, u)


class BatonJSONEncoder(json.JSONEncoder):
    """Encoder for baton JSON."""
    def default(self, o: Any) -> Any:

        if isinstance(o, AVU):
            enc = {BatonClient.ATTRIBUTE: o.attribute,
                   BatonClient.VALUE: o.value}
            if o.units:
                enc[BatonClient.UNITS] = o.units
            return enc

        if isinstance(o, Permission):
            return o.name.lower()

        if isinstance(o, AC):
            return {BatonClient.OWNER: o.user,
                    BatonClient.ZONE: o.zone,
                    BatonClient.LEVEL: o.perm}

        if isinstance(o, PurePath):
            return o.as_posix()


def as_baton(d: Dict) -> Any:
    """Object hook for decoding baton JSON."""

    # Match an AVU sub-document
    if BatonClient.ATTRIBUTE in d:
        attr = str(d[BatonClient.ATTRIBUTE])
        value = d[BatonClient.VALUE]
        units = d.get(BatonClient.UNITS, None)

        if attr.find(AVU.SEPARATOR) >= 0:  # Has namespace
            (ns, _, bare_attr) = attr.partition(AVU.SEPARATOR)

            # This accepts an attribute with a namespace that is the empty
            # string i.e. ":foo" or is whitespace i.e. " :foo" and discards
            # the namespace.
            if not ns.strip():
                ns = None

            return AVU(bare_attr, value, units, namespace=ns)

        return AVU(attr, value, units)

    # Match an access permission sub-document
    if BatonClient.OWNER in d and BatonClient.LEVEL in d:
        user = d[BatonClient.OWNER]
        zone = d[BatonClient.ZONE]
        level = d[BatonClient.LEVEL]

        return AC(user, Permission[level.upper()], zone=zone)

    return d


class BatonClient(object):
    """A wrapper around the baton client program, used for interacting with
     iRODS."""

    AVUS = "avus"
    ATTRIBUTE = "attribute"
    VALUE = "value"
    UNITS = "units"

    COLL = "collection"
    OBJ = "data_object"
    ZONE = "zone"

    ACCESS = "access"
    OWNER = "owner"
    LEVEL = "level"

    ADD = "add"
    CHMOD = "chmod"
    REM = "rem"
    LIST = "list"
    METAQUERY = "metaquery"
    METAMOD = "metamod"

    OP = "operation"
    ARGS = "arguments"
    TARGET = "target"

    RESULT = "result"
    SINGLE = "single"
    MULTIPLE = "multiple"
    CONTENTS = "contents"

    ERR = "error"
    MSG = "message"
    CODE = "code"

    def __init__(self):
        self.proc = None

    def is_running(self) -> bool:
        """Returns true if the client is running."""
        return self.proc and self.proc.poll() is None

    def start(self):
        """Starts the client if it is not already running."""
        if self.is_running():
            log.warning("Tried to start a BatonClient that is already running")
            return

        self.proc = subprocess.Popen(['baton-do', '--unbuffered'],
                                     bufsize=0,
                                     stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        log.debug("Started a new baton-do process "
                  "with PID {}".format(self.proc.pid))

    def stop(self):
        """Stops the client if it is running."""
        if not self.is_running():
            log.warning("Tried to start a BatonClient that is not running")
            return

        self.proc.stdin.close()
        try:
            log.debug("Terminating baton-do PID {}".format(self.proc.pid))
            self.proc.terminate()
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            log.error("Failed to terminate baton-do PID {}; "
                      "killing".format(self.proc.pid))
            self.proc.kill()
        self.proc = None

    def list(self, item: Dict, acl=False, avu=False, contents=False,
             recurse=False, size=False, timestamp=False) -> List[Dict]:
        if recurse:
            raise NotImplementedError("recurse")

        args = {"acl": acl, "avu": avu, "contents": contents,
                "size": size, "timestamp": timestamp}

        result = self._execute(BatonClient.LIST, args, item)
        if contents:
            result = result[BatonClient.CONTENTS]
        else:
            result = [result]

        return result

    def meta_add(self, item: Dict):
        args = {BatonClient.OP: BatonClient.ADD}
        self._execute(BatonClient.METAMOD, args, item)

    def meta_rem(self, item: Dict):
        args = {BatonClient.OP: BatonClient.REM}
        self._execute(BatonClient.METAMOD, args, item)

    def meta_query(self, avus: List[AVU],
                   zone=None,
                   collection=False,
                   data_object=False) -> List[Union[DataObject, Collection]]:
        args = {}
        if collection:
            args["collection"] = True
        if data_object:
            args["object"] = True

        item = {BatonClient.AVUS: avus}
        if zone:
            item[BatonClient.COLL] = self._zone_hint_to_path(zone)

        result = self._execute(BatonClient.METAQUERY, args, item)
        items = [make_rods_item(self, item) for item in result]
        items.sort()

        return items

    def ac_set(self, item: Dict, recurse=False):
        args = {"recurse": recurse}
        self._execute(BatonClient.CHMOD, args, item)

    def _execute(self, operation: str, args: Dict, item: Dict) -> Dict:
        if not self.is_running():
            log.debug("baton-do is not running ... starting")
            self.start()
            if not self.is_running():
                raise BatonError("baton-do failed to start")

        response = self._send(self._wrap(operation, args, item))
        return self._unwrap(response)

    @staticmethod
    def _wrap(operation: str, args: Dict, item: Dict) -> Dict:
        return {BatonClient.OP: operation,
                BatonClient.ARGS: args,
                BatonClient.TARGET: item}

    @staticmethod
    def _unwrap(envelope: Dict) -> Dict:
        if BatonClient.ERR in envelope:
            err = envelope[BatonClient.ERR]
            raise RodsError(err[BatonClient.MSG], err[BatonClient.CODE])

        if BatonClient.RESULT not in envelope:
            raise BatonError("invalid {} operation result "
                             "(no result)".format(envelope[BatonClient.OP]),
                             -1)

        if BatonClient.SINGLE in envelope[BatonClient.RESULT]:
            return envelope[BatonClient.RESULT][BatonClient.SINGLE]

        if BatonClient.MULTIPLE in envelope[BatonClient.RESULT]:
            return envelope[BatonClient.RESULT][BatonClient.MULTIPLE]

        raise BatonError("Invalid {} operation result "
                         "(no content)".format(envelope), -1)

    def _send(self, envelope: Dict) -> Dict:
        encoded = json.dumps(envelope, cls=BatonJSONEncoder)
        log.debug("Sending {}".format(encoded))

        msg = bytes(encoded, 'utf-8')
        self.proc.stdin.write(msg)
        self.proc.stdin.flush()

        resp = self.proc.stdout.readline()
        log.debug("Received {}".format(resp))

        return json.loads(resp, object_hook=as_baton)

    @staticmethod
    def _zone_hint_to_path(zone) -> str:
        z = str(zone)
        if z.startswith("/"):
            return z

        return "/" + z


class RodsItem(PathLike):
    """A base class for iRODS path entities."""

    def __init__(self, client: BatonClient, path: Union[PurePath, str]):
        self.client = client
        self.path = PurePath(path)

    def exists(self) -> bool:
        """Return true if the item exists in iRODS."""
        try:
            self._list()
        except RodsError as re:
            if re.code == -310000:
                return False
        return True

    def meta_add(self, *avus: Union[AVU, Tuple[AVU]]) -> int:
        """Add AVUs to the item's metadata, if they are not already present.
        Return the number of AVUs added.

        Args:
            *avus: AVUs to add.

        Returns: int
        """
        current = self.metadata()
        to_add = sorted(list(set(avus).difference(current)))

        if to_add:
            log.debug("Adding AVUs to {}: {}".format(self.path, to_add))
            item = self._to_dict()
            item[BatonClient.AVUS] = to_add
            self.client.meta_add(item)

        return len(to_add)

    def meta_remove(self, *avus: Union[AVU, Tuple[AVU]]) -> int:
        """Remove AVUs from the item's metadata, if they are present.
        Return the number of AVUs removed.

        Args:
            *avus: AVUs to remove.

        Returns: int
        """
        current = self.metadata()
        to_remove = sorted(list(set(current).intersection(avus)))

        if to_remove:
            log.debug("Adding AVUs from {}: {}".format(self.path, to_remove))
            item = self._to_dict()
            item[BatonClient.AVUS] = to_remove
            self.client.meta_rem(item)

        return len(to_remove)

    def meta_supersede(self, *avus: Union[AVU, Tuple[AVU]],
                       history=False, history_date=None) -> Tuple[int, int]:
        """Remove AVUs from the item's metadata that share an attribute with
         any of the argument AVUs and add the argument AVUs to the item's
         metadata. Return the numbers of AVUs added and removed, including any
         history AVUs created.

         Args:
             avus: AVUs to add in place of existing AVUs sharing those
             attributes.
             history: Create history AVUs describing any AVUs removed when
             superseding. See AVU.history.
             history_date: A datetime to be embedded as part of the history
             AVU values.

        Returns: Tuple[int, int]
        """
        if history_date is None:
            history_date = datetime.utcnow()

        current = self.metadata()
        log.debug("Superseding AVUs of {}; current: {} "
                  "new {}".format(self.path, current, avus))

        rem_attrs = set(map(lambda avu: avu.attribute, avus))
        to_remove = set(filter(lambda a: a.attribute in rem_attrs, current))

        # If the argument AVUs have some of the AVUs to remove amongst them,
        # we don't want to remove them from the item, just to add them back.
        to_remove.difference_update(avus)
        to_remove = sorted(list(to_remove))
        if to_remove:
            log.debug("Removing AVUs from {}: {}".format(self.path, to_remove))
            item = self._to_dict()
            item[BatonClient.AVUS] = to_remove
            self.client.meta_rem(item)

        to_add = sorted(list(set(avus).difference(current)))
        if history:
            hist = []
            for avus in AVU.collate(*to_remove).values():
                hist.append(AVU.history(*avus, history_date=history_date))
            to_add += hist

        if to_add:
            log.debug("Adding AVUs to {}: {}".format(self.path, to_add))
            item = self._to_dict()
            item[BatonClient.AVUS] = to_add
            self.client.meta_add(item)

        return len(to_remove), len(to_add)

    def ac_add(self, *acs: Union[AC, Tuple[AC]], recurse=False) -> int:
        """Add access controls to the item. Return the number of access
        controls added. If some of the argument access controls are already
        present, those arguments will be ignored.

        Args:
            acs: Access controls.
            recurse: Recursively add access control.

        Returns: int
        """
        current = self.acl()
        to_add = sorted(list(set(acs).difference(current)))
        if to_add:
            log.debug("Adding ACL to {}: {}".format(self.path, to_add))
            item = self._to_dict()
            item[BatonClient.ACCESS] = to_add
            self.client.ac_set(item, recurse=recurse)

        return len(to_add)

    def ac_rem(self, *acs: Union[AC, Tuple[AC]], recurse=False) -> int:
        """Remove access controls from the item. Return the number of access
        controls removed. If some of the argument access controls are not
        present, those arguments will be ignored.

        Args:
            acs: Access controls.
            recurse: Recursively add access control.

        Returns: int
        """
        current = self.acl()
        to_remove = sorted(list(set(current).intersection(acs)))
        if to_remove:
            log.debug("Removing ACL from {}: {}".format(self.path, to_remove))

            # In iRODS we "remove" permissions by setting them to NULL
            for ac in to_remove:
                ac.perm = Permission.NULL

            item = self._to_dict()
            item[BatonClient.ACCESS] = to_remove
            self.client.ac_set(item, recurse=recurse)

        return len(to_remove)

    def ac_supersede(self, *acs: Union[AC, Tuple[AC]],
                     recurse=False) -> Tuple[int, int]:
        """Remove all access controls from the item, replacing them with the
        specified access controls. Return the numbers of access controls
        removed and added.


        """
        current = self.acl()
        log.debug("Superseding ACL of {}; current: {} "
                  "new {}".format(self.path, current, acs))

        to_remove = sorted(list(set(current).difference(acs)))
        if to_remove:
            log.debug("Removing ACL from {}: {}".format(self.path, to_remove))

            # In iRODS we "remove" permissions by setting them to NULL
            for ac in to_remove:
                ac.perm = Permission.NULL

            item = self._to_dict()
            item[BatonClient.ACCESS] = to_remove
            self.client.ac_set(item, recurse=recurse)

        to_add = sorted(list(set(acs).difference(current)))
        if to_add:
            log.debug("Adding ACL to {}: {}".format(self.path, to_add))
            item = self._to_dict()
            item[BatonClient.ACCESS] = to_add
            self.client.ac_set(item, recurse=recurse)

        return len(to_remove), len(to_add)

    def metadata(self) -> List[AVU]:
        """Return the item's metadata.

        Returns: List[AVU]
        """
        item = self._list(avu=True).pop()
        if BatonClient.AVUS not in item.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.AVUS, item))
        return sorted(item[BatonClient.AVUS])

    def acl(self) -> List[AC]:
        """Return the item's Access Control List (ACL).

        Returns: List[AC]"""
        item = self._list(acl=True).pop()
        if BatonClient.ACCESS not in item.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.ACCESS, item))
        return sorted(item[BatonClient.ACCESS])

    @abstractmethod
    def _to_dict(self):
        pass

    @abstractmethod
    def _list(self, **kwargs):
        pass


class DataObject(RodsItem):
    """An iRODS data object."""

    def __init__(self, client, remote_path: Union[PurePath, str]):
        super().__init__(client, PurePath(remote_path).parent)
        self.name = PurePath(remote_path).name

    def list(self) -> DataObject:
        """Return a new DataObject representing this one.

        Returns: DataObject
        """
        item = self._list().pop()
        if BatonClient.OBJ not in item.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.OBJ, item))

        return make_rods_item(self.client, item)

    def _list(self, **kwargs) -> List[dict]:
        item = self._to_dict()
        return self.client.list(item, **kwargs)

    def _to_dict(self) -> Dict:
        return {BatonClient.COLL: self.path, BatonClient.OBJ: self.name}

    def __eq__(self, other):
        if not isinstance(other, DataObject):
            return False

        return self.path == other.path and self.name == other.name

    def __fspath__(self):
        return self.__repr__()

    def __repr__(self):
        return PurePath(self.path, self.name).as_posix()


class Collection(RodsItem):
    """An iRODS collection."""

    def __init__(self, client: BatonClient, path: Union[PurePath, str]):
        super().__init__(client, path)

    def contents(self,
                 acl=False,
                 avu=False,
                 recurse=False) -> List[Union[DataObject, Collection]]:
        """Return list of the Collection contents.

        Keyword Args:
          acl: Include ACL information.
          avu: Include AVU (metadata) information.
          recurse: Recurse into sub-collections. NOT IMPLEMENTED.

        Returns: List[Union[DataObject, Collection]]
        """
        items = self._list(acl=acl, avu=avu, contents=True, recurse=recurse)

        return [make_rods_item(self.client, item) for item in items]

    def list(self, acl=False, avu=False) -> Collection:
        """Return a new Collection representing this one.

        Keyword Args:
          acl: Include ACL information.
          avu: Include AVU (metadata) information.

        Returns: Collection
        """
        items = self._list(acl=acl, avu=avu)
        # Gets a single item
        return make_rods_item(self.client, items.pop())

    def _list(self, **kwargs) -> List[dict]:
        return self.client.list({BatonClient.COLL: self.path}, **kwargs)

    def _to_dict(self):
        return {BatonClient.COLL: self.path}

    def __eq__(self, other):
        if not isinstance(other, Collection):
            return False

        return self.path == other.path

    def __fspath__(self):
        return self.__repr__()

    def __repr__(self):
        return self.path.as_posix()


def make_rods_item(client: BatonClient,
                   item: Dict) -> Union[DataObject, Collection]:
    """Create a new Collection or DataObject as appropriate for a dictionary
    returned by a BatonClient.

    Returns: Union[DataObject, Collection]
    """
    if BatonClient.COLL not in item.keys():
        raise BatonError("{} key missing "
                         "from {}".format(BatonClient.COLL, item))

    if BatonClient.OBJ in item.keys():
        return DataObject(client, PurePath(item[BatonClient.COLL],
                                           item[BatonClient.OBJ]))
    return Collection(client, PurePath(item[BatonClient.COLL]))


def have_admin() -> bool:
    """Returns true if the current user has iRODS admin capability."""
    cmd = ["iadmin", "lu"]
    try:
        _run(cmd)
        return True
    except RodsError:
        return False


def mkgroup(name: str):
    cmd = ["iadmin", "mkgroup", name]
    _run(cmd)


def rmgroup(name: str):
    cmd = ["iadmin", "rmgroup", name]
    _run(cmd)


def imkdir(remote_path: Union[PurePath, str], make_parents=True):
    cmd = ["imkdir"]
    if make_parents:
        cmd.append("-p")

    cmd.append(remote_path)
    _run(cmd)


def iget(remote_path: Union[PurePath, str], local_path: Union[PurePath, str],
         force=False, verify_checksum=True, recurse=False):
    cmd = ["iget"]
    if force:
        cmd.append("-f")
    if verify_checksum:
        cmd.append("-K")
    if recurse:
        cmd.append("-r")

    cmd.append(remote_path)
    cmd.append(local_path)
    _run(cmd)


def iput(local_path: Union[PurePath, str], remote_path: Union[PurePath, str],
         force=False, verify_checksum=True, recurse=False):
    cmd = ["iput"]
    if force:
        cmd.append("-f")
    if verify_checksum:
        cmd.append("-K")
    if recurse:
        cmd.append("-r")

    cmd.append(local_path)
    cmd.append(remote_path)
    _run(cmd)


def irm(remote_path: Union[PurePath, str], force=False, recurse=False):
    cmd = ["irm"]
    if force:
        cmd.append("-f")
    if recurse:
        cmd.append("-r")

    cmd.append(remote_path)
    _run(cmd)


def _run(cmd: List[str]):
    log.debug("Running {}".format(cmd))

    completed = subprocess.run(cmd, capture_output=True)
    if completed.returncode == 0:
        return

    raise RodsError(completed.stderr.decode("utf-8").rstrip(), 0)
