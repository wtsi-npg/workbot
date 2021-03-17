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
        return "<RodsError: {} - {}>".format(self.code, self.message)


class BatonError(Exception):
    pass


@total_ordering
class AVU(object):
    """AVU is an iRODS attribute, value , units tuple.

    AVUs may be sorted, where they will sorted lexically, first by
    namespace (if present), then by attribute, then by value and finally by
    units (if present).
    """

    SEPARATOR = ":"
    """The attribute namespace separator"""

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
            enc = {"attribute": o.attribute, "value": o.value}
            if o.units:
                enc["units"] = o.units
            return enc
        if isinstance(o, PurePath):
            return o.as_posix()


def as_baton(d: Dict) -> Any:
    """Object hook for decoding baton JSON."""
    if "attribute" in d:
        attr = str(d["attribute"])
        value = d["value"]
        units = d.get("units", None)

        if attr.find(AVU.SEPARATOR) >= 0:  # Has namespace
            (ns, _, bare_attr) = attr.partition(AVU.SEPARATOR)

            # This accepts an attribute with a namespace that is the empty
            # string i.e. ":foo" or is whitespace i.e. " :foo" and discards
            # the namespace.
            if not ns.strip():
                ns = None

            return AVU(bare_attr, value, units, namespace=ns)

        return AVU(attr, value, units)

    return d


class BatonClient(object):
    """A wrapper around the baton client program, used for interacting with
     iRODS."""

    ACL = "acl"
    AVUS = "avus"
    COLL = "collection"
    OBJ = "data_object"

    ADD = "add"
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
        to_add = set(avus).difference(current)

        if to_add:
            item = self._to_dict()
            item[BatonClient.AVUS] = list(to_add)
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
        to_remove = set(current).intersection(avus)

        if to_remove:
            item = self._to_dict()
            item[BatonClient.AVUS] = list(to_remove)
            self.client.meta_rem(item)

        return len(to_remove)

    def meta_supersede(self, *avus: Union[AVU, Tuple[AVU]]) -> Tuple[int, int]:
        """Remove AVUs from the item's metadata that share an attribute with
         any of the argument AVUs and add the argument AVUs to the item's
         metadata. Return the numbers of AVUs added and removed."""
        current = self.metadata()

        rem_attrs = set(map(lambda avu: avu.attribute, avus))
        to_remove = set(filter(lambda a: a.attribute in rem_attrs, current))

        # If the argument AVUs have some of the AVUs to remove amongst them,
        # we don't want to remove them from the item, just to add them back.
        to_remove.difference_update(avus)

        if to_remove:
            item = self._to_dict()
            item[BatonClient.AVUS] = list(to_remove)
            self.client.meta_rem(item)

        to_add = set(avus).difference(current)

        if to_add:
            item = self._to_dict()
            item[BatonClient.AVUS] = list(to_add)
            self.client.meta_add(item)

        return len(to_remove), len(to_add)

    def metadata(self) -> List[AVU]:
        """Return the item's metadata.

        Returns: List[AVU]
        """
        item = self._list(avu=True).pop()
        if BatonClient.AVUS not in item.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.AVUS, item))
        avus = item[BatonClient.AVUS]
        avus.sort()

        return avus

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
