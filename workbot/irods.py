# -*- coding: utf-8 -*-
#
# Copyright © 2020 Genome Research Ltd. All rights reserved.
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

import json
import logging
import subprocess
from abc import ABCMeta, abstractmethod
from pathlib import PurePath
from typing import Any, Dict, List, Union

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


class AVU(object):
    """AVU is an iRODS attribute, value , units tuple."""

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

    def __eq__(self, other):
        return (isinstance(other, AVU) and
                self.namespace == other.namespace and
                self.attribute == other.attribute and
                self.value == other.value and
                self.units == other.units)

    def __str__(self):
        return "<AVU '{}' = '{}' {}>".format(self.attribute, self.value,
                                             self.units)


class BatonJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, AVU):
            enc = {"attribute": o.attribute, "value": o.value}
            if o.units:
                enc["units"] = o.units
            return enc
        if isinstance(o, PurePath):
            return o.as_posix()


def as_baton(d: Dict):
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

    def is_running(self):
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
             recurse=False, size=False, timestamp=False):
        if recurse:
            raise NotImplementedError("recurse")

        args = {"acl": acl, "avu": avu, "contents": contents,
                "size": size, "timestamp": timestamp}

        result = self._execute(BatonClient.LIST, args, item)
        if contents:
            contents = result[BatonClient.CONTENTS]
            result = [self._item_to_path(x) for x in contents]

        return result

    def meta_add(self, item: Dict):
        args = {BatonClient.OP: BatonClient.ADD}
        self._execute(BatonClient.METAMOD, args, item)

    def meta_rem(self, item: Dict):
        args = {BatonClient.OP: BatonClient.REM}
        self._execute(BatonClient.METAMOD, args, item)

    def meta_query(self, avus: List[AVU], zone=None,
                   collection=False, data_object=False):
        args = {}
        if collection:
            args["collection"] = True
        if data_object:
            args["object"] = True

        item = {BatonClient.AVUS: avus}
        if zone:
            item[BatonClient.COLL] = zone  # Zone hint

        result = self._execute(BatonClient.METAQUERY, args, item)

        return [self._item_to_path(x) for x in result]

    def _execute(self, operation: str, args: Dict, item: Dict):
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
    def _item_to_path(item: Dict) -> str:
        if BatonClient.OBJ in item:
            return PurePath(item[BatonClient.COLL], item[BatonClient.OBJ])
        return PurePath(item[BatonClient.COLL])


class RodsItem(object, metaclass=ABCMeta):
    """A base class for iRODS path entities."""

    def __init__(self, client: BatonClient, path: Union[PurePath, str]):
        self.client = client
        self.path = PurePath(path)

    def exists(self):
        try:
            self._list()
        except RodsError as re:
            if re.code == -310000:
                return False
        return True

    def meta_add(self, *avus) -> int:
        current = self.metadata()
        to_add = []

        for avu in avus:
            if avu not in current:
                to_add.append(avu)

        if to_add:
            item = self._to_dict()
            item[BatonClient.AVUS] = to_add
            self.client.meta_add(item)

        return len(to_add)

    def metadata(self):
        val = self._list(avu=True)
        if BatonClient.AVUS not in val.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.AVUS, val))
        return val[BatonClient.AVUS]

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

    def list(self):
        val = self._list()
        if BatonClient.COLL not in val.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.COLL, val))
        if BatonClient.OBJ not in val.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.OBJ, val))

        return PurePath(val[BatonClient.COLL], val[BatonClient.OBJ])

    def _list(self, **kwargs):
        item = self._to_dict()
        return self.client.list(item, **kwargs)

    def _to_dict(self):
        return {BatonClient.COLL: self.path, BatonClient.OBJ: self.name}

    def __repr__(self):
        return "<Data object: {}/{}>".format(self.path, self.name)


class Collection(RodsItem):
    """An iRODS collection."""

    def __init__(self, client: BatonClient, path: Union[PurePath, str]):
        super().__init__(client, path)

    def list(self, acl=False, avu=False, contents=False, recurse=False):
        val = self._list(acl=acl, avu=avu, contents=contents, recurse=recurse)

        # Gets a list
        if contents:
            return val

        # Gets a single item
        if BatonClient.COLL not in val.keys():
            raise BatonError("{} key missing "
                             "from {}".format(BatonClient.COLL, val))

        return PurePath(val[BatonClient.COLL])

    def _list(self, **kwargs):
        return self.client.list({BatonClient.COLL: self.path}, **kwargs)

    def _to_dict(self):
        return {BatonClient.COLL: self.path}

    def __repr__(self):
        return "<Collection: {}>".format(self.path)


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
