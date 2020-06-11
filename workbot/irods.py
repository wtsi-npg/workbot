import logging
import json
import subprocess

from os.path import dirname, basename
from typing import Dict, List

log = logging.getLogger(__package__)


class RodsError(Exception):
    def __init__(self,
                 message: str,
                 code: int):
        self.message = message
        self.code = code

    def __repr__(self):
        return "<RodsError: {} - {}>".format(self.code, self.message)


class BatonError(Exception):
    pass


class BatonClient(object):

    def __init__(self):
        self.proc = None

    def is_running(self):
        return self.proc and self.proc.poll() is None

    def start(self):
        if self.is_running():
            log.warning("Tried to start a BatonClient that is already running")
            return

        self.proc = subprocess.Popen(['baton-do', '--unbuffered'],
                                     bufsize=0,
                                     stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        log.debug("Started a new baton-do process with PID {}".format(
            self.proc.pid))

    def stop(self):
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

        result = self._execute("list", args, item)
        if contents:
            contents = result["contents"]
            result = [self._item_to_path(x) for x in contents]

        return result

    def meta_add(self, item: Dict):
        args = {"operation": "add"}
        self._execute("metamod", args, item)

    def meta_rem(self, item: Dict):
        args = {"operation": "rem"}
        self._execute("metamod", args, item)

    def meta_query(self, avus: List, zone=None,
                   collection=False, data_object=False):
        args = {}
        if collection:
            args["collection"] = True
        if data_object:
            args["object"] = True

        item = {"avus": avus}
        if zone:
            item["collection"] = zone  # Zone hint

        result = self._execute("metaquery", args, item)

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
        return {"operation": operation,
                "arguments": args,
                "target": item}

    @staticmethod
    def _unwrap(envelope: Dict) -> Dict:
        if "error" in envelope:
            err = envelope["error"]
            raise RodsError(err["message"], err["code"])

        if "result" not in envelope:
            raise BatonError("invalid {} operation result "
                             "(no result)".format(envelope["operation"]), -1)

        if "single" in envelope["result"]:
            return envelope["result"]["single"]

        if "multiple" in envelope["result"]:
            return envelope["result"]["multiple"]

        raise BatonError("Invalid {} operation result "
                         "(no content)".format(envelope), -1)

    def _send(self, envelope: Dict) -> Dict:
        encoded = json.dumps(envelope)
        log.debug("Sending {}".format(encoded))

        msg = bytes(encoded, 'utf-8')
        self.proc.stdin.write(msg)
        self.proc.stdin.flush()

        resp = self.proc.stdout.readline()
        log.debug("Received {}".format(resp))

        return json.loads(resp)

    @staticmethod
    def _item_to_path(item: Dict) -> str:
        if "data_object" in item:
            return item["collection"] + "/" + item["data_object"]
        return item["collection"]


class RodsItem(object):
    def __init__(self, client: BatonClient, path: str):
        self.client = client
        self.path = path

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
            item["avus"] = to_add
            self.client.meta_add(item)

        return len(to_add)

    def metadata(self):
        val = self._list(avu=True)
        if "avus" not in val.keys():
            raise BatonError("avus key missing from {}".format(val))
        return val["avus"]

    def _to_dict(self):
        raise NotImplementedError

    def _list(self, **kwargs):
        raise NotImplementedError


class DataObject(RodsItem):
    def __init__(self, client, remote_path: str):
        super().__init__(client, dirname(remote_path))
        self.name = basename(remote_path)

    def list(self):
        val = self._list()
        if "collection" not in val.keys():
            raise BatonError("collection key missing from {}".format(val))
        if "data_object" not in val.keys():
            raise BatonError("data_object key missing from {}".format(val))

        return val["collection"] + "/" + val["data_object"]

    def _list(self, **kwargs):
        item = self._to_dict()
        return self.client.list(item, **kwargs)

    def _to_dict(self):
        return {"collection": self.path, "data_object": self.name}

    def __repr__(self):
        return "<Data object: {}/{}>".format(self.path, self.name)


class Collection(RodsItem):
    def __init__(self, client: BatonClient, path: str):
        self.client = client
        self.path = path

    def list(self, acl=False, avu=False, contents=False, recurse=False):
        val = self._list(acl=acl, avu=avu, contents=contents, recurse=recurse)

        # Gets a list
        if contents:
            return val

        # Gets a single item
        if "collection" not in val.keys():
            raise BatonError("collection key missing from {}".format(val))

        return val["collection"]

    def _list(self, **kwargs):
        return self.client.list({"collection": self.path}, **kwargs)

    def _to_dict(self):
        return {"collection": self.path}

    def __repr__(self):
        return "<Collection: {}>".format(self.path)


def imkdir(remote_path: str, make_parents=True):
    cmd = ["imkdir"]
    if make_parents:
        cmd.append("-p")

    cmd.append(remote_path)
    _run(cmd)


def iget(remote_path: str, local_path: str, force=False, verify_checksum=True,
         recurse=False):
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


def iput(local_path: str, remote_path: str, force=False, verify_checksum=True,
         recurse=False):
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


def irm(remote_path: str, force=False, recurse=False):
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

