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

    def list(self, item: Dict, acl=False, avu=False, contents=False,
             recurse=False, size=False, timestamp=False):
        if recurse:
            raise NotImplementedError("recurse")

        args = {"acl": acl, "avu": avu, "contents": contents,
                "size": size, "timestamp": timestamp}

        return self.execute("list", args, item)

    def meta_add(self, item: Dict):
        args = {"operation": "add"}
        self.execute("metamod", args, item)

    def meta_rem(self, item: Dict):
        args = {"operation": "rem"}
        self.execute("metamod", args, item)

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

        result = self.execute("metaquery", args, item)

        if collection:
            return [x["collection"] for x in result]
        if data_object:
            return [x["collection"] + "/" + x["data_object"] for x in result]

        return []

    def execute(self, operation: str, args: Dict, item: Dict):
        if not self.is_running():
            raise BatonError("client is not running")

        response = self.send(self.wrap(operation, args, item))
        return self.unwrap(response)

    def wrap(self, operation: str, args: Dict, item: Dict) -> Dict:
        return {"operation": operation,
                "arguments": args,
                "target": item}

    def unwrap(self, envelope: Dict) -> Dict:
        if "error" in envelope:
            err = envelope["error"]
            raise RodsError(err["message"], err["code"])

        if "result" not in envelope:
            raise RodsError("invalid {} operation result "
                            "(no result)".format(envelope["operation"]), -1)

        if "single" in envelope["result"]:
            return envelope["result"]["single"]

        if "multiple" in envelope["result"]:
            return envelope["result"]["multiple"]

        raise RodsError("Invalid {} operation result "
                        "(no content)".format(envelope), -1)

    def send(self, envelope: Dict) -> Dict:
        encoded = json.dumps(envelope)
        log.debug("Sending {}".format(encoded))

        msg = bytes(encoded, 'utf-8')
        self.proc.stdin.write(msg)
        self.proc.stdin.flush()

        resp = self.proc.stdout.readline()
        log.debug("Received {}".format(resp))

        return json.loads(resp)


class RodsItem(object):
    def __init__(self, client: BatonClient, path: str):
        self.client = client
        self.path = path

    def _to_dict(self):
        raise NotImplementedError

    def _list(self, **kwargs):
        raise NotImplementedError

    def exists(self):
        try:
            self._list()
        except RodsError as re:
            if re.code == -310000:
                return False
        return True

    def meta_add(self, *avus):
        item = self._to_dict()
        item["avus"] = avus
        self.client.meta_add(item)

    def metadata(self):
        val = self._list(avu=True)
        if "avus" not in val.keys():
            raise BatonError("avus key missing from {}".format(val))
        return val["avus"]


class DataObject(RodsItem):
    def __init__(self, client, remote_path: str):
        super().__init__(client, dirname(remote_path))
        self.name = basename(remote_path)

    def _to_dict(self):
        return {"collection": self.path, "data_object": self.name}

    def _list(self, **kwargs):
        item = self._to_dict()
        return self.client.list(item, **kwargs)

    def list(self):
        val = self._list()
        if "collection" not in val.keys():
            raise BatonError("collection key missing from {}".format(val))
        if "data_object" not in val.keys():
            raise BatonError("data_object key missing from {}".format(val))

        return val["collection"] + "/" + val["data_object"]


class Collection(RodsItem):
    def __init__(self, client: BatonClient, path: str):
        self.client = client
        self.path = path

    def _to_dict(self):
        return {"collection": self.path}

    def _list(self, **kwargs):
        return self.client.list({"collection": self.path}, **kwargs)

    def list(self):
        val = self._list()
        if "collection" not in val.keys():
            raise BatonError("collection key missing from {}".format(val))

        return val["collection"]


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
    try:
        log.debug("Running {}".format(cmd))
        subprocess.run(cmd, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        log.error("{}".format(e.stderr.decode("utf-8")))
        raise e
