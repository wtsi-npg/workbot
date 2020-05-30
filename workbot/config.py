# Work types
import configparser
import os
import pwd
from pathlib import Path

ARTIC_NEXTFLOW_WORKTYPE = "ARTIC NextFlow"

# Work states
PENDING_STATE = "Pending"
STAGED_STATE = "Staged"


STARTED_STATE = "Started"
SUCCEEDED_STATE = "Succeded"
ARCHIVED_STATE = "Archived"
ANNOTATED_STATE = "Annotated"
UNSTAGED_STATE = "Unstaged"
COMPLETED_STATE = "Complete"

FAILED_STATE = "Failed"
CANCELLED_STATE = "Cancelled"


def get_config_paths():
    config_file = "workbot.ini"
    config_dir = "workbot"
    dot_config_dir = "." + config_dir

    user = pwd.getpwuid(os.getuid()).pw_name
    home = os.getenv("HOME", os.path.join("home", user))
    xdg_data_home = os.getenv("XDG_DATA_HOME",
                              os.path.join(home, ".local", "share"))

    override_path = os.environ.get("WORKBOT_CONFIG")

    paths = []
    if override_path:
        paths.append(override_path)

    paths.append(os.path.join(os.getcwd(), config_file))

    if xdg_data_home:
        paths.append(os.path.join(xdg_data_home, config_dir, config_file))
    if home:
        paths.append(os.path.join(home, dot_config_dir, config_file))

    return paths


def read_config_file():
    search = get_config_paths()

    for p in search:
        f = Path(p)
        if f.is_file():
            config = configparser.ConfigParser()
            config.read(f)
            return config

    raise FileNotFoundError("No configuration file found "
                            "in: {}".format(search))
