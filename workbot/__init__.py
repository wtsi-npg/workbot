import os
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from workbot.ml_warehouse_schema import MLWHBase
from workbot.schema import WorkBotDBBase


class ConfigurationError(Exception):
    """Exception raised for errors in the configuration or environment."""
    def __init__(self, message: str):
        self.message = message


def _init_workbot_db():
    uri = os.environ.get("WBDB_URI")
    if not uri:
        return None

    engine = create_engine(uri, echo=False)
    WorkBotDBBase.metadata.create_all(engine)

    return sessionmaker(bind=engine)


def _init_mlwh_db():
    uri = os.environ.get("MLWH_URI")
    if not uri:
        return None

    engine = create_engine(uri, echo=True)
    MLWHBase.metadata.create_all(engine)

    return sessionmaker(bind=engine)

# Could use scoped_session to get thread-local sessions and avoid this. See
# https://docs.sqlalchemy.org/en/13/orm/contextual.html#\
# sqlalchemy.orm.scoping.scoped_session


wb_lock = threading.Lock()
WBSession = _init_workbot_db()

wh_lock = threading.Lock()
WHSession = _init_mlwh_db()


def get_wb_session() -> Session:
    """Get a new SQL session for the WorkBot database from the factory. This
    function ensures thread safe access to the SQLAlchemy database engine.

    Returns: Session

    """
    if WBSession is None:
        raise ConfigurationError("The WBDB_URI environment variable is not "
                                 "set. This should be set to the database "
                                 "connection URI of the Workbot database")
    with wb_lock:
        return WBSession()


def get_wh_session() -> Session:
    """Get a new SQL session for the ML warehouse database from the factory.
    This function ensures thread safe access to the SQLAlchemy database engine.

    Returns: Session

    """
    if WHSession is None:
        raise ConfigurationError("The MLWH_URI environment is variable not "
                                 "set. This should be set to the database "
                                 "connection URI of the ML warehouse")

    with wh_lock:
        return WHSession()
