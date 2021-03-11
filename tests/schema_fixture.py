from configparser import ConfigParser
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from workbot import ConfigurationError
from workbot.schema import WorkBotDBBase, initialize_database


@pytest.fixture(scope="function", params=["sqlite", "mysql"])
def wb_session(request, config, tmp_path):
    """Returns a WorkBot database session for testing."""

    url = None
    if request.param == "mysql":
        url = mysql_url(config)
    elif request.param == "sqlite":
        url = sqlite_url(tmp_path)
    else:
        pytest.fail("Unknown database platform %s", request.param)

    engine = create_engine(url, echo=False)
    if not database_exists(engine.url):
        create_database(engine.url)

    WorkBotDBBase.metadata.create_all(engine)

    session_maker = sessionmaker(bind=engine)
    sess = session_maker()
    initialize_database(sess)
    sess.commit()

    try:
        yield sess
    finally:
        sess.close()

        # This is for the benefit of MySQL where we have a schema reused for
        # a number of tests. Without using sqlalchemy-utils, one would call:
        #
        #   for t in reversed(meta.sorted_tables):
        #       t.drop(engine)
        #
        # Dropping the database for SQLite deletes the SQLite file.
        drop_database(engine.url)


def mysql_url(config: ConfigParser):
    """Returns a MySQL URL configured through an ini file.

    The required keys and values are:

    [MySQL]
    user       = <database user, defaults to "workbot">
    password   = <database password, defaults to empty i.e. "">
    ip_address = <database IP address, defaults to "127.0.0.1">
    port       = <database port, defaults to 3306>
    schema     = <database schema, defaults to "workbot">

    """
    section = "MySQL"

    if section not in config.sections():
        raise ConfigurationError("The {} configuration section is missing. "
                                 "You need to fill this in before running "
                                 "tests on a {} database".format(section,
                                                                 section))
    connection_conf = config[section]
    user = connection_conf.get("user", "workbot")
    password = connection_conf.get("password", "")
    ip_address = connection_conf.get("ip_address", "127.0.0.1")
    port = connection_conf.get("port", "3306")
    schema = connection_conf.get("schema", "workbot")

    uri = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password,
                                                  ip_address, port, schema)
    return uri


def sqlite_url(tmp_path: Path):
    """Returns an SQLite URL configured to a temporary path."""
    p = tmp_path / "workbot"
    uri = 'sqlite:///{}'.format(p)
    return uri
