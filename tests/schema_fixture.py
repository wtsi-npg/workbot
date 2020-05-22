import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from workbot.schema import WorkBotDBBase, initialize_database


@pytest.fixture(scope="function")
def wb_session(tmp_path):
    p = tmp_path / "workbot"
    uri = 'sqlite:///{}'.format(p)

    engine = create_engine(uri, echo=False)
    WorkBotDBBase.metadata.create_all(engine)

    session_maker = sessionmaker(bind=engine)
    sess = session_maker()

    initialize_database(sess)
    sess.commit()

    yield sess
    sess.close()
