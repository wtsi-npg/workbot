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

import os
from pathlib import Path, PurePath
from typing import List, Union

import sqlalchemy
from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship
from sqlalchemy.sql import func

from workbot.enums import WorkState

WorkBotDBBase = declarative_base()


class PathString(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.types.String

    @property
    def python_type(self):
        return Path

    def process_literal_param(self, value, dialect):
        return os.fspath(value)

    def process_bind_param(self, value, dialect):
        return os.fspath(value)

    def process_result_value(self, value, dialect):
        return PurePath(value)

    def coerce_compared_value(self, op, value):
        if isinstance(value, PurePath):
            return os.fspath(value)
        else:
            return self


class State(WorkBotDBBase):
    __tablename__ = 'state'

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(Enum(WorkState,
                       create_constraint=True,
                       validate_strings=True), unique=True, nullable=False)
    desc = Column(String(1024), nullable=False)

    def __init__(self, ws: WorkState):
        self.name = ws
        self.desc = ws.value

    def __repr__(self):
        return self.name.name


class StateTransitionError(Exception):
    def __init__(self,
                 current: WorkState,
                 new: WorkState):
        """Exception raised for errors moving a WorkInstance from one State to
        another.

        Args:
            current: A State the WorkInstance is in.
            new: A State the WorkInstance is moving to.
        """
        self.current = current
        self.new = new
        self.message = "An error occurred changing state: " \
                       "invalid transition " \
                       "from {} to {}".format(self.current.name, self.new.name)

    def __repr__(self):
        return "<StateTransitionError: {}>".format(self.message)


class WorkInstance(WorkBotDBBase):
    __tablename__ = 'workinstance'

    id = Column(Integer, autoincrement=True, primary_key=True)
    input_path = Column(PathString(2048), nullable=False)
    # output_path = Column(String(2048), nullable=True)

    work_type = Column(String(128), nullable=False, index=True)

    state_id = Column(Integer, ForeignKey('state.id'), nullable=False)
    state = relationship("State")

    created = Column(DateTime(timezone=True), nullable=False,
                     default=func.now())
    last_updated = Column(DateTime(timezone=True), nullable=False,
                          default=func.now())

    def __init__(self,
                 input_path: Union[Path, str],
                 work_type: str,
                 state: State):
        """Create a new WorkInstance describing an analysis to do.

        Args:
            input_path: The iRODS collection where the initial data are
                        located.
            work_type: A WorkType to perform.
            state: An initial State.
        """
        self.input_path = Path(os.fspath(input_path))
        self.work_type = work_type
        self.state = state

    def __repr__(self):
        return "<WorkInstance: id={}, " \
               "input={}, type={}, " \
               "state={} created={} " \
               "updated={}>".format(self.id,
                                    self.input_path, self.work_type,
                                    self.state, self.created,
                                    self.last_updated)

    """Changes the current state to Staged.

    Changes the state, if the current state is Pending.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def staged(self, session: Session):
        if self.state.name != WorkState.PENDING:
            raise StateTransitionError(self.state.name, WorkState.STAGED)

        self._update_state(session, WorkState.STAGED)

    """Changes the current state to Started.

    Changes state to Started, if the current state is Staged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def started(self, session: Session):
        if self.state.name != WorkState.STAGED:
            raise StateTransitionError(self.state.name, WorkState.STAGED)

        self._update_state(session, WorkState.STARTED)

    """Changes the current state to Succeeded.

    Changes state to Succeeded, if the current state is Started.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def succeeded(self, session: Session):
        if self.state.name != WorkState.STARTED:
            raise StateTransitionError(self.state.name, WorkState.STARTED)

        self._update_state(session, WorkState.SUCCEEDED)

    """Changes the current state to Archived.

    Changes state to Archived, if the current state is Succeeded.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def archived(self, session: Session):
        if self.state.name != WorkState.SUCCEEDED:
            raise StateTransitionError(self.state.name, WorkState.ARCHIVED)

        self._update_state(session, WorkState.ARCHIVED)

    """Changes the current state to Annotated.

    Changes state to Annotated, if the current state is Archived.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def annotated(self, session: Session):
        if self.state.name != WorkState.ARCHIVED:
            raise StateTransitionError(self.state.name, WorkState.ANNOTATED)

        self._update_state(session, WorkState.ANNOTATED)

    """Changes the current state to Unstaged.

    Changes state to Unstaged, if the current state is Staged or Annotated.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def unstaged(self, session):
        if self.state.name not in [WorkState.STAGED, WorkState.ANNOTATED]:
            raise StateTransitionError(self.state.name, WorkState.UNSTAGED)

        self._update_state(session, WorkState.UNSTAGED)

    """Changes the current state to Completed.

    Changes state to Completed, if the current state is Unstaged.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def completed(self, session: Session):
        if self.state.name != WorkState.UNSTAGED:
            raise StateTransitionError(self.state.name, WorkState.COMPLETED)

        self._update_state(session, WorkState.COMPLETED)

    """Changes the current state to Failed.

    Changes state to Failed, if the current state is Started. Failed is an
    end state and data will remain staged for inspection until cleaned up.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def failed(self, session: Session):
        if self.state.name != WorkState.STARTED:
            raise StateTransitionError(self.state.name, WorkState.FAILED)

        self._update_state(session, WorkState.FAILED)

    """Changes the current state to Cancelled.

    Changes state to Cancelled. This can be done from any state.

    Raises:
        StateTransitionError: An error occurred changing state.
    """

    def cancelled(self, session: Session):
        self._update_state(session, WorkState.CANCELLED)

    def is_pending(self):
        return self.state.name == WorkState.PENDING

    def is_staged(self):
        return self.state.name == WorkState.STAGED

    def is_started(self):
        return self.state.name == WorkState.STARTED

    def is_succeeded(self):
        return self.state.name == WorkState.SUCCEEDED

    def is_archived(self):
        return self.state.name == WorkState.ARCHIVED

    def is_annotated(self):
        return self.state.name == WorkState.ANNOTATED

    def is_unstaged(self):
        return self.state.name == WorkState.UNSTAGED

    def is_completed(self):
        return self.state.name == WorkState.COMPLETED

    def is_failed(self):
        return self.state.name == WorkState.FAILED

    def is_cancelled(self):
        return self.state.name == WorkState.CANCELLED

    def _update_state(self, session: Session, name: WorkState):
        s = session.query(State).filter(State.name == name).one()
        self.state = s
        self.last_updated = func.now()
        session.flush()


class ONTMeta(WorkBotDBBase):
    """Oxford Nanopore-specific metadata."""
    __tablename__ = 'ontmeta'

    def __init__(self,
                 wi: WorkInstance,
                 experiment_name: str,
                 instrument_slot: int):
        """Create new ONTMeta metadata.

        Args:
            wi: A WorkInstance to which the metadata applies.
            experiment_name: ONT experiment name.
            instrument_slot: ONT instrument slot.
        """
        self.workinstance = wi
        self.experiment_name = experiment_name
        self.instrument_slot = instrument_slot

    id = Column(Integer, autoincrement=True, primary_key=True)
    workinstance_id = Column(Integer, ForeignKey('workinstance.id'),
                             nullable=False)
    workinstance = relationship("WorkInstance")
    experiment_name = Column(String(255), nullable=False)
    instrument_slot = Column(Integer, nullable=False)

    def __repr__(self):
        return "<ONTMeta: experiment: {}, " \
               "position: {}>".format(self.experiment_name,
                                      self.instrument_slot)


def find_state(session: Session, ws: WorkState) -> State:
    """Returns a State from the database corresponding to a member of the
    WorkState enum."""
    try:
        return session.query(State).filter(State.name == ws).one()
    except SQLAlchemyError as e:
        log.error("Failed to look up a member of the State dictionary. Has "
                  "the database been initialised with its dictionaries?")
        raise e


def find_work_in_progress(session: Session) -> List[WorkInstance]:
    """Returns a list of WorkInstances that have not finished i.e. reached
    a state of either COMPLETED or CANCELLED.

    Args:
        session: An open session.

    Returns: List[WorkInstance]

    """
    return session.query(WorkInstance). \
        join(State). \
        filter(State.name.notin_([WorkState.CANCELLED,
                                  WorkState.COMPLETED])).all()


def initialize_database(session: Session):
    """Initializes the database dictionary tables

    Inserts values into the dictionary tables for work types and work
    states."""

    _initialize_states(session)
    session.commit()


def _initialize_states(session):
    session.add_all([State(s) for s in WorkState])
