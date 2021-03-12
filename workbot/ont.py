import os
import re
from abc import ABCMeta
from pathlib import PurePath
from typing import List, Tuple

from sqlalchemy.orm import Session

from workbot.base import AnnotationMixin, RodsHandler, WorkBot, WorkBroker, \
    analyse_op, annotate_op, archive_op, complete_op, log, register, \
    stage_op, unstage_op
from workbot.enums import WorkState
from workbot.irods import AVU, BatonError, Collection
from workbot.metadata import ONTMetadata
from workbot.ml_warehouse_metadata import make_sample_metadata, \
    make_study_metadata
from workbot.ml_warehouse_schema import find_ont_plex_info, find_recent_ont_pos
from workbot.schema import ONTMeta, WorkInstance


class ONTMetadataMixin(object, metaclass=ABCMeta):
    """ONTMetadataMixin provides methods for adding ONT metadata to the Workbot
    database."""

    @staticmethod
    def add_metadata(session: Session, wi: WorkInstance, **kwargs):
        """Adds metadata for this work instance to the WorkBot database.

        For ONT the necessary keyword arguments are:
            experiment_name: An ONT experiment name (string)
            instrument_position: An ONT instrument position (integer)

        Args:
            session: An open Session.
            wi: A WorkInstance to annotate.
            kwargs: Metadata keys and values.
        """
        experiment_name = kwargs["experiment_name"]
        instrument_position = kwargs["instrument_position"]

        log.debug("Adding metadata experiment: {}, position: {}".
                  format(experiment_name, instrument_position))
        session.add(ONTMeta(wi, experiment_name, instrument_position))
        session.commit()


class ONTWorkBot(WorkBot, AnnotationMixin):
    """ONTWorkBot is specialised for processing data from an ONT instrument
    (GridION, PromethION). It provides extra support for linking ONT-specific
    annotation to work being done."""

    @staticmethod
    def add_metadata(session: Session, wi: WorkInstance, **kwargs):
        """Adds metadata for this work instance to the WorkBot database.

        For ONT the necessary keyword arguments are:
            experiment_name: An ONT experiment name (string)
            instrument_position: An ONT instrument position (integer)

        Args:
            session: An open Session.
            wi: A WorkInstance to annotate.
            kwargs: Metadata keys and values.
        """
        experiment_name = kwargs["experiment_name"]
        instrument_position = kwargs["instrument_position"]

        log.debug("Adding metadata experiment: {}, position: {}".
                  format(experiment_name, instrument_position))
        session.add(ONTMeta(wi, experiment_name, instrument_position))
        session.commit()


@register
class ONTRunDataWorkBot(ONTWorkBot):
    """ONTRunDataWorkBot is specialised for processing archived run data
    that has come from an ONT instrument (GridION, PromethION). It provides
    extra support for adding ONT-specific annotation to the output.
    """

    def __init__(self, work_type, archive_root=None, staging_root=None):
        super().__init__(work_type=work_type,
                         archive_root=archive_root,
                         staging_root=staging_root,
                         rods_handler=ONTRodsHandler())

    @annotate_op
    def annotate_output_data(self, session: Session, wi: WorkInstance):
        """Annotates the archived analysis results if archiving is complete,
        possibly using platform-specific metadata.

        Args:
            session: An open Session.
            wi: A WorkInstance.
        """
        log.info("Annotating output data for {}".format(wi))

        # FIXME: Under what circumstances does it make sense to be annotating
        #  multiple experiment/slot tuples on a single collection? Don't we
        #  expect a single tuple? Multiple implies that we have merged data
        #  from multiple flowcells.
        meta = session.query(ONTMeta).filter(ONTMeta.workinstance == wi).all()
        log.debug("Got metadata for {}: {}".format(wi, meta))

        dst = self.archive_path(wi)
        coll = self.rods_handler.collection(dst)

        try:
            for m in meta:
                avus = [AVU(ONTMetadata.EXPERIMENT_NAME.value,
                            m.experiment_name),
                        AVU(ONTMetadata.INSTRUMENT_SLOT.value,
                            m.instrument_slot)]
                avus = [avu.with_namespace(ONTMetadata.namespace) for avu in
                        avus]
                coll.meta_add(*avus)
        except BatonError as e:
            log.error("Failed to annotate output data "
                      "for {}: {}".format(wi, e))
            raise

        return


@register
class ONTRunMetadataWorkBot(ONTWorkBot):
    """ONTRunMetadataWorkBot is specialised for setting and/or updating
    archive (iRODS) metadata of ONT sequencing runs. These are the raw run
    data, not the result of processing with an ONTRunDataWorkBot.
    """

    def __init__(self, work_type: str):
        super().__init__(work_type, end_states=[WorkState.CANCELLED])

    @stage_op
    def stage_input_data(self, session: Session, wi: WorkInstance, **kwargs):
        pass

    @analyse_op
    def run_analysis(self, session: Session, wi: WorkInstance, **kwargs):
        pass

    @archive_op
    def archive_output_data(self, session: Session, wi: WorkInstance,
                            **kwargs):
        pass

    @annotate_op
    def annotate_output_data(self, session: Session, wi: WorkInstance,
                             mlwh_session: Session):
        meta = session.query(ONTMeta).filter(ONTMeta.workinstance == wi).one()
        log.debug("Searching the warehouse for plex information on "
                  "experiment {} slot {}".format(meta.experiment_name,
                                                 meta.instrument_slot))

        path = PurePath(wi.input_path)
        plex_info = find_ont_plex_info(mlwh_session,
                                       meta.experiment_name,
                                       meta.instrument_slot)

        avus = [AVU(ONTMetadata.EXPERIMENT_NAME.value,
                    meta.experiment_name),
                AVU(ONTMetadata.INSTRUMENT_SLOT.value,
                    meta.instrument_slot)]
        avus = [avu.with_namespace(ONTMetadata.namespace) for avu in avus]
        self.rods_handler.collection(path).meta_add(*avus)

        for fc in plex_info:
            log.debug("Found experiment {} slot {} "
                      "tag index: {}".format(meta.experiment_name,
                                             meta.instrument_slot,
                                             fc.tag_index))

            if fc.tag_index:
                # This is the barcode directory naming style created by ONT's
                # Guppy and qcat de-plexers
                p = path / "barcode{}".format(str(fc.tag_index).zfill(2))
                log.debug("Annotating iRODS path {} with "
                          "tag index {}".format(p, fc.tag_index))
                log.debug("Annotating iRODS path {} with "
                          "{} and {}".format(p, fc.sample, fc.study))

                coll = self.rods_handler.collection(p)
                coll.meta_add(AVU("tag_index", fc.tag_index))
                coll.meta_add(*make_study_metadata(fc.study))
                coll.meta_add(*make_sample_metadata(fc.sample))

    @unstage_op
    def unstage_input_data(self, session: Session, wi: WorkInstance, **kwargs):
        pass

    @complete_op
    def complete_analysis(self, session: Session, wi: WorkInstance, **kwargs):
        pass


class ONTRodsHandler(RodsHandler):
    def is_input_data_complete(self, wi: WorkInstance):
        """Returns true if the input data is complete and ready to process.

        Args:
            wi: A WorkInstance.

        Returns: bool
        """
        complete = False

        if self.is_input_path_present(wi):
            log.info("Checking for complete input data for {}".format(wi))
            # If a file named .*final_report.txt.gz is present, the run is
            # complete

            try:
                coll = Collection(self.client, wi.input_path)
                contents = coll.list(contents=True)
                matches = list(filter(lambda p:
                                      re.search(r'final_report.txt.gz$',
                                                os.fspath(p)),
                                      contents))
                if list(matches):
                    log.debug("Found final report matches: {}".format(matches))
                    complete = True
            except BatonError as e:
                log.error("Failed to check input data "
                          "for {}: {}".format(wi, e))
                raise

        return complete


class ONTWorkBroker(WorkBroker):
    """A broker capable of finding what work needs to be done for ONT data and
    queueing it in the Workbot database. Typically work needs to be done
    when something has changed in iRODS (new data arriving) or in the
    multi-LIMS warehouse (secondary metadata on existing data in iRODS
    need updating).

    This broker works by using primary metadata (i.e. metadata intrinsic to
    the data and which do not change) to locate data in iRODS.
    """

    def __init__(self, workbot: ONTWorkBot):
        self.worbot = workbot

    def request_work(self, wb_session=None,
                     mlwh_session=None, start_date=None, zone=None) -> int:
        """Adds work for ONT runs in iRODS whose metadata have been updated in
        the multi-LIMS  warehouse since a given start datetime. Returns the
        total number of work instances added.

        Args:
            wb_session: An open WorkBot Session.
            mlwh_session: An open multi-LIMS warehouse Session.
            start_date: A start datetime for the search.
            zone: An iRODS zone.

        Returns: int
        """
        # DEFINE: We don't always need to check the ML warehouse for recent
        #  changes; just the presence of records should be enough if we are
        #  starting an analysis. Perhaps start_date should default to e.g. the
        #  epoch to encompass all LIMS history for these cases?

        expos = find_recent_ont_pos(mlwh_session, start_date)
        return self.__add_work_for_runs(wb_session, expos, zone=zone)

    def __add_work_for_runs(self,
                            session: Session,
                            experiment_slots: List[Tuple],
                            zone=None) -> int:
        """Adds work for ONT runs in iRODS defined by a list of experiment
        name, instrument slot tuples. Returns the total number of
        work instances added.

        iRODS metadata are used to locate the run, so unless the run is both
        in iRODS and annotated, this method will do nothing. If incomplete
        work exists for the data, new work is not added.

        Args:
            session: An open Session.
            experiment_slots: Experiment, slot tuples.
            zone: An iRODS zone.

        Returns: int
        """
        num_added = 0

        try:
            for expt, slot in experiment_slots:
                n = self.__add_work_for_run(session, expt, slot, zone=zone)
                log.info("Checked for work for experiment {}, "
                         "instrument slot {} and "
                         "added {} jobs".format(expt, slot, n))
                num_added += n
        except Exception as e:
            log.error("Failed to add new analysis: {}".format(e))
            raise

        log.info("Added a total of {} jobs".format(num_added))

        return num_added

    def __add_work_for_run(self,
                           session: Session,
                           experiment_name: str,
                           instrument_slot: int,
                           zone=None) -> int:
        """Adds work for a particular ONT run in iRODS defined by its
        experiment name and instrument slot. Returns the number of work
        instances added.

        iRODS metadata are used to locate the run, so unless the run is both in
        iRODS and annotated, this method will do nothing. If incomplete work
        exists for the data, new work is not added.

        Args:
            session: An open Session.
            experiment_name: The experiment name.
            instrument_slot: The instrument slot.

        Returns: int
        """
        avus = [avu.with_namespace(ONTMetadata.namespace) for avu in
                [AVU(ONTMetadata.EXPERIMENT_NAME.value, experiment_name),
                 AVU(ONTMetadata.INSTRUMENT_SLOT.value, instrument_slot)]]
        found = self.worbot.rods_handler.meta_query(avus, collection=True,
                                                    zone=zone)

        if not found:
            log.info("No collection in iRODS for "
                     "expt: {} pos: {}".format(experiment_name,
                                               instrument_slot))
            return 0

        log.info("Found {} collections in iRODS zone {} for expt: {} pos: {} "
                 "{}".format(len(found), zone, experiment_name,
                             instrument_slot, found))

        # DEFINE: This will set up work for all iRODS paths having matching
        #  metadata. Is this the behaviour we want?
        num_added = 0
        for path in found:
            wi = self.worbot.add_work(session, path)
            if wi:
                self.worbot.add_metadata(session, wi,
                                         experiment_name=experiment_name,
                                         instrument_position=instrument_slot)
                session.commit()

                log.info("Added {}".format(wi))
                num_added += 1

        return num_added
