# coding: utf-8
from datetime import datetime
from typing import List, Tuple

from sqlalchemy import Column, DateTime, ForeignKey, Index, String, Text, \
    Boolean, Integer, func, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session

MLWHBase = declarative_base()


class Sample(MLWHBase):
    __tablename__ = 'sample'

    id_sample_tmp = Column(Integer, primary_key=True)
    id_lims = Column(String, nullable=False)
    uuid_sample_lims = Column(String, unique=True)
    id_sample_lims = Column(String, nullable=False)
    last_updated = Column(DateTime)
    recorded_at = Column(DateTime)
    deleted_at = Column(DateTime)
    created = Column(DateTime)
    name = Column(String)
    reference_genome = Column(String)
    organism = Column(String)
    accession_number = Column(String)
    common_name = Column(String)
    description = Column(Text)
    taxon_id = Column(Integer)
    father = Column(String)
    mother = Column(String)
    replicate = Column(String)
    ethnicity = Column(String)
    gender = Column(String)
    cohort = Column(String)
    country_of_origin = Column(String)
    geographical_region = Column(String)
    sanger_sample_id = Column(String)
    control = Column(Boolean)
    supplier_name = Column(String)
    public_name = Column(String)
    sample_visibility = Column(String)
    strain = Column(String)
    consent_withdrawn = Column(Boolean, nullable=False, default=False)
    donor_id = Column(String)
    phenotype = Column(String)
    developmental_stage = Column(String)
    control_type = Column(String)

    def __repr__(self):
        return "<Sample: name={}, id_sample_lims={} last_updated={}>".format(
            self.name, self.id_sample_lims, self.last_updated)


class Study(MLWHBase):
    __tablename__ = 'study'

    id_study_tmp = Column(Integer, primary_key=True)
    id_lims = Column(String, nullable=False)
    uuid_study_lims = Column(String, unique=True)
    id_study_lims = Column(String, nullable=False)
    last_updated = Column(DateTime, nullable=False, default=func.now())
    recorded_at = Column(DateTime, nullable=False, default=func.now())
    deleted_at = Column(DateTime)
    created = Column(DateTime)
    name = Column(String)
    reference_genome = Column(String)
    ethically_approved = Column(Boolean)
    faculty_sponsor = Column(String)
    state = Column(String)
    study_type = Column(String)
    abstract = Column(Text)
    abbreviation = Column(String)
    accession_number = Column(String)
    description = Column(Text)
    contains_human_dna = Column(Boolean)
    contaminated_human_dna = Column(Boolean)
    data_release_strategy = Column(String)
    data_release_sort_of_study = Column(String)
    ena_project_id = Column(String)
    study_title = Column(String)
    study_visibility = Column(String)
    ega_dac_accession_number = Column(String)
    array_express_accession_number = Column(String)
    ega_policy_accession_number = Column(String)
    data_release_timing = Column(String)
    data_release_delay_period = Column(String)
    data_release_delay_reason = Column(String)
    remove_x_and_autosomes = Column(Boolean, nullable=False, default=False)
    aligned = Column(Boolean, nullable=False, default=True)
    separate_y_chromosome_data = Column(Boolean, nullable=False, default=False)
    data_access_group = Column(String)
    prelim_id = Column(String)
    hmdmc_number = Column(String)
    data_destination = Column(String)
    s3_email_list = Column(String)
    data_deletion_period = Column(String)

    def __repr__(self):
        return "<Study: name={}, id_study_lims={} last_updated={}>".format(
            self.name, self.id_study_lims, self.last_updated)


class OseqFlowcell(MLWHBase):
    __tablename__ = 'oseq_flowcell'

    id_oseq_flowcell_tmp = Column(Integer, primary_key=True)
    id_flowcell_lims = Column(String, nullable=False)
    last_updated = Column(DateTime, nullable=False, default=func.now())
    recorded_at = Column(DateTime, nullable=False, default=func.now())
    id_sample_tmp = Column(ForeignKey('sample.id_sample_tmp'), nullable=False)
    id_study_tmp = Column(ForeignKey('study.id_study_tmp'), nullable=False)
    experiment_name = Column(String, nullable=False)
    instrument_name = Column(String, nullable=False)
    instrument_slot = Column(Integer, nullable=False)
    tag_set_id_lims = Column(String, nullable=True)
    tag_set_name = Column(String, nullable=True)
    tag_identifier = Column(Integer, nullable=True)
    tag_sequence = Column(String, nullable=True)
    tag2_set_id_lims = Column(String, nullable=True)
    tag2_set_name = Column(String, nullable=True)
    tag2_identifier = Column(Integer, nullable=True)
    tag2_sequence = Column(String, nullable=True)
    pipeline_id_lims = Column(String, nullable=False)
    requested_data_type = Column(String, nullable=False)
    deleted_at = Column(DateTime)
    id_lims = Column(String)

    sample = relationship('Sample')
    study = relationship('Study')

    def __repr__(self):
        return "<OseqFlowcell: inst_name={}, inst_slot={} " \
               "expt_name={} tag_set_name={} tag_id={} " \
               "last_updated={}>".format(self.instrument_name,
                                         self.instrument_slot,
                                         self.experiment_name,
                                         self.tag_set_name,
                                         self.tag_identifier,
                                         self.last_updated)


def find_recent_experiments(session: Session,
                            since: datetime) -> List[str]:
    """Finds recent experiments in the ML warehouse database.

    Finds ONT experiments in the ML warehouse database that have been updated
    since a specified date and time. If any element of the experiment (any of
    the positions in a multi-flowcell experiment, any of the multiplexed
    elements within a position) have been updated in the query window, the
    experiment name will be returned.

    Args:
        session: An open SQL session.
        since: A datetime.

    Returns:
        List of matching experiment name strings
    """

    result = session.query(distinct(OseqFlowcell.experiment_name)).\
        filter(OseqFlowcell.last_updated >= since).all()

    # The default behaviour of SQLAlchemy is that the result here is a list
    # of tuples, each of which must be unpacked. The official way to do
    # that for all cases is to extend sqlalchemy.orm.query.Query to do the
    # unpacking. However, that's too fancy for MVP, so we just unpack
    # manually.
    return [value for value, in result]


def find_recent_experiment_pos(session: Session,
                               since: datetime) -> List[Tuple]:
    """Finds recent experiments and instrument positions in the ML warehouse
     database.

    Finds ONT experiments and associated instrument positions in the ML
    warehouse database that have been updated since a specified date and time.

    Args:
        session: An open SQL session.
        since: A datetime.

    Returns:
        List of matching (experiment name, position) tuples
    """

    return session.query(OseqFlowcell.experiment_name,
                         OseqFlowcell.instrument_slot).\
        filter(OseqFlowcell.last_updated >= since).\
        group_by(OseqFlowcell.experiment_name,
                 OseqFlowcell.instrument_slot).\
        order_by(OseqFlowcell.experiment_name.asc(),
                 OseqFlowcell.instrument_slot.asc()).all()
