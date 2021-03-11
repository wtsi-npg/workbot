# -*- coding: utf-8 -*-
#
# Copyright © 2021 Genome Research Ltd. All rights reserved.
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

from datetime import datetime
from itertools import starmap

from workbot.irods import AVU
from workbot.metadata import DublinCore, SampleMetadata, \
    StudyMetadata
from workbot.ml_warehouse_schema import Sample, Study


def make_creation_metadata(creator: str, created: datetime):
    return [AVU(DublinCore.CREATOR.value, creator,
                namespace=DublinCore.namespace),
            AVU(DublinCore.CREATED.value,
                created.isoformat(timespec="seconds"),
                namespace=DublinCore.namespace)]


def make_modification_metadata(modified: datetime):
    return [AVU(DublinCore.MODIFIED.value,
                modified.isoformat(timespec="seconds"),
                namespace=DublinCore.namespace)]


def avu_if_value(attribute, value):
    if value is not None:
        return AVU(attribute, value)


def make_sample_metadata(sample: Sample):
    av = [[SampleMetadata.SAMPLE_ID.value,
           sample.sanger_sample_id],
          [SampleMetadata.SAMPLE_NAME.value,
           sample.name],
          [SampleMetadata.SAMPLE_ACCESSION_NUMBER.value,
           sample.accession_number],
          [SampleMetadata.SAMPLE_DONOR_ID.value,
           sample.donor_id],
          [SampleMetadata.SAMPLE_SUPPLIER_NAME.value,
           sample.supplier_name],
          [SampleMetadata.SAMPLE_CONSENT_WITHDRAWN.value,
           1 if sample.consent_withdrawn else None]]

    return filter(lambda avu: avu is not None, starmap(avu_if_value, av))


def make_study_metadata(study: Study):
    av = [[StudyMetadata.STUDY_ID.value,
           study.id_study_lims],
          [StudyMetadata.STUDY_NAME.value,
           study.name],
          [StudyMetadata.STUDY_ACCESSION_NUMBER.value,
           study.accession_number]]

    return filter(lambda avu: avu is not None, starmap(avu_if_value, av))

# FIXME
# def make_ont_metadata(flowcell: OseqFlowcell):
#     return [AVU(ONTMetadata.EXPERIMENT_NAME.value,
#                 flowcell.experiment_name),
#             AVU()]
