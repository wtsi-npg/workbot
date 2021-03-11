from enum import Enum, unique


@unique
class WorkType(Enum):
    EMPTY = "A null pipeline, does nothing"
    ARTICNextflow = "ARTIC Nextflow pipeline for ONT run data",
    ONTRunMetadataUpdate = "ONT Run Metadata update"


@unique
class WorkState(Enum):
    PENDING = "Pending any action"
    STAGED = "Work data have been staged"

    STARTED = "Work has started"
    SUCCEEDED = "Work was done successfully"
    ARCHIVED = "Work data have been archived"
    ANNOTATED = "Work data have been annotated"
    UNSTAGED = "Work data have been unstaged"
    COMPLETED = "All actions are complete"

    FAILED = "Work has failed"
    CANCELLED = "Work has been cancelled"
