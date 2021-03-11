
# WorkBot 

## Overview

`WorkBot` is a basic extract, transform, load
([ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load)) application.

It provides a means get some data from an archive
([iRODS](https://www.irods.org/) is the only supported archive), stage it to a
temporary filesystem, run an analysis on the staged data and then return the
results to the archive, annotated with metadata about the process.

## Status

[![Build Status](https://travis-ci.org/kjsanger/workbot.svg?branch=devel)](https://travis-ci.org/kjsanger/workbot)

## Design

Datasets are identified by their unique absolute directory path in the archive.
Anything under that directory is considered to be part of the same dataset.
Work is identified by a tuple dataset path and work type controlled vocabulary
term.

e.g. these represent the same work: 

`"/path/to/my/dataset/1", "ARTICNextFlow"`
`"/path/to/my/dataset/1", "ARTICNextflow"`

while these are distinct:

`"/path/to/my/dataset/1", "ARTICNextFlow"`
`"/path/to/my/dataset/2", "ARTICNextFlow"`
`"/path/to/my/dataset/1", "ONTMetadataUpdate"`

Only one instance of any work can be queued at a time. Queuing new work is
idempotent, so attempts to add multiple instances of the same work will be 
successful no-ops. If work fails, a new instance of it may be queued. Once work
has completed successfully, a new instance of it may be queued for work types
where multiple runs are permitted.

The types of work supported are intended to be coarse-grained e.g. a complete
pipelines wrapped in a shell script. There is no dependency between work
instances. Where multiple types of work are being carried out on one dataset,
they must be fully independent of each other.
