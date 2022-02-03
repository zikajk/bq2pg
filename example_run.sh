#!/bin/bash

CONFIG=$1

BQEXPORT=true PGIMPORT=true PGHOST=localhost PGDATABASE=postgres PGPORT=5432 PGUSER=postgres PGPASSWORD=my_password GCSNAME=some-bucket GCSFOLDER=bq2pg lein run $CONFIG
