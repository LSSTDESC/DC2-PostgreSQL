#!/usr/bin/env python

# Imports are copied from ingest-object-catalog.py.
# Might not need them all
import numpy  
import psycopg2
import sys

import lib.fits
import lib.misc
import lib.dbtable
import lib.sourcetable
import lib.common
import lib.config

from lib.assumptions import Assumptions
from lib.forcedsource_finder import ForcedSourceFinder

if lib.config.MULTICORE:
    from lib import pipe_printf

import glob
import io
import itertools
import os
import re
import textwrap

def main():
    import argparse
    cmdline = ' '.join(sys.argv)

    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Read a rerun directory to create "forced" summary table.')

    parser.add_argument('forcedDir', dest='rootdir',
                        help="Directory from which to read data")
    parser.add_argument('schemaName', 
                        help="DB schema name in which to load data")
    parser.add_argument("--table-name", default="forced_source", 
                        help="Top-level table's name")
    parser.add_argument("--db-server", metavar="key=value", nargs="+", 
                        action="append", 
                        help="DB connect parms. Must come after reqd args.")
    parser.add_argument('--create-keys',  action='store_true',
       help="Create index, foreign keys (only; don't insert data)")

    parser.add_argument('--dry-run', dest='dryrun', action='store_true',
                        help="Do not write to db. Ignored for create-index", 
                        default=False)
    parser.add_argument('--no-insert', dest='no_insert', action='store_true',
                        help="Just create tables and views; no inserts", 
                        default=False)
    parser.add_argument('--visits', dest='visits', type=int, nargs='+', 
                        help="Ingest data for specified tracts only if present. Else ingest all")
    parser.add_argument('--assumptions', default='forced_source_assumptions.yaml', help="Path to description of prior assumptions about data schema")

    args = parser.parse_args()

    if args.visits is not None:
        print("Processing the following visits:")
        for v in args.visits: print(v)

    if args.db_server:
        lib.config.dbServer.update(keyvalue.split('=', 1) for keyvalue in itertools.chain.from_iterable(args.db_server))

    lib.config.tableSpace = ""
    lib.config.indexSpace = ""

    assumptions = Assumptions(args.assumptions, args.schema)
    if (args.create_keys):
        create_keys(assumptions, args.dryrun)
        exit(0)
    
    finder = ForcedSource_finder(args.rootdir)

    something = create_table(finder, args.table_name,
                             args.assumptions, args.dryrun)




def create_keys(schema, assumptions='assumptions.yaml', dryrun=True):
    """
    Suppose there is an assumptions-file-parsing class
    with methods to return table name(s) and generate
    sql for creating indexes, foreign keys
    assumptions = Assumptions(assumptions_file)
    """
    if (not dryrun):
        if not lib.common.db_table_exists(schema, table_name):
            return False
    # Generate list of SQL commands for creating constraints
    if (dryrun):
        # print the list
        return True
    else:
        # Try to execute.
        # return True iff successful
        pass

    return True

def create_table(schema, finder, tbl='forced_source', 
                 assumptions='assumptions.yaml', dryrun=True):
    """
    If dryrun is False and the table already exists, do nothing.
    Otherwise
    From information in the assumptions file plus information about
    the schema for the input data (obtainable from a file or files
    found for us by the finder) determine the table definition.
    If dryrun just print it out.  Otherwise create the table.
    """

    # Find a data file using the finder

    #Read fields into a SourceTable via static method SourceTable.from_hdu


    # Using data from assumptions file, cut out all the 'ignores'


    #; finalize table schema; generate CREATE TABLE string
