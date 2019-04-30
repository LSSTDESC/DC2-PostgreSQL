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

    parser.add_argument('forceddir', 
                        help="Directory from which to read data")
    parser.add_argument('schemaname', 
                        help="DB schema name in which to load data")
    # Table name(s) specified in assumptions file.  Do we need an override?
    #  Maybe something to specify name for dpdd view? Or maybe that belongs
    # in native_to_dpdd.yaml and there should be an argument for that
    # filepath
    #parser.add_argument("--table-name", default="forced_source", 
    #                    help="Top-level table's name")
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

    assumptions = Assumptions(args.assumptions)
    if (args.create_keys):
        create_keys(args.schema, assumptions, args.dryrun)
        exit(0)
    
    finder = ForcedSourceFinder(args.forceddir)

    something = create_table(args.schemaname, finder, assumptions, 
                             args.dryrun)

def create_keys(schema, assumptions, dryrun=True):
    """
    Suppose there is an assumptions-file-parsing class
    with methods to return table name(s) and generate
    sql for creating indexes, foreign keys
    assumptions = Assumptions(assumptions_file)
    """
    # if (not dryrun):
    #     #  Get table name list from Assumptions
    #     for t in tables:
    #     if not lib.common.db_table_exists(schema, t):
    #         return False

    # Generate list of SQL commands for creating constraints
    # This should be a service of Assumptions.  Pass in schema name
    # Return a list of strings, each corresponding to one CREATE
    # command.  Or maybe lump together all those belonging to the
    # same table.  Decide what should be done in a single commit.
    if (dryrun):
        # print the list
        return True
    else:
        # Try to execute.
        # return True iff successful
        pass

    return True

def drop_keys(schema, assumptions):
    # Assumptions routine, given schema name, should generate the SQL
    pass

def create_table(schema, finder, assumptions, dryrun=True):
    """
    @param  schema       (Postgres) schema name
    @param  finder       Instance of class which knows how to find schema 
                         for input data and the data itself
    @param  assumptions  Instance of class describing columns to be 
                         included and excluded, among other things
    @param  dryrun       If true only print out sql.  If false, execute 
    If dryrun is False and the table already exists, do nothing.
    Otherwise
    From information in the assumptions file plus information about
    the schema for the input data (obtainable from a file or files
    found for us by the finder) determine the table definition.
    If dryrun just print it out.  Otherwise create the table.
    """

    # If not dryrun, check to see if table(s) already exists

    # Find a data file path using the finder
    afile, determiners = finder.get_some_file()

    hdus = lib.fits.fits_open(afile)  

    #Read fields into a SourceTable via static method SourceTable.from_hdu
    # Note this should be generalized in case there are several tables.
    # That wouldn't be hard, but still wouldn't be adequate for object
    # catalog, where input for each chunk comes from two different files
    # Simplify a bit by insisting each table stores data from only
    # one of the different files.  This is the case now for object catalog.
    raw_table = lib.sourcetable.SourceTable.from_hdu(hdus[1])

    #  Assumptions class applies its 'ignores' to cut it down to what we need
    #  Maybe also subdivide into multiple tables if so described in yaml
    #  Also add definitions for columns not obtained from raw read-in
    remaining_tables = assumptions.apply(raw_table, input_params=determiners)

    # Generate CREATE TABLE string for each table in remaining_tables from 
    #the fields in the table (DbImage object)
    for name in remaining_tables:
        remaining_tables[name].transform()
        remaining_tables[name].create(None, schema)

    # Depending on value of dryrun, actually create or just print it out

def insert_visit(schema, finder, assumptions, visit, dryrun=True):
    """
    @param  schema       (Postgres) schema name
    @param  finder       Instance of class which knows how to find schema 
                         for input data and the data itself
    @param  assumptions  Instance of class describing columns to be 
                         included and excluded, among other things
    @param  visit        integer visit number
    @param  dryrun       If true only print out sql.  If false, insert
                         data for the visit 
    """
    
    # Find all data files belonging to the visit.   Many may be of
    # the minimum size which indicates they have no data.  Make a
    # list of the rest.   [This could all be done by the finder]

    # Using first non-trivial data file, go through essentially same 
    # procedure as for create_table to determine what columns we're 
    # looking for, but this time read data as well as header
    # Encapsulate computation of ccdVisitId in Assumptions.
    # It appears that ccdvisitid is formed from
    # decimal digits as follows
    #             RRSSxxxxxxxx 
    #  where RR is raft number, SS is sensor designation, and x's are
    #  zero-filled representation of visit id.
    #
    #  Is it sufficient to chunk by visit?  Or should it be by raft, or
    #  even by ccd?
    #  Modify old bookkeeping scheme (where granularity was by patch)
    #
    # The guts of of the insert for object catalog is in 
    # insert_patch_into_multibandtable.   It
    #    * assembles a list of column names (called `fieldNames`) an 
    #      arrays of column data (called `columns`) and generates format
    #      string (called `format`).   This information all comes from
    #      the dbtable, plus insertion of tab character between fields
    #    * If multicore, use pipe_printf.   Write to a pipe, using zip
    #      to output column-oriented inputs as rows:
    #           for tpl in zip(*columns):
    #               fout.write(format % tpl)
    #       and meanwhile start copying from the pipe to db use copy_from
    #    * otherwise write the whole thing to an in-memory byte stream,
    #      then use copy_from on that.

if __name__ == "__main__":
    main()
