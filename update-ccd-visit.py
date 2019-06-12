import os
import sys
import itertools
# Copyright (C) 2019  LSST Dark Energy Science Collaboration (DESC)
#
# This file is part of the project DC2-PostgreSQL
# DC2-PostgreSQL is free software: you can redistribute it and/or modify
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

import psycopg2

from lib.visit_utils import ingest_calexp_info 

default_db_server = {
    'dbname': os.environ.get("USER", "postgres"),
}


def main():
    import argparse
    cmdline = ' '.join(sys.argv)

    print('Invocation: ' + cmdline)
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Update CcdVisit table with calexp information')

    parser.add_argument('schemaName',
                        help="DB schema name in which to load data")
    parser.add_argument('--dry-run', dest='dryrun', action='store_true',
                        help="Do not write to db. ", 
                        default=False)
    parser.add_argument('--repodir', dest='repodir', 
                        help="where to find calexp data", default=None)
    parser.add_argument("--db-server", metavar="key=value", nargs="+", 
                        action="append", 
                        help="DB connect parms. Must come after reqd args.")
    #parser.add_argument('--visits', dest='visits', type=int, nargs='+', 
    #                    help="visits for which calexp info will be inserted")
    parser.add_argument('--min-visit', dest='min_visit', type=int,
                        help='insert data only for this visit id and beyond',
                        default=0)
    parser.add_argument('--max-visit', dest='max_visit', type=int,
                        help='insert data only for visits <= this value',
                        default=100000000)

    args = parser.parse_args()

    db_server = dict(default_db_server)
    db_server.update(key_value.split('=', 1) for key_value in itertools.chain.from_iterable(args.db_server))
    args.db_server = db_server
    
    connection = psycopg2.connect(**db_server)

    if args.repodir is not None:
        ingest_calexp_info(connection, args.repodir, args.schemaName, 
                           args.dryrun, args.min_visit, args.max_visit)

if __name__ == "__main__":
    main()
