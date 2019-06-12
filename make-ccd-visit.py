# Copyright (C) 2019  LSST Dark Energy Science Collaboration (DESC)
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

import os
import sys
import itertools
import psycopg2

from lib.visit_utils import ingest_registry, create_table, ingest_calexp_info 

default_db_server = {
    'dbname': os.environ.get("USER", "postgres"),
}


def main():
    import argparse
    cmdline = ' '.join(sys.argv)

    print('Invocation: ' + cmdline)
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Create CcdVisit table from sqlite registry.')

    parser.add_argument('regDir', 
                        help="Directory containing registry.sqlite3")
    parser.add_argument('schemaName',
                        help="DB schema name in which to load data")
    parser.add_argument('--dry-run', dest='dryrun', action='store_true',
                        help="Do not write to db. ", 
                        default=False)
    parser.add_argument('--calexp-only', dest='calexp_only',action='store_true',
                        help="Assumes table with registry info exists already",
                        default=False)
    parser.add_argument('--repodir', dest='repodir', 
                        help="where to find calexp data", default=None)
    parser.add_argument('--sql-dir', dest='sqldir', default='.')
    parser.add_argument("--db-server", metavar="key=value", nargs="+", 
                        action="append", 
                        help="DB connect parms. Must come after reqd args.")

    args = parser.parse_args()

    db_server = dict(default_db_server)
    db_server.update(key_value.split('=', 1) for key_value in itertools.chain.from_iterable(args.db_server))
    args.db_server = db_server
    
    connection = psycopg2.connect(**db_server)

    if  args.calexp_only==False:
        create_table(connection, 'CcdVisit', args.schemaName, args.sqldir, 
                     args.dryrun)

        ingest_registry(connection, 
                        os.path.join(args.regDir, 'registry.sqlite3'),
                        args.schemaName, args.dryrun)
    if args.repodir is not None:
        ingest_calexp_info(connection, args.repodir, args.schemaName, 
                           args.dryrun)

if __name__ == "__main__":
    main()
