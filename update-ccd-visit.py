import os
import sys
import itertools
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
