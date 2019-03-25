import os
import sys
import itertools
import psycopg2

from lib.visit_utils import ingest_registry, create_table

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

    parser.add_argument('repoDir', 
                        help="Directory containing registry.sqlite3")
    parser.add_argument('schemaName',
                        help="DB schema name in which to load data")
    parser.add_argument('--dry-run', dest='dryrun', action='store_true',
                        help="Do not write to db. ", 
                        default=False)
    parser.add_argument('--sql-dir', dest='sqldir', default='.')
    parser.add_argument("--db-server", metavar="key=value", nargs="+", 
                        action="append", 
                        help="DB connect parms. Must come after reqd args.")

    args = parser.parse_args()

    db_server = dict(default_db_server)
    db_server.update(key_value.split('=', 1) for key_value in itertools.chain.from_iterable(args.db_server))
    args.db_server = db_server
    
    connection = psycopg2.connect(**db_server)

    create_table(connection, 'CcdVisit', args.schemaName, args.sqldir, args.dryrun)
    ingest_registry(connection, os.path.join(args.repoDir, 'registry.sqlite3'),
                    args.schemaName, args.dryrun)

if __name__ == "__main__":
    main()
