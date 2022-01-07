#!/usr/bin/env python3
"""Explore a dataset of Covid tracking data from 2020-01-13 to 2021-03-07.  The data
   is taken from http://covidtracking.com/data/download

This script can be invoked from the command line::

    $ python3 main.py {inspect,query} [args]

The `inspect` subcommand looks up the COVID tracking data by date or by state.

    $ python3 main.py inspect --date 2021-01-15
    $ python3 main.py inspect --state CA

The `query` subcommand searches for COVID tracking data that match given criteria:

    $ python3 main.py query --date 2020-12-29
    $ python3 main.py query --state CA
    $ python3 main.py query --start-date 2021-01-01 --end-date 2021-01-31 --state CA
    $ python3 main.py query --start-date 2021-01-01 --min-hospitalized 1000 --min-death 50
    $ python3 main.py query --date 2020-03-14 --max-hospitalized 2500 --min-icu 50
    $ python3 main.py query --start-date 2021-01-01 --min-onvent 10 --max-onvent 5000

"""
import argparse
import cmd
import datetime
import pathlib
import sys
import time

from extract import load_cdc_data
from database import CDCTrackingDatabase
from filters import create_filters, limit


# Paths to the root of the project and the `data` subfolder.
PROJECT_ROOT = pathlib.Path(__file__).parent.resolve()
DATA_ROOT = PROJECT_ROOT / 'data'


def date_fromisoformat(date_string):
    """Return a `datetime.date` corresponding to a string in YYYY-MM-DD format.
    :param date_string: A date in the format YYYY-MM-DD.
    :return: A `datetime.date` correspondingo the given date string.
    """
    try:
        return datetime.datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{date_string}' is not a valid date. Use YYYY-MM-DD.")


def make_parser():
    """Create an ArgumentParser for this script.

    :return: A tuple of the top-level, inspect, and query parsers.
    """
    parser = argparse.ArgumentParser(
        description="Explore CDC tracking data of all US States from 2020-01-13 to 2021-03-07."
    )

    # Add arguments for custom data files.
    parser.add_argument('--datafile', default=(DATA_ROOT / 'all-states-history.csv'),
                        type=pathlib.Path,
                        help="Path to CSV file of CDC tracking data of all states.")
    subparsers = parser.add_subparsers(dest='cmd')

    # Add the `inspect` subcommand parser.
    inspect = subparsers.add_parser('inspect',
                                    description="Inspect CDC tracking data by date or by state.")
    inspect_id = inspect.add_mutually_exclusive_group(required=True)
    inspect_id.add_argument('-d', '--date',
                            help="The date of the Covid tracking data to inspect (e.g. '2021-02-01').")
    inspect_id.add_argument('-s', '--state',
                            help="The state abbreviation of the state to inspect (e.g. 'CA').")

    # Add the `query` subcommand parser.
    query = subparsers.add_parser('query',
                                  description="Query for CDC Covid tracking data that "
                                              "match a collection of filters.")
    filters = query.add_argument_group('Filters',
                                       description="Filter tracking data by their attributes.")
    filters.add_argument('-d', '--date', type=date_fromisoformat,
                         help="Only return COVID data on the given date, "
                              "in YYYY-MM-DD format (e.g. 2020-12-31).")
    filters.add_argument('-s', '--start-date', type=date_fromisoformat,
                         help="Only return COVID data on or after the given date, "
                              "in YYYY-MM-DD format (e.g. 2020-12-31).")
    filters.add_argument('-e', '--end-date', type=date_fromisoformat,
                         help="Only return COVID data on or before the given date, "
                              "in YYYY-MM-DD format (e.g. 2020-12-31).")

    filters.add_argument('-st', '--state', type=str,
                         help="Only return COVID data of a give US state, "
                              "in US state abbreviationformat (e.g. 'CA').")

    filters.add_argument('--min-hospitalized', dest='hospitalized_min', type=int,
                         help="Only return COVID data of the number of people currently hospitalized that"
                              "equals or bigger than the given number.")
    filters.add_argument('--max-hospitalized', dest='hospitalized_max', type=int,
                         help="Only return COVID data of the number of people currently hospitalized that"
                              "equals or smaller than the given number.")

    filters.add_argument('--min-icu', dest='icu_min', type=int,
                         help="Only return COVID data of the number of people currently in ICU that"
                              "equals or bigger than the given number.")
    filters.add_argument('--max-icu', dest='icu_max', type=int,
                         help="Only return COVID data of the number of people currently in ICU that"
                              "equals or smaller than the given number.")

    filters.add_argument('--min-onvent', dest='onvent_min', type=int,
                         help="Only return COVID data of the number of people currently on ventilation that"
                              "equals or bigger than the given number.")
    filters.add_argument('--max-onvent', dest='onvent_max', type=int,
                         help="Only return COVID data of the number of people currently on ventilation that"
                              "equals or smaller than the given number.")

    filters.add_argument('--min-death', dest='death_min', type=int,
                         help="Only return COVID data of the number of deaths"
                              "equals or bigger than the given number.")
    filters.add_argument('--max-death', dest='death_max', type=int,
                         help="Only return COVID data of the number of deaths"
                              "equals or smaller than the given number.")

    query.add_argument('-l', '--limit', type=int,
                       help="The maximum number of matches to return. "
                            "Defaults to 50.")
    return parser, inspect, query


def inspect(database, date=None, state=None):
    """Perform the `inspect` subcommand.

    This function fetches a CDCTrackingObject by date or by state. If a matching object is
    found, information about the CDCTrackingObject is printed.

    At least one of `date` and `state` must be given.

    :param database: The `CDCTrackingDatabase` containing data on COVID tracking data from 2020-01-13 to 2021-03-07
    :param date: The date for which to search the data.
    :param state: The state for which to search the data.
    :return: The matching `CDCTrakingObject`, or None if not found.
    """
    # Fetch Covid tracking data of interest.
    if date:
        covidData = database.get_tracking_data_by_date(date)
    else:
        covidData = database.get_tracking_data_by_state(state)

    if not covidData:
        print("No matching CDC tracking data exist in the database.", file=sys.stderr)
        return None

    for _, d in enumerate(covidData):
       print(d)

    return covidData


def query(database, args):
    """Perform the `query` subcommand.

    Create a collection of filters with `create_filters` and supply them to the
    database's `query` method to produce a stream of matching results.

    Print matching results to stdout, limiting to 50 entries if no limit was specified.

    :param database: The `CDCTrackingDatabase` containing data on COVID tracking data.
    :param args: All arguments from the command line, as parsed by the top-level parser.
    """
    # Construct a collection of filters from arguments supplied at the command line.
    filters = create_filters(
        date=args.date, start_date=args.start_date, end_date=args.end_date,
        state=args.state,
        hospitalized_min=args.hospitalized_min, hospitalized_max=args.hospitalized_max,
        icu_min=args.icu_min, icu_max=args.icu_max,
        onvent_min=args.onvent_min, onvent_max=args.onvent_max,
        death_min=args.death_min, death_max=args.death_max
    )
    # Query the database with the collection of filters.
    results = database.query(filters)

    # Write the results to stdout, limiting to 50 entries if not specified.
    for result in limit(results, args.limit or 50):
        print(result)

def main():
    """Run the main script."""
    parser, inspect_parser, query_parser = make_parser()
    args = parser.parse_args()

    # Extract data from the data files into structured Python objects.
    database = CDCTrackingDatabase(load_cdc_data(args.datafile))

    # Run the chosen subcommand.
    if args.cmd == 'inspect':
        inspect(database, date=args.date, state=args.state)
    elif args.cmd == 'query':
        query(database, args)


if __name__ == '__main__':
    main()
