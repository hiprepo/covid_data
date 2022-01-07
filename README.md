# Introduction
Explore a dataset of Covid tracking data from 2020-01-13 to 2021-03-07.  The data
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
    
# Testing
There are 2 tests in the tests directory, test_extract.py and test_query.py.  These tests use
a subset of the sample COVID CSV file ranging from 2021-01-01 to 2021-03-07.
    
To run these tests from the project root, run::

    $ python3 -m unittest --verbose
    
To run individuate test from the project root, for example, test_query.py, run:
    
    $ python3 -m unittest --verbose tests.test_query
