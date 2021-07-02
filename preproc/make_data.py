#!/usr/bin/env python3

import argparse
import sys

import aws
import cleaners
import decorators


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('inpath', help='path to input file')
    parser.add_argument('outpath', help='path to output file')
    return parser.parse_args()


def read(inpath, **kwargs):
    """Read unprocessed input data.

    Set column types if necessary to save memory.
    """
    dtypes = {
        'Transaction Reference': 'int32',
        'User Reference': 'int32',
        'Year of Birth': 'float32',
        'Account Reference': 'int32',
        'Latest Balance': 'float32',
        'Amount': 'float32',
    }

    dates = [
        'User Registration Date',
        'Transaction Date',
        'Account Created Date',
        'Account Last Refreshed',
    ]

    return aws.s3read_csv(inpath,
                          sep='|',
                          dtype=dtypes,
                          parse_dates=dates,
                          infer_datetime_format=True)


def pipeline(inpath):
    functions = decorators.cleaner_funcs
    df = read(inpath)
    for f in functions:
        df = f(df)

    return df


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_args(args)
    df = pipeline(args.inpath) 
    aws.s3write_parquet(df, args.outpath)
    print(df.head())


if __name__ == "__main__":
    main()

