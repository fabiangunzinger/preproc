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

def pipeline(df):
    functions = decorators.cleaner_funcs
    return [f(df) for f in functions]

def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_args(args)

    #todo
    # need to use mdb reader function to preproc var names

    df = aws.s3read_csv(args.inpath, sep='|')
    df = pipeline(df) 
    aws.s3write_parquet(df, args.outpath)
    print(df.head())


if __name__ == "__main__":
    main()

