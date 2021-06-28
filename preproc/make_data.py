#!/usr/bin/env python3

import argparse

import aws

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('inpath', help='path to input file')
    parser.add_argument('output', help='path to output file')
    return parser.parse_args()


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_args(args)
    data = aws.s3read_csv(args.inpath, sep='|')
    print(data.head())


if __name__ == "__main__":
    main()

