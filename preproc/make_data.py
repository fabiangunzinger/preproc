#!/usr/bin/env python3

import aws

def main():
    fp = 's3://3di-data-mdb/raw/mdb_000.csv'
    data = aws.s3read_csv(fp, sep='|')
    print(data.head())


if __name__ == "__main__":
    main()

