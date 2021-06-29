# directories
MDB_DATA := "s3://3di-data-mdb"
MDB_RAW := $(MDB_DATA)/raw/mdb_000.csv
MDB_CLEAN := $(MDB_DATA)/panel/mdb_000.parquet

tmp := $$( aws s3 ls s3://3di-data-mdb/clean/ --profile 'tracker-fgu' )

.PHONY: mdb
mdb:
	@python preproc/make_data.py $(MDB_RAW) $(MDB_CLEAN)
