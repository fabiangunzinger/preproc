# directories
MDB_DATA := "s3://3di-data-mdb"
MDB_RAW := $(MDB_DATA)/raw/mdb_000.csv
MDB_CLEAN := $(MDB_DATA)/clean/mdb_000.parquet

.PHONY: mdb
# .DELETE_ON_ERROR: $(MDB_CLEAN)

mdb:
	echo "hello"

# mdb: $(MDB_CLEAN)

# $(MDB_CLEAN): $(MDB_RAW)
	# @python preproc/make_data.py $^
	echo $^
