export LD_LIBRARY_PATH=`pwd`
make > /dev/null
cd tests
cp example_dbs/singletable_singlepage.cdb example_dbs/volatile.singletable_singlepage.cdb
cp example_dbs/tableindex_singlepage.cdb example_dbs/volatile.tableindex_singlepage.cdb
cp example_dbs/tableindex_multipage.cdb example_dbs/volatile.tableindex_multipage.cdb
gdb tests
rm -rf example_dbs/volatile.singletable_singlepage.cdb
rm -rf example_dbs/volatile.tableindex_singlepage.cdb
rm -rf example_dbs/volatile.tableindex_multipage.cdb
cd ..
