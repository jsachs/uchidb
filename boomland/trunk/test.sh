export LD_LIBRARY_PATH=`pwd`
echo 'Making project...' && make
echo "Enter 'c' and press enter to continue: "
read cont
if (( $cont == 'c' )); then
  echo 'Testing...'
  cd tests
  cp example_dbs/singletable_singlepage.cdb example_dbs/volatile.singletable_singlepage.cdb
  cp example_dbs/tableindex_singlepage.cdb example_dbs/volatile.tableindex_singlepage.cdb
  cp example_dbs/tableindex_multipage.cdb example_dbs/volatile.tableindex_multipage.cdb
  ./tests
  rm -rf example_dbs/volatile.singletable_singlepage.cdb
  rm -rf example_dbs/volatile.tableindex_singlepage.cdb
  rm -rf example_dbs/volatile.tableindex_multipage.cdb
  cd ..
fi
