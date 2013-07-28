all: libchidb shell-main tests

.PHONY: libchidb tests clean

libchidb: 
	$(MAKE) -C src/libchidb

shell: 
	$(MAKE) -C src/shell

shell-main: 
	$(MAKE) -C src/shell ../../chidb-main

tests: 
	$(MAKE) -C tests

clean: 
	$(MAKE) clean -C src/libchidb
	$(MAKE) clean -C src/shell	
	$(MAKE) clean -C tests	