#include <stdlib.h>
#include "CUnit/Basic.h"
#include "libchidb/schemaloader.h"

#define TESTFILE_1 ("example_dbs/singletable_singlepage.cdb")

void test_schemaLoad() {
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  Schema *schema;
  rc = chidb_loadSchema(db, &schema);
  CU_ASSERT(rc == CHIDB_OK);

  chidb_destroySchema(schema);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);
}

int init_tests_schema() {
	CU_pSuite schemaTests = NULL;

	/* add a suite to the registry */
	schemaTests = CU_add_suite("schema", NULL, NULL);
	if (NULL == schemaTests) {
		CU_cleanup_registry();
		return CU_get_error();
	}
  
	if (
		(NULL == CU_add_test(schemaTests, "Load a schema", test_schemaLoad))
		) {
    CU_cleanup_registry();
    return CU_get_error();
  }

	return CU_get_error();
}
