#include <stdlib.h>
#include "CUnit/Basic.h"
#include "libchidb/btree.h"
#include "libchidb/dbm.h"

#define TESTFILE_1 ("example_dbs/singletable_singlepage.cdb")

void test_1a() {
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i0; i0.op = _Integer_; i0.p1 = 2; i0.p2 = 0; i0.p3 = 0; i0.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i0);
  CU_ASSERT(rc == CHIDB_OK);  

  DBMInstruction i1; i1.op = _OpenRead_; i1.p1 = 0; i1.p2 = 0; i1.p3 = 3; i1.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i1);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i2; i2.op = _Rewind_; i2.p1 = 0; i2.p2 = 7; i2.p3 = 0; i2.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i2);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i3; i3.op = _Column_; i3.p1 = 0; i3.p2 = 1; i3.p3 = 1; i3.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i3);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i4; i4.op = _Column_; i4.p1 = 0; i4.p2 = 2; i4.p3 = 2; i4.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i4);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i5; i5.op = _ResultRow_; i5.p1 = 1; i5.p2 = 2; i5.p3 = 0; i5.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i5);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i6; i6.op = _Next_; i6.p1 = 0; i6.p2 = 3; i6.p3 = 0; i6.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i6);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i7; i7.op = _Close_; i7.p1 = 0; i7.p2 = 0; i7.p3 = 0; i7.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i7);
  CU_ASSERT(rc == CHIDB_OK);

  DBMInstruction i8; i8.op = _Halt_; i8.p1 = 0; i8.p2 = 0; i8.p3 = 0; i8.p4 = NULL;
  rc = chidb_DBM_add_instruction(dbm, &i8);
  CU_ASSERT(rc == CHIDB_OK);

  // SELECT name, dept FROM courses;
  //
  // # | Opcode    | p1  | p2  | p3
  // ================================
  // 0 | Integer   | i=2 | r=0 
  // 1 | OpenRead  | c=0 | r=0 | n=3
  // 2 | Rewind    | c=0 | j=7
  // 3 | Column    | c=0 | n=1 | r=1
  // 4 | Column    | c=0 | n=2 | r=2
  // 5 | ResultRow | r=1 | n=2
  // 6 | Next      | c=0 | j=3
  // 7 | Close     | c=0
  // 8 | Halt      | n=0
  printf("\n");
  do {
    rc = chidb_DBM_execute(dbm);
    CU_ASSERT(rc == CHIDB_OK);

    if (dbm->returned) {
      printf("Result returned ...\n");
      chidb_DBRecord_print(dbm->result);
      printf("\n");
      fflush(stdout);
    }
  } while (CHIDB_OK == rc && !dbm->halted);

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);
}

int init_tests_dbm() {
	CU_pSuite dbmTests = NULL;

	/* add a suite to the registry */
	dbmTests = CU_add_suite("dbm", NULL, NULL);
	if (NULL == dbmTests) {
		CU_cleanup_registry();
		return CU_get_error();
	}
  
	if (
		(NULL == CU_add_test(dbmTests, "Simple select from singletable_singlepage", test_1a))
	) {
    CU_cleanup_registry();
    return CU_get_error();
  }

	return CU_get_error();
}
