#include <stdlib.h>
#include "CUnit/Basic.h"
#include "libchidb/btree.h"
#include "libchidb/dbm.h"
#include "libchidb/util.h"
#include "libchidb/parser.h"
#include "libchidb/gen.h"
#include "libchidb/schemaloader.h"

#define TESTFILE_1 ("example_dbs/volatile.singletable_singlepage.cdb")
#define TESTFILE_2 ("example_dbs/volatile.tableindex_singlepage.cdb")
#define TESTFILE_3 ("example_dbs/volatile.tableindex_multipage.cdb")
#define TESTFILE_4 ("example_dbs/join-these.cdb")

void test_print_instructions(DBM *dbm)
{
    int i;
    for(i=0; i < dbm->ninstructions; i++){
        printf("\n\t%02d : %02d - %-3d | %-3d | %-3d",
            (dbm->instructions[i]).id,
            (dbm->instructions[i]).op,
            (dbm->instructions[i]).p1,
            (dbm->instructions[i]).p2,
            (dbm->instructions[i]).p3);
    }
    printf("\n");
    return;
}

void test_Select_1()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT * FROM courses;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);
  
  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);
  
  chidb_Gen(stmt, dbm, schema);
    
  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}

void test_Select_2()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);
  
  const char *sql = "SELECT code, name, prof, dept FROM courses WHERE dept = 42;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Select_3()
{
    int rc;
    chidb *db;
    db = malloc(sizeof(chidb));
    rc = chidb_Btree_open(TESTFILE_2, db, &db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    
    DBM *dbm;
    rc = chidb_DBM_create(db, &dbm);
    CU_ASSERT(rc == CHIDB_OK);
    
    const char *sql = "SELECT textcode FROM numbers WHERE altcode = 20300;";
    printf("\n\t%s", sql);
    SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
    chidb_parser(sql, &stmt);
    
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    chidb_loadSchema(db, &schema);
    
    chidb_Gen(stmt, dbm, schema);
    
    test_print_instructions(dbm);
    
    // Running the program...
    printf("\n\tResults ...");
    while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
      printf("\n\t");
      chidb_DBRecord_print(dbm->result);
      fflush(stdout);
    }
    CU_ASSERT(rc == CHIDB_DONE);
    printf("\n");
    
    rc = chidb_DBM_destroy(dbm);
    CU_ASSERT(rc == CHIDB_OK);
    
    rc = chidb_Btree_close(db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    free(db);

    return;
}

void test_Select_4()
{
    int rc;
    chidb *db;
    db = malloc(sizeof(chidb));
    rc = chidb_Btree_open(TESTFILE_3, db, &db->bt);
    CU_ASSERT(rc == CHIDB_OK);

    DBM *dbm;
    rc = chidb_DBM_create(db, &dbm);
    CU_ASSERT(rc == CHIDB_OK);

    const char *sql = "SELECT code, textcode, altcode FROM numbers;";
    printf("\n\t%s", sql);
    SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
    chidb_parser(sql, &stmt);

    Schema *schema = (Schema *) malloc(sizeof(Schema));
    chidb_loadSchema(db, &schema);

    chidb_Gen(stmt, dbm, schema);

    test_print_instructions(dbm);

    // Running the program...
    printf("\n\tResults ...");
    while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
      printf("\n\t");
      chidb_DBRecord_print(dbm->result);
      fflush(stdout);
    }
    CU_ASSERT(rc == CHIDB_DONE);
    printf("\n");

    rc = chidb_DBM_destroy(dbm);
    CU_ASSERT(rc == CHIDB_OK);

    rc = chidb_Btree_close(db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    free(db);

    return;
}

void test_Select_5()
{
    int rc;
    chidb *db;
    db = malloc(sizeof(chidb));
    rc = chidb_Btree_open(TESTFILE_3, db, &db->bt);
    CU_ASSERT(rc == CHIDB_OK);

    DBM *dbm;
    rc = chidb_DBM_create(db, &dbm);
    CU_ASSERT(rc == CHIDB_OK);

    const char *sql = "SELECT code FROM numbers WHERE code > 3000;";
    printf("\n\t%s", sql);
    SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
    chidb_parser(sql, &stmt);

    Schema *schema = (Schema *) malloc(sizeof(Schema));
    chidb_loadSchema(db, &schema);

    chidb_Gen(stmt, dbm, schema);

    test_print_instructions(dbm);

    // Running the program...
    printf("\n\tResults ...");
    while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
      printf("\n\t");
      chidb_DBRecord_print(dbm->result);
      fflush(stdout);
    }
    CU_ASSERT(rc == CHIDB_DONE);
    printf("\n");

    rc = chidb_DBM_destroy(dbm);
    CU_ASSERT(rc == CHIDB_OK);

    rc = chidb_Btree_close(db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    free(db);

    return;
}


void test_Select_6()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT code, name, prof, dept FROM courses WHERE dept = 42 AND code = 23500;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Select_7()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT code, name, prof, dept FROM courses WHERE dept = 42 AND code = 23600;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Select_8()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_4, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT t1.k1,t1.v1,t2.k2,t2.k1,t2.k3 FROM t1, t2;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Select_9()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_4, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT * FROM t1, t2, t3;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}

void test_Select_10()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_4, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT * FROM t1, t2 WHERE t2.k1 = 3;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Select_11()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_4, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT * FROM t1, t2 WHERE t1.k1 = 3 AND t2.k1 = 3";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Select_12()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_4, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT * FROM t2 WHERE k1 > k2";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Select_13()
{
  int rc;
  chidb *db;
  db = malloc(sizeof(chidb));
  rc = chidb_Btree_open(TESTFILE_4, db, &db->bt);
  CU_ASSERT(rc == CHIDB_OK);

  DBM *dbm;
  rc = chidb_DBM_create(db, &dbm);
  CU_ASSERT(rc == CHIDB_OK);

  const char *sql = "SELECT t1.v1 FROM t1, t2;";
  printf("\n\t%s", sql);
  SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
  chidb_parser(sql, &stmt);

  Schema *schema = (Schema *) malloc(sizeof(Schema));
  chidb_loadSchema(db, &schema);

  chidb_Gen(stmt, dbm, schema);

  test_print_instructions(dbm);

  // Running the program...
  printf("\n\tResults ...");
  while (CHIDB_ROW == (rc = chidb_DBM_step(dbm))) {
    printf("\n\t");
    chidb_DBRecord_print(dbm->result);
    fflush(stdout);
  }
  CU_ASSERT(rc == CHIDB_DONE);
  printf("\n");

  rc = chidb_DBM_destroy(dbm);
  CU_ASSERT(rc == CHIDB_OK);

  rc = chidb_Btree_close(db->bt);
  CU_ASSERT(rc == CHIDB_OK);
  free(db);

  return;
}


void test_Insert_1()
{
    int rc;
    chidb *db;
    db = malloc(sizeof(chidb));
    rc = chidb_Btree_open(TESTFILE_1, db, &db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    
    DBM *dbm;
    rc = chidb_DBM_create(db, &dbm);
    CU_ASSERT(rc == CHIDB_OK);
    
    const char *sql = "INSERT INTO courses VALUES (32000, \"Graduate Algorithms\", 60, 42);";
    printf("\n\t%s", sql);
    SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
    chidb_parser(sql, &stmt);
    
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    chidb_loadSchema(db, &schema);
    
    chidb_Gen(stmt, dbm, schema);
    test_print_instructions(dbm);

    // Running the program...
    printf("\n\tInserting ...");
    rc = chidb_DBM_step(dbm);
    CU_ASSERT(rc == CHIDB_DONE);
    printf("\n");

    rc = chidb_DBM_destroy(dbm);
    CU_ASSERT(rc == CHIDB_OK);
    
    rc = chidb_Btree_close(db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    free(db);

    return;
}


void test_Insert_2()
{
    int rc;
    chidb *db;
    db = malloc(sizeof(chidb));
    rc = chidb_Btree_open(TESTFILE_2, db, &db->bt);
    CU_ASSERT(rc == CHIDB_OK);

    DBM *dbm;
    rc = chidb_DBM_create(db, &dbm);
    CU_ASSERT(rc == CHIDB_OK);

    const char *sql = "INSERT INTO numbers VALUES(400, \"foo400\", 20400);";
    printf("\n\t%s", sql);
    SQLStatement *stmt = (SQLStatement *)malloc(sizeof(SQLStatement));
    chidb_parser(sql, &stmt);

    Schema *schema = (Schema *) malloc(sizeof(Schema));
    chidb_loadSchema(db, &schema);

    chidb_Gen(stmt, dbm, schema);

    test_print_instructions(dbm);

    // Running the program...
    printf("\n\tInserting ...");
    rc = chidb_DBM_step(dbm);
    CU_ASSERT(rc == CHIDB_DONE);
    printf("\n");

    rc = chidb_DBM_destroy(dbm);
    CU_ASSERT(rc == CHIDB_OK);

    rc = chidb_Btree_close(db->bt);
    CU_ASSERT(rc == CHIDB_OK);
    free(db);

    return;
}



int init_tests_gen() {
    CU_pSuite genTests = NULL;

    /* add a suite to the registry */
    genTests = CU_add_suite("gen", NULL, NULL);
    if (NULL == genTests) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if ((NULL == CU_add_test(genTests, "SELECT * 1", test_Select_1))) {
    CU_cleanup_registry();
    return CU_get_error();
    }

    if ((NULL == CU_add_test(genTests, "SELECT/WHERE 1", test_Select_2))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT/WHERE 2", test_Select_3))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT/WHERE 3", test_Select_4))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT/WHERE 4", test_Select_5))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT/WHERE/AND 1", test_Select_6))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT/WHERE/AND 2", test_Select_7))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT Multiple Table 1", test_Select_8))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT Multiple Table 2", test_Select_9))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT Multiple Table WHERE 1", test_Select_10))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT Multiple Table WHERE/AND 1", test_Select_11))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT/WHERE Two Columns 1", test_Select_12))) {
    CU_cleanup_registry();
    return CU_get_error();
    }
    
    if ((NULL == CU_add_test(genTests, "SELECT Multiple Table 0", test_Select_13))) {
    CU_cleanup_registry();
    return CU_get_error();
    }

    if ((NULL == CU_add_test(genTests, "INSERT 1", test_Insert_1))) {
    CU_cleanup_registry();
    return CU_get_error();
    }

    if ((NULL == CU_add_test(genTests, "INSERT 2", test_Insert_2))) {
    CU_cleanup_registry();
    return CU_get_error();
    }

    return CU_get_error();
}