#ifndef GEN_H_
#define GEN_H_

#include "schemaloader.h"


int chidb_Gen(SQLStatement *stmt, DBM *dbm, Schema *schema);
int chidb_Gen_SelectStmt(SelectStatement *stmt, DBM *dbm, Schema *schema);
int chidb_Gen_InsertStmt(InsertStatement *stmt, DBM *dbm, Schema *schema);
int chidb_Gen_CreateTableStmt(CreateTableStatement *stmt, DBM *dbm, Schema *schema);
int chidb_Gen_CreateIndexStmt(CreateIndexStatement *stmt, DBM *dbm, Schema *schema);

int chidb_Gen_condition_register(Condition *cond, DBM *dbm, uint32_t reg);
int chidb_Gen_get_column_no(Schema_Table *st, char *table, char *name, int8_t ntables);
int chidb_Gen_get_table_no(Schema_Table *st, char *table, int8_t ntables);
int chidb_Gen_cond_op_register(Condition *cond, DBM *dbm, uint32_t jump, uint32_t reg, int first_reg);
int chidb_Gen_make_result_row(DBM *dbm, Schema_Table *st, uint8_t ncols, Column *cols, uint32_t start_reg, int8_t ntables);

// obsolete eventually
int chidb_Gen_getColNo(int ncols, Schema_ColumnMap *cmap, char *name);

#endif