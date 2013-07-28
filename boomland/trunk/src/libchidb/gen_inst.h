#ifndef GENINST_H_
#define GENINST_H_

int chidb_Gen_OpenRead(DBM *dbm, uint32_t c, uint32_t r, uint32_t n);
int chidb_Gen_OpenWrite(DBM *dbm, uint32_t c, uint32_t r, uint32_t n);
int chidb_Gen_Close(DBM *dbm, uint32_t c);

int chidb_Gen_Rewind(DBM *dbm, uint32_t c, uint32_t j);
int chidb_Gen_Next(DBM *dbm, uint32_t c, uint32_t j);
int chidb_Gen_Prev(DBM *dbm, uint32_t c, uint32_t j);
int chidb_Gen_Seek(DBM *dbm, uint32_t c, uint32_t j, key_t k);
int chidb_Gen_SeekGt(DBM *dbm, uint32_t c, uint32_t j, uint32_t r);
int chidb_Gen_SeekGe(DBM *dbm, uint32_t c, uint32_t j, uint32_t r);
int chidb_Gen_Column(DBM *dbm, uint32_t c, uint32_t n, uint32_t r);

int chidb_Gen_Key(DBM *dbm, uint32_t c, uint32_t r);
int chidb_Gen_Integer(DBM *dbm, int n, uint32_t r);
int chidb_Gen_String(DBM *dbm, char *string, uint32_t r);
int chidb_Gen_Null(DBM *dbm, uint32_t r);

int chidb_Gen_ResultRow(DBM *dbm, uint32_t r, int n);
int chidb_Gen_MakeRecord(DBM *dbm, uint32_t r1, int n, uint32_t r2);
int chidb_Gen_InsertEntry(DBM *dbm, uint32_t c, uint32_t r1, uint32_t r2);

int chidb_Gen_Eq(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2);
int chidb_Gen_Ne(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2);
int chidb_Gen_Lt(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2);
int chidb_Gen_Le(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2);
int chidb_Gen_Gt(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2);
int chidb_Gen_Ge(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2);

int chidb_Gen_IdxGt(DBM *dbm, uint32_t c, uint32_t j, uint32_t r);
int chidb_Gen_IdxGe(DBM *dbm, uint32_t c, uint32_t j, uint32_t r);
int chidb_Gen_IdxLt(DBM *dbm, uint32_t c, uint32_t j, uint32_t r);
int chidb_Gen_IdxLe(DBM *dbm, uint32_t c, uint32_t j, uint32_t r);

int chidb_Gen_IdxKey(DBM *dbm, uint32_t c, uint32_t r);
int chidb_Gen_IdxInsert(DBM *dbm, uint32_t c, uint32_t r1, uint32_t r2);

int chidb_Gen_CreateTable(DBM *dbm, uint32_t r);
int chidb_Gen_CreateIndex(DBM *dbm, uint32_t r);

int chidb_Gen_SCopy(DBM *dbm, uint32_t r1, uint32_t r2);
int chidb_Gen_Halt(DBM *dbm, int n, char* msg);


#endif