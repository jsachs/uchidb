#ifndef DBM_DECLS_H_
#define DBM_DECLS_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <chidbInt.h>
#include <chidb.h>
#include "btree.h"
#include "util.h"
#include "dbm.h"


// Machine state and utilities
int chidb_DBM_execute(DBM *machine);
int chidb_DBM_grab_cells(DBM *machine);
int chidb_DBM_jump(DBM *machine, uint32_t instruction_id);
int chidb_DBM_find_instruction(DBM *machine, uint32_t instruction_id, DBMInstruction **instruction);
int chidb_DBM_find_register(DBM *machine, uint32_t reg_id, DBMRegister **reg);
int chidb_DBM_create_register(DBM *machine, uint32_t reg_id, DBMRegister **reg);
int chidb_DBM_find_or_create_register(DBM *machine, uint32_t reg_id, DBMRegister **reg);
int chidb_DBM_free_register(DBMRegister *reg);
int chidb_DBM_find_cursor(DBM *machine, uint32_t cursor_id, DBMCursor **cursor);
int chidb_DBM_create_cursor(DBM *machine, uint32_t cursor_id, DBMCursor **cursor);
int chidb_DBM_find_or_create_cursor(DBM *machine, uint32_t cursor_id, DBMCursor **cursor);
int chidb_DBM_find_node(DBM *machine, npage_t page_num, BTreeNode **node);

// Instructions
int chidb_DBM_execute_Open(DBM *machine, DBMCursor *cursor, DBMRegister *reg, uint32_t ncols, uint8_t mode);
int chidb_DBM_execute_OpenRead(DBM *machine, DBMCursor *cursor, DBMRegister *reg, uint32_t ncols);
int chidb_DBM_execute_OpenWrite(DBM *machine, DBMCursor *cursor, DBMRegister *reg, uint32_t ncols);
int chidb_DBM_execute_Close(DBM *machine, DBMCursor *cursor);
int chidb_DBM_execute_Rewind(DBM *machine, DBMCursor *cursor, uint32_t instruction_id);
int chidb_DBM_execute_Next(DBM *machine, DBMCursor *cursor, uint32_t instruction_id);
int chidb_DBM_execute_Prev(DBM *machine, DBMCursor *cursor, uint32_t instruction_id);
int chidb_DBM_execute_Seek(DBM *machine, DBMCursor *cursor, key_t key, uint32_t instruction_id);
int chidb_DBM_execute_SeekGt(DBM *machine, DBMCursor *cursor, key_t key, uint32_t instruction_id);
int chidb_DBM_execute_SeekGe(DBM *machine, DBMCursor *cursor, key_t key, uint32_t instruction_id);
int chidb_DBM_execute_Column(DBM *machine, DBMCursor *cursor, int32_t col_num, DBMRegister *reg);
int chidb_DBM_execute_Key(DBM *machine, DBMCursor cursor, DBMRegister *reg);
int chidb_DBM_execute_Integer(DBM *machine, DBMRegister *reg, int32_t integer);
int chidb_DBM_execute_String(DBM *machine, DBMRegister *reg, void *data, size_t len);
int chidb_DBM_execute_Null(DBM *machine, DBMRegister *reg);
int chidb_DBM_execute_ResultRow(DBM *machine, uint32_t reg_id, int32_t ncols);
int chidb_DBM_execute_MakeRecord(DBM *machine, uint32_t reg_id, int32_t ncols, DBMRegister *result_reg);
int chidb_DBM_execute_Insert(DBM *machine, DBMCursor *cursor, key_t key, DBRecord *record);
int chidb_DBM_execute_Eq(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id);
int chidb_DBM_execute_Ne(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id);
int chidb_DBM_execute_Lt(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id);
int chidb_DBM_execute_Le(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id);
int chidb_DBM_execute_Gt(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id);
int chidb_DBM_execute_Ge(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id);
int chidb_DBM_execute_IdxGe(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id);
int chidb_DBM_execute_IdxGt(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id);
int chidb_DBM_execute_IdxLt(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id);
int chidb_DBM_execute_IdxLe(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id);
int chidb_DBM_execute_IdxKey(DBM *machine, DBMCursor cursor, DBMRegister *reg);
int chidb_DBM_execute_IdxInsert(DBM *machine, DBMRegister reg1, DBMRegister reg2, DBMCursor cursor);
// int chidb_DBM_execute_CreateTable(DBM *machine, ...);
// int chidb_DBM_execute_CreateIndex(DBM *machine, ...);
int chidb_DBM_execute_SCopy(DBM *machine, DBMRegister *reg1, DBMRegister *reg2);
int chidb_DBM_execute_Halt(DBM *machine, uint32_t err, const char *err_msg);

#endif