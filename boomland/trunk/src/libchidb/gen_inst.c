#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "chidb.h"
#include "chidbInt.h"
#include "util.h"
#include "dbm.h"
#include "gen_inst.h"

/* Open a B-Tree for read-only access
 *
 * Poarameters:
 * - dbm: the DBM machine being used
 * - c: the cursor to point to the B-Tree
 * - r: a register containing a page number n
 * - n: the number of columns in the table
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_OpenRead(DBM *dbm, uint32_t c, uint32_t r, uint32_t n)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _OpenRead_;
    dbmi.p1 = c;
    dbmi.p2 = r;
    dbmi.p3 = n;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Open a B-Tree for read/write access
*
* Poarameters:
* - dbm: the DBM machine being used
* - c: the cursor to point to the B-Tree
* - r: a register containing a page number n
* - n: the number of columns in the table
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_OpenWrite(DBM *dbm, uint32_t c, uint32_t r, uint32_t n)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _OpenWrite_;
    dbmi.p1 = c;
    dbmi.p2 = r;
    dbmi.p3 = n;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Close and free a cursor
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor 
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Close(DBM *dbm, uint32_t c)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Close_;
    dbmi.p1 = c;
    dbmi.p2 = 0;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Point back at the first entry in a B-Tree
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - c: a cursor to point to a B-Tree entry
 * - j: a jump address if the B-Tree is empty
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_Rewind(DBM *dbm, uint32_t c, uint32_t j)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Rewind_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Advance a cursor to the next B-Tree entry
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor to point to a B-Tree entry
* - j: a jump address, unless there are no more entries
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Next(DBM *dbm, uint32_t c, uint32_t j)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Next_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Advance a cursor to the previous B-Tree entry
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor to point to a B-Tree entry
* - j: a jump address, unless there are no more entries
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Prev(DBM *dbm, uint32_t c, uint32_t j)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Prev_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Move the cursor to a specific entry
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor to point to a B-Tree entry
* - j: a jump address, unless there are no more entries
* - k: a key to seek for
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Seek(DBM *dbm, uint32_t c, uint32_t j, key_t k)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Seek_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = k;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Move the cursor to an entry with greater-than key value
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor to point to a B-Tree entry
* - j: a jump address, unless there are no more entries
* - r: a register containing a key k
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_SeekGt(DBM *dbm, uint32_t c, uint32_t j, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _SeekGt_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = r;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Move the cursor to an entry with greater-than or equal to key value
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor to point to a B-Tree entry
* - j: a jump address, unless there are no more entries
* - r: a register containing a key k
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_SeekGe(DBM *dbm, uint32_t c, uint32_t j, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _SeekGe_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = r;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Get an entry from a column
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor to point to a B-Tree entry
* - n: a column number
* - r: a register for storing the column entry
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Column(DBM *dbm, uint32_t c, uint32_t n, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Column_;
    dbmi.p1 = c;
    dbmi.p2 = n;
    dbmi.p3 = r;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Store a key in a register
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - c: the cursor pointing to the key to be added
 * - r: the register to store the value in
 * 
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_Key(DBM *dbm, uint32_t c, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Key_;
    dbmi.p1 = c;
    dbmi.p2 = r;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Store an integer in a register
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - n: the integer to be stored
 * - r: a register to store it in (do I need to malloc this register?)
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_Integer(DBM *dbm, int n, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Integer_;
    dbmi.p1 = n;
    dbmi.p2 = r;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Store a string in a register
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - string: the string to be stored
 * - r: a register to store it in
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_String(DBM *dbm, char *string, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _String_;
    dbmi.p1 = strlen(string);
    dbmi.p2 = r;
    dbmi.p3 = 0;
    dbmi.p4 = string;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Store a NULL value in a register
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - r: a register for storing the NULL value
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_Null(DBM *dbm, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Null_;
    dbmi.p2 = r;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Fetch a result row
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - r: a register
 * - n: collects values in r through r + n - 1
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_ResultRow(DBM *dbm, uint32_t r, int n)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _ResultRow_;
    dbmi.p1 = r;
    dbmi.p2 = n;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Create a database record
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - r1: a register
 * - n: uses values from r1 to r1 + n - 1
 * - r2: a register to store the database record in
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_MakeRecord(DBM *dbm, uint32_t r1, int n, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _MakeRecord_;
    dbmi.p1 = r1;
    dbmi.p2 = n;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Insert an entry in a B-Tree
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor pointing to a B-Tree
* - r1: register contains a database record v
* - r2: register contains a key k
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_InsertEntry(DBM *dbm, uint32_t c, uint32_t r1, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Insert_;
    dbmi.p1 = c;
    dbmi.p2 = r1;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for equality
*
* Parameters:
* - dbm: the DBM machine being used
* - r1: register containing a value
* - j: a jump address if the condition is met
* - r2: register containing a value
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Eq(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Eq_;
    dbmi.p1 = r1;
    dbmi.p2 = j;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for inequality
*
* Parameters:
* - dbm: the DBM machine being used
* - r1: register containing a value
* - j: a jump address if the condition is met
* - r2: register containing a value
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Ne(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Ne_;
    dbmi.p1 = r1;
    dbmi.p2 = j;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for less-than
*
* Parameters:
* - dbm: the DBM machine being used
* - r1: register containing a value
* - j: a jump address if the condition is met
* - r2: register containing a value
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Lt(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Lt_;
    dbmi.p1 = r1;
    dbmi.p2 = j;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for less-than or equals
*
* Parameters:
* - dbm: the DBM machine being used
* - r1: register containing a value
* - j: a jump address if the condition is met
* - r2: register containing a value
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Le(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Le_;
    dbmi.p1 = r1;
    dbmi.p2 = j;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for greater-than
*
* Parameters:
* - dbm: the DBM machine being used
* - r1: register containing a value
* - j: a jump address if the condition is met
* - r2: register containing a value
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Gt(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Gt_;
    dbmi.p1 = r1;
    dbmi.p2 = j;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for greater-than or equals
*
* Parameters:
* - dbm: the DBM machine being used
* - r1: register containing a value
* - j: a jump address if the condition is met
* - r2: register containing a value
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_Ge(DBM *dbm, uint32_t r1, uint32_t j, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Ge_;
    dbmi.p1 = r1;
    dbmi.p2 = j;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for greater-than for keys
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - c: a cursor pointing to an index entry
 * - j: a jump address if the condition is met
 * - r: register containing a key k
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_IdxGt(DBM *dbm, uint32_t c, uint32_t j, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _IdxGt_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = r;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for greater-than or equals for keys
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor pointing to an index entry
* - j: a jump address if the condition is met
* - r: register containing a key k
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_IdxGe(DBM *dbm, uint32_t c, uint32_t j, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _IdxGe_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = r;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for less-than for keys
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor pointing to an index entry
* - j: a jump address if the condition is met
* - r: register containing a key k
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_IdxLt(DBM *dbm, uint32_t c, uint32_t j, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _IdxLt_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = r;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Test for less-than or equals for keys
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor pointing to an index entry
* - j: a jump address if the condition is met
* - r: register containing a key k
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_IdxLe(DBM *dbm, uint32_t c, uint32_t j, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _IdxLe_;
    dbmi.p1 = c;
    dbmi.p2 = j;
    dbmi.p3 = r;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Store the PKey of an index entry
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor pointing to an index entry
* - r: a register to store a pkey
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_IdxKey(DBM *dbm, uint32_t c, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _IdxKey_;
    dbmi.p1 = c;
    dbmi.p2 = r;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Add a new key pair to a B-Tree
*
* Parameters:
* - dbm: the DBM machine being used
* - c: a cursor pointing to an index B-Tree
* - r1: a register containing an index key
* - r2: a register containing a pkey
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_IdxInsert(DBM *dbm, uint32_t c, uint32_t r1, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _IdxInsert_;
    dbmi.p1 = c;
    dbmi.p2 = r1;
    dbmi.p3 = r2;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Create a new Table B-Tree
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - r: a register to store a root page
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_CreateTable(DBM *dbm, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _CreateTable_;
    dbmi.p1 = r;
    dbmi.p2 = 0;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Create a new Index B-Tree
*
* Parameters:
* - dbm: the DBM machine being used
* - r: a register to store a root page
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_CreateIndex(DBM *dbm, uint32_t r)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _CreateIndex_;
    dbmi.p1 = r;
    dbmi.p2 = 0;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Make a shallow copy between registers
*
* Parameters:
* - dbm: the DBM machine being used
* - r1: a register containing a value
* - r2: a register to copy r1 into
*
* Returns:
* - CHIDB_OK
*/
int chidb_Gen_SCopy(DBM *dbm, uint32_t r1, uint32_t r2)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _SCopy_;
    dbmi.p1 = r1;
    dbmi.p2 = r2;
    dbmi.p3 = 0;
    dbmi.p4 = NULL;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}


/* Halt execution of a program, and possibly return an error
 *
 * Parameters:
 * - dbm: the DBM machine being used
 * - n: the integer code for the halt instruction
 * - msg: optional parameter with an error message
 *
 * Returns:
 * - CHIDB_OK
 */
int chidb_Gen_Halt(DBM *dbm, int n, char *msg)
{
    DBMInstruction dbmi;
    dbmi.machine = dbm;
    dbmi.id = dbm->ninstructions;
    dbmi.op = _Halt_;
    dbmi.p1 = n;
    dbmi.p2 = 0;
    dbmi.p3 = 0;
    dbmi.p4 = msg;
    return chidb_DBM_add_instruction(dbm, &dbmi);
}