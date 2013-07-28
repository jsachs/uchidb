#ifndef DBM_H_
#define DBM_H_

//#include "chidb.h"
#include "chidbInt.h"
#include "record.h"
#include "util.h"
#include "schemaloader.h"



// Forward declarations
typedef struct DBM DBM;
typedef struct DBMCursor DBMCursor;
typedef struct DBMRegister DBMRegister;
typedef struct DBMInstruction DBMInstruction;


// Map instruction names to codes
typedef enum {
  _OpenRead_,    // 0
  _OpenWrite_,   // 1
  _Close_,       // 2 
  _Rewind_,      // 3
  _Next_,        // 4
  _Prev_,        // 5
  _Seek_,        // 6
  _SeekGt_,      // 7
  _SeekGe_,      // 8
  _Column_,      // 9
  _Key_,         // 10
  _Integer_,     // 11
  _String_,      // 12
  _Null_,        // 13
  _ResultRow_,   // 14
  _MakeRecord_,  // 15
  _Insert_,      // 16
  _Eq_,          // 17
  _Ne_,          // 18
  _Lt_,          // 19
  _Le_,          // 20
  _Gt_,          // 21
  _Ge_,          // 22
  _IdxGt_,       // 23
  _IdxGe_,       // 24
  _IdxLt_,       // 25
  _IdxLe_,       // 26
  _IdxKey_,      // 27
  _IdxInsert_,   // 28
  _CreateTable_, // 29
  _CreateIndex_, // 20
  _SCopy_,       // 31
  _Halt_         // 32
} instruction_code;


// Instructions bind an operation name to some operands
// A program consists of an array of these instructions
struct DBMInstruction {
  DBM *machine;        // Pointer to machine cursor refers to
  uint32_t id;         // Instruction identifier (address for jumping)
  instruction_code op; // Instruction code
  int32_t  p1;         // First operand
  int32_t  p2;         // Second operand
  int32_t  p3;         // Third operand
  void *p4;            // Fourth operand
};


// Instructions may modify registers, which hold integer, string, or NULL values
// Using same register types as chidbInt's private codes
#define DBM_NULL_REGISTER_TYPE     0
#define DBM_BYTE_REGISTER_TYPE     1
#define DBM_SMALLINT_REGISTER_TYPE 2
#define DBM_INTEGER_REGISTER_TYPE  4
#define DBM_STRING_REGISTER_TYPE   13

struct DBMRegister {
  DBM *machine;       // Pointer to machine register refers to
  uint32_t id;        // Register identifier
  uint8_t type;       // Type of register (integer, string, or NULL)
  union {             // NULL type doesn't get anything...
    int32_t integer;  // Signed 32-bit integer
    int16_t smallint; // Signed 16-bit integer
    int8_t byte;      // Signed 8-bit integer
    struct {          // String or binary data
      size_t len;     // Length of data
      uint8_t *data;  // Data stored on heap
    } string;
  } fields;
};


// Can open a B-tree node as read-only or read-write
#define DBM_READONLY  0
#define DBM_READWRITE 1

struct DBMCursor {
  DBM *machine;     // Pointer to machine cursor refers to
  uint32_t id;      // Cursor identifier
  uint8_t mode;     // Read-only or read-write access
  uint32_t ncols;   // Number of columns in table
  uint32_t cell_id; // Index in DBM cell array
};


// Wrapper for B-Tree cells
// Keeps a pointer to the B-Tree node within the DBM
typedef struct {
  BTreeCell entry;
  BTreeNode *node;
  uint32_t node_offset;
} DBMCell;

// Instantaneous configuration of the machine itself
// Instructions and registers are stored on the heap
struct DBM {
  uint32_t pc;                  // Program counter (id of current instruction)
  DBMInstruction *instructions; // Program is a list of instructions
  uint32_t ninstructions;       // Number of instructions

  DBMRegister *registers;       // Registers and their values
  uint32_t nregisters;          // Number of registers

  DBMCursor *cursors;           // Cursors and their nodes
  uint32_t ncursors;            // Number of cursors

  chidb *db;                    // Database - should point to B-Tree file and contain schema
  BTreeNode **nodes;            // B-Tree nodes
  uint32_t nnodes;              // Number of B-Tree nodes

  DBMCell *cells;               // B-Tree cell wrappers (sorted)
  uint32_t ncells;              // Number of B-Tree cells

  bool jumped;                  // True if execution resulted in a jump
  bool returned;                // True if ResultRow returns
  bool halted;                  // True if machine not halted

  npage_t root_page;            // Root page in B-Tree file
  DBRecord *result;             // ResultRow result

  Schema_Table *maps;
  uint32_t nmaps;

  uint32_t err;                 // Error code
  char *err_msg;                // Error message
};


int chidb_DBM_create(chidb *db, DBM **machine);
int chidb_DBM_destroy(DBM *machine);
int chidb_DBM_add_instruction(DBM *machine, DBMInstruction *instruction);
int chidb_DBM_step(DBM *machine);

#endif
