#include "dbmInt.h"


/* Create a database machine
 *
 * Parameters
 * - bt: An already-open B-tree file
 * - machine: An out parameters. Used to return a pointer to the newly created DBM
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_create(chidb *db, DBM **machine) {
  DBM *newMachine = (DBM *) malloc(sizeof(DBM));
  if (NULL == newMachine) return CHIDB_ENOMEM;

  // Nothing in the machine just yet, but allocate space anyway
  newMachine->pc            = 0;
  newMachine->instructions  = malloc(sizeof(DBMInstruction));
  newMachine->ninstructions = 0;
  newMachine->registers     = malloc(sizeof(DBMRegister));
  newMachine->nregisters    = 0;
  newMachine->cursors       = malloc(sizeof(DBMCursor));
  newMachine->ncursors      = 0;
  newMachine->db            = db;

  newMachine->nnodes = newMachine->db->bt->pager->n_pages;
  newMachine->nodes  = malloc(newMachine->nnodes * sizeof(BTreeNode));

  int rc;
  BTreeNode *btn;
  for (npage_t i = 1; i <= newMachine->nnodes; ++i) {
    rc = chidb_Btree_getNodeByPage(newMachine->db->bt, i, &btn);
    if (CHIDB_OK != rc) return rc;
    newMachine->nodes[i-1] = btn;
  }

  newMachine->ncells = 0;
  newMachine->cells  = malloc(sizeof(DBMCell));
  if (NULL == newMachine->cells) return CHIDB_ENOMEM;
  rc = chidb_DBM_grab_cells(newMachine);
  if (CHIDB_OK != rc) return rc;

  newMachine->jumped    = false;
  newMachine->returned  = false;
  newMachine->halted    =  true; // Not running yet
  newMachine->root_page =    -1; // Bogus
  newMachine->maps      =  NULL;
  newMachine->nmaps     =     0;
  newMachine->result    =  NULL;

  *machine = newMachine;
  return CHIDB_OK;
}



/* Destroy a database machine
 *
 * Parameters
 * - machine: DBM to close
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_DBM_destroy(DBM *machine) {
  int rc;

  for (uint32_t i = 0; i < machine->ncursors; ++i) {
    rc = chidb_DBM_execute_Close(machine, &machine->cursors[i]);
    if (CHIDB_OK != rc) return rc;
  }

  for (uint32_t i = 0; i < machine->nregisters; ++i) {
    chidb_DBM_free_register(&machine->registers[i]);
  }

  for (uint32_t i = 0; i < machine->nnodes; ++i) {
    rc = chidb_Btree_freeMemNode(machine->db->bt, machine->nodes[i]);
    if (CHIDB_OK != rc) return rc;
  }

  if (machine->nmaps > 0) {
    free(machine->maps);
  }

  if (machine->err > 0) {
    free(machine->err_msg);
  }

  if (machine->result != NULL) {
    chidb_DBRecord_destroy(machine->result);
  }

  free(machine);
  machine = NULL;
  return CHIDB_OK;
}



/* Add an instruction to the machine's program
 *
 * Parameters
 * - machine: DBM to act upon
 * - instruction: Instruction to copy
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_add_instruction(DBM *machine, DBMInstruction *instruction) {
  machine->instructions = realloc(machine->instructions, (machine->ninstructions + 1) * sizeof(DBMInstruction));
  if (NULL == machine->instructions) return CHIDB_ENOMEM;

  machine->instructions[machine->ninstructions]         = *instruction;
  machine->instructions[machine->ninstructions].machine = machine;
  machine->instructions[machine->ninstructions].id      = machine->ninstructions;

  machine->ninstructions++;
  return CHIDB_OK;
}



/* Execute the machine's next instruction
 *
 * Parameters
 * - machine: DBM to act upon
 *
 * Return
 * - CHIDB_OK: Operation successful
 * ...
 */
int chidb_DBM_execute(DBM *machine) {
  int rc = CHIDB_OK;

  machine->halted   = false;
  machine->returned = false;
  machine->jumped   = false;

  DBMInstruction *inst = &machine->instructions[machine->pc];

  if (_OpenRead_ == inst->op) {
    DBMCursor *cursor;
    DBMRegister *reg;
    rc = chidb_DBM_find_or_create_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_register(machine, inst->p2, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_OpenRead(machine, cursor, reg, inst->p3);
  }

  if (_OpenWrite_ == inst->op) {
    DBMCursor *cursor;
    DBMRegister *reg;
    rc = chidb_DBM_find_or_create_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_register(machine, inst->p2, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_OpenWrite(machine, cursor, reg, inst->p3);
  }

  if (_Close_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Close(machine, cursor);
  }

  if (_Rewind_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Rewind(machine, cursor, inst->p2);
  }

  if (_Next_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Next(machine, cursor, inst->p2);
  }

  if (_Prev_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Prev(machine, cursor, inst->p2);
  }

  if (_Seek_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Seek(machine, cursor, inst->p3, inst->p2);
  }

  if (_SeekGt_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_SeekGt(machine, cursor, inst->p3, inst->p2);
  }

  if (_SeekGe_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_SeekGe(machine, cursor, inst->p3, inst->p2);
  }

  if (_Column_ == inst->op) {
    DBMCursor *cursor;
    DBMRegister *reg;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_or_create_register(machine, inst->p3, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Column(machine, cursor, inst->p2, reg);
  }

  if (_Key_ == inst->op) {
    DBMCursor *cursor;
    DBMRegister *reg;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_or_create_register(machine, inst->p2, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Key(machine, *cursor, reg);
  }

  if (_Integer_ == inst->op) {
    DBMRegister *reg;
    rc = chidb_DBM_find_or_create_register(machine, inst->p2, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Integer(machine, reg, inst->p1);
  }

  if (_String_ == inst->op) {
    DBMRegister *reg;
    rc = chidb_DBM_find_or_create_register(machine, inst->p2, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_String(machine, reg, inst->p4, inst->p1);
  }

  if (_Null_ == inst->op) {
    DBMRegister *reg;
    rc = chidb_DBM_find_or_create_register(machine, inst->p2, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Null(machine, reg);
  }

  if (_ResultRow_ == inst->op) {
    rc = chidb_DBM_execute_ResultRow(machine, inst->p1, inst->p2);
  }

  if (_MakeRecord_ == inst->op) {
    DBMRegister *reg;
    rc = chidb_DBM_find_or_create_register(machine, inst->p3, &reg);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_MakeRecord(machine, inst->p1, inst->p2, reg);
  }

  if (_Insert_ == inst->op) {
    DBMCursor *cursor;
    rc = chidb_DBM_find_cursor(machine, inst->p1, &cursor);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p2, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if (CHIDB_OK != rc) return rc;

    // Must have an integer key and a db-record stored in a string
    if (DBM_STRING_REGISTER_TYPE == reg1->type && DBM_INTEGER_REGISTER_TYPE == reg2->type) {
      DBRecord *record;
      rc = chidb_DBRecord_unpack(&record, reg1->fields.string.data);
      if (CHIDB_OK != rc) return rc;
      rc = chidb_DBM_execute_Insert(machine, cursor, reg2->fields.integer, record);
    }
  }

  if (_Eq_ == inst->op) {
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p1, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Eq(machine, *reg1, *reg2, inst->p2);
  }

  if (_Ne_ == inst->op) {
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p1, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Ne(machine, *reg1, *reg2, inst->p2);
  }

  if (_Lt_ == inst->op) {
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p1, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Lt(machine, *reg1, *reg2, inst->p2);
  }

  if (_Le_ == inst->op) {
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p1, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Le(machine, *reg1, *reg2, inst->p2);
  }

  if (_Gt_ == inst->op) {
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p1, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Gt(machine, *reg1, *reg2, inst->p2);
  }

  if (_Ge_ == inst->op) {
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p1, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_Ge(machine, *reg1, *reg2, inst->p2);
  }

  if (_IdxGt_ == inst->op) {
    DBMRegister *reg;
    DBMCursor *cursor;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg);
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_cursor(machine, inst->p1,&cursor); 
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_IdxGt(machine,*reg,*cursor,inst->p2);
  }

  if (_IdxGe_ == inst->op) {
    DBMRegister *reg;
    DBMCursor *cursor;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg);
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_cursor(machine, inst->p1,&cursor); 
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_IdxGe(machine,*reg,*cursor,inst->p2);
  }

  if (_IdxLt_ == inst->op) {
    DBMRegister *reg;
    DBMCursor *cursor;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg);
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_cursor(machine, inst->p1,&cursor); 
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_IdxLt(machine,*reg,*cursor,inst->p2);  
  }

  if (_IdxLe_ == inst->op) {
    DBMRegister *reg;
    DBMCursor *cursor;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg);
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_cursor(machine, inst->p1,&cursor); 
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_IdxLe(machine,*reg,*cursor,inst->p2);
  }

  if (_IdxKey_ == inst->op) {
    DBMRegister *reg;
    DBMCursor *cursor;
    rc = chidb_DBM_find_register(machine, inst->p2, &reg);
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_cursor(machine, inst->p1,&cursor); 
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_IdxKey(machine,*cursor,reg);
  }

  if (_IdxInsert_ == inst->op) {
    DBMRegister *reg1, *reg2;
    DBMCursor *cursor;
    rc = chidb_DBM_find_register(machine, inst->p2, &reg1);
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_register(machine, inst->p3, &reg2);
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_find_cursor(machine, inst->p1,&cursor); 
    if(CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_IdxInsert(machine,*reg1,*reg2,*cursor);
  }

/*
  if (_CreateTable_ == inst->op) {}

  if (_CreateIndex_ == inst->op) {}
*/

  if (_SCopy_ == inst->op) {
    DBMRegister *reg1;
    rc = chidb_DBM_find_register(machine, inst->p1, &reg1);
    if (CHIDB_OK != rc) return rc;
    DBMRegister *reg2;
    rc = chidb_DBM_find_register(machine, inst->p2, &reg2);
    if (CHIDB_OK != rc) return rc;
    rc = chidb_DBM_execute_SCopy(machine, reg1, reg2);
  }

  if (_Halt_ == inst->op) {
    rc = chidb_DBM_execute_Halt(machine, inst->p1, inst->p4);
  }

  if (CHIDB_OK == rc && !machine->jumped) ++machine->pc;
  return rc;
}



/* Step through the machine's program
 *
 * Parameters
 * - machine: DBM to act upon
 *
 * Return
 * - CHIDB_ROW: Result row returned
 * - CHIDB_DONE: Operation successful
 * ...
 */
int chidb_DBM_step(DBM *machine) {
  int rc;
  do {
    rc = chidb_DBM_execute(machine);
    if (CHIDB_OK == rc && machine->returned) {
      return CHIDB_ROW;
    }
  } while (CHIDB_OK == rc && !machine->halted);
  return (rc == CHIDB_OK) ? CHIDB_DONE : rc;
}



/* Fill in cells given the machine current B-Tree nodes
 *
 * Parameters
 * - machine: DBM to act upon
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_grab_cells(DBM *machine) {
  int rc;
  BTreeCell btc;

  // Skip schema on page 1 (index 0 in DBM)
  for (npage_t i = 1; i < machine->nnodes; ++i) {
    uint32_t ncells = machine->nodes[i]->n_cells;

    // Only read in table leaf cells
    if (PGTYPE_TABLE_LEAF != machine->nodes[i]->type) continue;

    // Remember, we'll have multiple tables, so store
    // in each cell the offset where the table starts
    uint32_t node_id_offset = machine->ncells;

    for (uint32_t j = 0; j < ncells; ++j) {
      rc = chidb_Btree_getCell(machine->nodes[i], j, &btc);
      if (CHIDB_OK != rc) return rc;

      // Insert cell into machine->cells in key order
      if (PGTYPE_TABLE_LEAF == btc.type) {
        machine->cells                              = realloc(machine->cells, (machine->ncells + 1) * sizeof(DBMCell));
        machine->cells[machine->ncells].entry       = btc;
        machine->cells[machine->ncells].node        = machine->nodes[i];
        machine->cells[machine->ncells].node_offset = node_id_offset;
        ++machine->ncells;
      }
    }
  }

  return CHIDB_OK;
}



/* Jump to a given instruction (i.e. repoint the pc)
 *
 * Parameters
 * - machine: DBM to act upon
 * - instruction_id: Identifier of instruction to jump to
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_jump(DBM *machine, uint32_t instruction_id) {
  for (uint32_t i = 0; i < machine->ninstructions; ++i) {
    if (instruction_id == machine->instructions[i].id) {
      machine->pc     = machine->instructions[i].id;
      machine->jumped = true;
      return CHIDB_OK;
    }
  }

  return CHIDB_ENOTFOUND;
}



/* Find a particular DBM instruction
 *
 * Parameters
 * - machine: DBM to act upon
 * - instruction_id: Instruction identifier
 * - instruction: Out parameter; will point to instruction
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_find_instruction(DBM *machine, uint32_t instruction_id, DBMInstruction **instruction) {
  for (uint32_t i = 0; i < machine->ninstructions; ++i) {
    if (instruction_id == machine->instructions[i].id) {
      *instruction = &machine->instructions[i];
      return CHIDB_OK;
    }
  }

  return CHIDB_ENOTFOUND;
}



/* Find a particular DBM register
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg_id: Register identifier
 * - reg: Out parameter; will point to register
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find register
 */
int chidb_DBM_find_register(DBM *machine, uint32_t reg_id, DBMRegister **reg) {
  for (uint32_t i = 0; i < machine->nregisters; ++i) {
    if (reg_id == machine->registers[i].id) {
      *reg = &machine->registers[i];
      return CHIDB_OK;
    }
  }

  return CHIDB_ENOTFOUND;
}



/* Create a new register
 * 
 * Parameters
 * - machine: DBM to act upon
 * - reg_id: Register identifier
 * - reg: Out parameter; will point to new register
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_create_register(DBM *machine, uint32_t reg_id, DBMRegister **reg) {
  DBMRegister *newRegister;
  machine->registers = realloc(machine->registers, (machine->nregisters + 1) * sizeof(DBMRegister));
  if (NULL == machine->registers) return CHIDB_ENOMEM;  
  newRegister     = &machine->registers[machine->nregisters];
  newRegister->id = reg_id;
  *reg = newRegister;
  machine->nregisters++;
  return CHIDB_OK;
}



/* Find a specific register or create it if it doesn't exist
 * 
 * Parameters
 * - machine: DBM to act upon
 * - reg_id: Register identifier
 * - reg: Out parameter; will point to register
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_find_or_create_register(DBM *machine, uint32_t reg_id, DBMRegister **reg) {
  int rc;
  rc = chidb_DBM_find_register(machine, reg_id, reg);
  if (CHIDB_ENOTFOUND == rc) {
    rc = chidb_DBM_create_register(machine, reg_id, reg);
  }
  return rc;
}



/* Free any data stored in a register
 * 
 * Parameters
 * - reg: Register to free
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_DBM_free_register(DBMRegister *reg) {
  if (NULL != reg) {
    if (DBM_STRING_REGISTER_TYPE == reg->type && NULL != reg->fields.string.data) {
      free(reg->fields.string.data);
    }
    
    // Set the type to NULL to avoid freeing if called again
    reg->type = DBM_NULL_REGISTER_TYPE;
  }

  return CHIDB_OK;
}



/* Find a particular DBM cursor
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor_id: Cursor identifier
 * - cursor: Out parameter; will point to cursor
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find cursor
 */
int chidb_DBM_find_cursor(DBM *machine, uint32_t cursor_id, DBMCursor **cursor) {
  for (uint32_t i = 0; i < machine->ncursors; ++i) {
    if (cursor_id == machine->cursors[i].id) {
      *cursor = &machine->cursors[i];
      return CHIDB_OK;
    }
  }

  return CHIDB_ENOTFOUND;
}



/* Create a new cursor
 * 
 * Parameters
 * - machine: DBM to act upon
 * - cursor_id: Cursor identifier
 * - cursor: Out parameter; will point to new cursor
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_create_cursor(DBM *machine, uint32_t cursor_id, DBMCursor **cursor) {
  DBMCursor *newCursor;
  machine->cursors = realloc(machine->cursors, (machine->ncursors + 1) * sizeof(DBMCursor));
  if (NULL == machine->cursors) return CHIDB_ENOMEM;  
  newCursor     = &machine->cursors[machine->ncursors];
  newCursor->id = cursor_id;
  *cursor = newCursor;
  machine->ncursors++;
  return CHIDB_OK;
}



/* Find a specific cursor or create it if it doesn't exist
 * 
 * Parameters
 * - machine: DBM to act upon
 * - cursor_id: Cursor identifier
 * - cursor: Out parameter; will point to cursor
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_find_or_create_cursor(DBM *machine, uint32_t cursor_id, DBMCursor **cursor) {
  int rc;
  rc = chidb_DBM_find_cursor(machine, cursor_id, cursor);
  if (CHIDB_ENOTFOUND == rc) {
    rc = chidb_DBM_create_cursor(machine, cursor_id, cursor);
  }
  return rc;
}



/* Find a particular B-Tree node in the DBM's cache
 *
 * Parameters
 * - machine: DBM to act upon
 * - page_num: Page number of node
 * - node: Out parameter; will point to B-Tree node
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find node
 */
int chidb_DBM_find_node(DBM *machine, npage_t page_num, BTreeNode **node) {
  for (uint32_t i = 0; i < machine->nnodes; ++i) {
    if (page_num == machine->nodes[i]->page->npage) {
      *node = machine->nodes[i];
      return CHIDB_OK;
    }
  }

  return CHIDB_ENOTFOUND;
}



/* Open a B-Tree
 * 
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to store B-Tree information within
 * - reg: Register with page number in integer field
 * - ncols: Number of columns in table
 * - mode: File access mode
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 */
int chidb_DBM_execute_Open(DBM *machine, DBMCursor *cursor, DBMRegister *reg, uint32_t ncols, uint8_t mode) {
  int rc;

  // Must have an integer register where we store the page number
  if (DBM_INTEGER_REGISTER_TYPE != reg->type) return CHIDB_EPAGENO;
  npage_t page  = reg->fields.integer;
  cursor->mode  = mode;
  cursor->ncols = ncols;

  BTreeNode *btn;
  rc = chidb_DBM_find_node(machine, page, &btn);
  if (CHIDB_OK != rc) return rc;

  BTreeCell btc;
  chidb_Btree_getCell(btn, 1, &btc);
  if (CHIDB_OK != rc) return rc;

  cursor->cell_id = 0;
  for (uint32_t i = 0; i < machine->ncells; ++i) {
    if (btc.key == machine->cells[i].entry.key && page == machine->cells[i].node->page->npage) {
      cursor->cell_id = i;
      break;
    }
  }

  return CHIDB_OK;
}

/* Open a B-Tree for reading */
int chidb_DBM_execute_OpenRead(DBM *machine, DBMCursor *cursor, DBMRegister *reg, uint32_t ncols) {
  return chidb_DBM_execute_Open(machine, cursor, reg, ncols, DBM_READONLY);
}

/* Open a B-Tree for reading and writing */
int chidb_DBM_execute_OpenWrite(DBM *machine, DBMCursor *cursor, DBMRegister *reg, uint32_t ncols) {
  return chidb_DBM_execute_Open(machine, cursor, reg, ncols, DBM_READWRITE);
}



/* Close a cursor
 * 
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to close
 * 
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_DBM_execute_Close(DBM *machine, DBMCursor *cursor) {
  size_t len = (machine->cursors + machine->ncursors) - (cursor + 1);
  if (len > 0) memmove(cursor, cursor + 1, len);
  machine->ncursors--;
  machine->cursors = realloc(machine->cursors, machine->ncursors * sizeof(DBMCursor));
  return CHIDB_OK;
}



/* Point a cursor to the first entry in the B-tree
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to rewind
 * - instruction_id: Instruction identifier for failure jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_execute_Rewind(DBM *machine, DBMCursor *cursor, uint32_t instruction_id) {
  if (machine->cells[cursor->cell_id].node->n_cells == 0) {
    // B-tree is empty, jump
    return chidb_DBM_jump(machine, instruction_id);
  }

  cursor->cell_id = machine->cells[cursor->cell_id].node_offset;
  return CHIDB_OK;
}



/* Advance a cursor to the next entry in the B-tree (if any)
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to advance
 * - instruction_id: Instruction identifier for jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_execute_Next(DBM *machine, DBMCursor *cursor, uint32_t instruction_id) {
  if (cursor->cell_id - machine->cells[cursor->cell_id].node_offset + 1 < machine->cells[cursor->cell_id].node->n_cells) {
    ++cursor->cell_id;
    return chidb_DBM_jump(machine, instruction_id);
  }

  return CHIDB_OK;
}



/* Move a cursor to the previous entry in the B-tree (if any)
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to move
 * - instruction_id: Instruction identifier for jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_execute_Prev(DBM *machine, DBMCursor *cursor, uint32_t instruction_id) {
  if (cursor->cell_id - machine->cells[cursor->cell_id].node_offset > 1 ) {
    --cursor->cell_id;
    return chidb_DBM_jump(machine, instruction_id);
  }

  return CHIDB_OK;
}



/* Move a cursor to the entry with a given key (if any)
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to move
 * - key: Key of B-Tree entry
 * - instruction_id: Instruction identifier for failure jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_execute_Seek(DBM *machine, DBMCursor *cursor, key_t key, uint32_t instruction_id) {
  for (uint32_t i = machine->cells[cursor->cell_id].node_offset; i < machine->cells[cursor->cell_id].node->n_cells; ++i) {
    if (key == machine->cells[i].entry.key) {
      cursor->cell_id = i;
      return CHIDB_OK;
    }
  }

  return chidb_DBM_jump(machine, instruction_id);
}



/* Move a cursor to the entry key greater than a given key (if any)
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to move
 * - key: Key of B-Tree entry
 * - instruction_id: Instruction identifier for failure jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_execute_SeekGt(DBM *machine, DBMCursor *cursor, key_t key, uint32_t instruction_id) {
  for (uint32_t i = machine->cells[cursor->cell_id].node_offset; i < machine->cells[cursor->cell_id].node->n_cells; ++i) {
    if (key < machine->cells[i].entry.key) {
      cursor->cell_id = i;
      return CHIDB_OK;
    }
  }

  return chidb_DBM_jump(machine, instruction_id);
}



/* Move a cursor to the entry key greater than a given key (if any)
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to move
 * - key: Key of B-Tree entry
 * - instruction_id: Instruction identifier for failure jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: Could not find instruction
 */
int chidb_DBM_execute_SeekGe(DBM *machine, DBMCursor *cursor, key_t key, uint32_t instruction_id) {
  for (uint32_t i = machine->cells[cursor->cell_id].node_offset; i < machine->cells[cursor->cell_id].node->n_cells; ++i) {
    if (key <= machine->cells[i].entry.key) {
      cursor->cell_id = i;
      return CHIDB_OK;
    }
  }

  return chidb_DBM_jump(machine, instruction_id);
}



/* Store a column's value into a register
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to inspect
 * - col_num: Column id to inspect
 * - reg: Register to modify
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_execute_Column(DBM *machine, DBMCursor *cursor, int32_t col_num, DBMRegister *reg) {
  if (NULL == machine->maps) return CHIDB_EIO;

  // If I'm getting this right, the B-Tree cell should be, in the case of table cells, table leaf cells
  BTreeCell btc = machine->cells[cursor->cell_id].entry;
  if (PGTYPE_TABLE_INTERNAL == btc.type) return CHIDB_EMISMATCH;

  if (col_num < 0 || col_num >= machine->maps[cursor->id].colMap.ncols) return CHIDB_EMISUSE;


  // First things first: Let's see where exactly in the db-record we are
  uint32_t text_length;                               // Will store length of any text data
  uint8_t data_offset   = *btc.fields.tableLeaf.data; // Offset for actual data (just header length for now)
  uint8_t header_offset = 1;                          // Offset in header (already read in header length)

  for (int32_t i = 0; i < col_num; ++i) {
    text_length = 0;
    switch (*(btc.fields.tableLeaf.data + header_offset)) {
      case SQL_NULL:
        ++header_offset;
        break;
      case SQL_INTEGER_1BYTE:
        ++header_offset;
        data_offset += 1;
        break;
      case SQL_INTEGER_2BYTE:
        ++header_offset;
        data_offset += 2;
        break;
      case SQL_INTEGER_4BYTE:
        ++header_offset;
        data_offset += 4;
        break;
      default:
        getVarint32(btc.fields.tableLeaf.data + header_offset, &text_length);
        data_offset += (text_length - 13) / 2;
        header_offset += 4;
        break;
    }
  }


  // Now we've gotta combine our db-record result type with the type given by the schema
  uint8_t data_type   = *(btc.fields.tableLeaf.data + header_offset);
  ColumnSchema schema = machine->maps[cursor->id].colMap.cols[col_num];

  // Primary key values aren't actually stored in the DB record, but rather in the B-tree cell itself
  if (machine->maps[cursor->id].colMap.primary_col >= 0 && machine->maps[cursor->id].colMap.primary_col == col_num) {
    reg->type           = DBM_INTEGER_REGISTER_TYPE;
    reg->fields.integer = btc.key;
    return CHIDB_OK;
  }

  // NULL value in database, just skip it
  if (SQL_NULL == data_type) {
    reg->type = DBM_NULL_REGISTER_TYPE;
    return CHIDB_OK;
  }

  // Value was returned, let's grab it
  switch (schema.type) {
    case SQL_INTEGER_1BYTE:
      reg->type        = DBM_BYTE_REGISTER_TYPE;
      reg->fields.byte = *((uint8_t *) btc.fields.tableLeaf.data + data_offset + 3 /* Stored in last of 4 bytes */);
      break;

    case SQL_INTEGER_2BYTE:
      reg->type            = DBM_SMALLINT_REGISTER_TYPE;
      reg->fields.smallint = get2byte(btc.fields.tableLeaf.data + data_offset);
      break;

    case SQL_INTEGER_4BYTE:
      reg->type           = DBM_INTEGER_REGISTER_TYPE;
      reg->fields.integer = get4byte(btc.fields.tableLeaf.data + data_offset);
      break;

    case SQL_TEXT:
      reg->type = DBM_STRING_REGISTER_TYPE;
      reg->fields.string.len  = btc.fields.tableLeaf.data_size;
      reg->fields.string.data = malloc(reg->fields.string.len * sizeof(uint8_t));
      if (NULL == memcpy((char *) reg->fields.string.data, (char *) btc.fields.tableLeaf.data + data_offset, reg->fields.string.len * sizeof(uint8_t))) return CHIDB_ENOMEM;
      break;
  }

  return CHIDB_OK;
}



/* Store a cursor's key into a register
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to inspect
 * - reg: Register to update
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_DBM_execute_Key(DBM *machine, DBMCursor cursor, DBMRegister *reg) {
  chidb_DBM_free_register(reg);
  reg->type = DBM_INTEGER_REGISTER_TYPE;
  reg->fields.integer = machine->cells[cursor.cell_id].entry.key;
  return CHIDB_OK;
}



/* Store an integer in a register
 *
 * Parameters
 * - machine: DBM to act upon
 * - integer: Integer to store
 * - reg: Register for storage
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_DBM_execute_Integer(DBM *machine, DBMRegister *reg, int32_t integer) {
  chidb_DBM_free_register(reg);
  reg->type = DBM_INTEGER_REGISTER_TYPE;
  reg->fields.integer = integer;
  return CHIDB_OK;
}



/* Store a string in a register
 *
 * Parameters
 * - machine: DBM to act upon
 * - len: Length of string data
 * - reg: Register for storage
 * - data: Pointer to start of datastore
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_DBM_execute_String(DBM *machine, DBMRegister *reg, void *data, size_t len) {
  chidb_DBM_free_register(reg);
  reg->type = DBM_STRING_REGISTER_TYPE;
  reg->fields.string.len  = len;
  reg->fields.string.data = malloc(len); // Freed with chidb_DBM_free_register
  if (NULL == memcpy((char *) reg->fields.string.data, (char *) data, len)) return CHIDB_ENOMEM;
  return CHIDB_OK;
}



/* Store a NULL value in a register
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg: Register for storage
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_DBM_execute_Null(DBM *machine, DBMRegister *reg) {
  chidb_DBM_free_register(reg);
  reg->type = DBM_NULL_REGISTER_TYPE;
  return CHIDB_OK;
}



/* Return result row (registers) to database user
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg_id: Identifier for first register
 * - ncols: Number of columns to return (total r+n-1)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_ENOTFOUND: Could not find register
 */
int chidb_DBM_execute_ResultRow(DBM *machine, uint32_t reg_id, int32_t ncols) {
  int rc;
  DBRecordBuffer buf;
  rc = chidb_DBRecord_create_empty(&buf, ncols);
  if (CHIDB_OK != rc) return rc;

  DBMRegister *reg;
  for (int32_t i = 0; i < ncols; ++i) {
    rc = chidb_DBM_find_register(machine, reg_id + i, &reg);
    if (CHIDB_OK != rc) return rc;

    switch (reg->type) {
      case DBM_NULL_REGISTER_TYPE:
        chidb_DBRecord_appendNull(&buf);
        break;
      case DBM_INTEGER_REGISTER_TYPE:
        chidb_DBRecord_appendInt32(&buf, reg->fields.integer);
        break;
      case DBM_SMALLINT_REGISTER_TYPE:
        chidb_DBRecord_appendInt16(&buf, reg->fields.smallint);
        break;
      case DBM_BYTE_REGISTER_TYPE:
        chidb_DBRecord_appendInt8(&buf, reg->fields.byte);
        break;
      case DBM_STRING_REGISTER_TYPE:
        chidb_DBRecord_appendString(&buf, (char *) reg->fields.string.data);
        break;
    }
  }

  if (machine->result != NULL) chidb_DBRecord_destroy(machine->result);
  rc = chidb_DBRecord_finalize(&buf, &machine->result);
  if (CHIDB_OK == rc) machine->returned = true;
  return rc;
}



/* Store a result row (registers) into another register
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg_id: Identifier for first register
 * - ncols: Number of columns to return (total r+n-1)
 * - result_reg: Register for storage
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_ENOTFOUND: Could not find register
 */
int chidb_DBM_execute_MakeRecord(DBM *machine, uint32_t reg_id, int32_t ncols, DBMRegister *result_reg) {
  int rc;
  DBRecordBuffer buf;
  rc = chidb_DBRecord_create_empty(&buf, ncols);
  if (CHIDB_OK != rc) return rc;

  DBMRegister *reg;
  for (int32_t i = 0; i < ncols; ++i) {
    rc = chidb_DBM_find_register(machine, reg_id + i, &reg);
    if (CHIDB_OK != rc) return rc;

    switch (reg->type) {
      case DBM_NULL_REGISTER_TYPE:
        chidb_DBRecord_appendNull(&buf);
        break;
      case DBM_INTEGER_REGISTER_TYPE:
        chidb_DBRecord_appendInt32(&buf, reg->fields.integer);
        break;
      case DBM_SMALLINT_REGISTER_TYPE:
        chidb_DBRecord_appendInt16(&buf, reg->fields.smallint);
        break;
      case DBM_BYTE_REGISTER_TYPE:
        chidb_DBRecord_appendInt8(&buf, reg->fields.byte);
        break;
      case DBM_STRING_REGISTER_TYPE:
        chidb_DBRecord_appendString(&buf, (char *) reg->fields.string.data);
        break;
    }
  }

  chidb_DBM_free_register(result_reg);
  result_reg->type               = DBM_STRING_REGISTER_TYPE;
  result_reg->fields.string.len  = buf.buf_size;
  DBRecord *result;
  rc = chidb_DBRecord_finalize(&buf, &result);
  if (CHIDB_OK != rc) return rc;
  rc = chidb_DBRecord_pack(result, &result_reg->fields.string.data);
  if (CHIDB_OK != rc) return rc;
  return CHIDB_OK;
}



/* Insert a DB record into the B-tree entry pointed by a cursor
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: Cursor to act upon
 * - key: Key of database record
 * - record: Database record to insert
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_execute_Insert(DBM *machine, DBMCursor *cursor, key_t key, DBRecord *record) {
  if (cursor->mode != DBM_READWRITE) return CHIDB_EMISUSE;
  int rc;

  // DEBUG
  printf("\n\t");
  chidb_DBRecord_print(record);

  // Result cell
  BTreeCell rcell;
  rcell.type = PGTYPE_TABLE_LEAF;
  rcell.key  = key;

  // Buffer for B-Tree cell data
  uint8_t *buffer = malloc(sizeof(uint8_t)); // Header byte
  if (NULL == buffer) return CHIDB_ENOMEM;

  uint8_t header_size = 1;
  uint8_t *data; // Current position within the buffer


  // Add our types to the header
  for (uint32_t i = 0; i < record->nfields; ++i) {
    data = buffer + header_size;
    int type = chidb_DBRecord_getType(record, i);
    uint32_t text_length = 0;

    // Primary keys aren't actually stored in the cell
    if (machine->maps[cursor->id].colMap.primary_col >= 0 && (uint32_t) machine->maps[cursor->id].colMap.primary_col == i) {
      buffer = realloc(buffer, header_size + 1);
      data   = buffer + header_size;
      *data  = (uint8_t) SQL_NULL;
      ++header_size;
      continue;
    }

    switch (type) {
      case SQL_NULL:
      case SQL_INTEGER_1BYTE:
      case SQL_INTEGER_2BYTE:
      case SQL_INTEGER_4BYTE:
        buffer = realloc(buffer, header_size + 1);
        data   = buffer + header_size;
        *data  = (uint8_t) type;
        ++header_size;
        break;
      case SQL_TEXT:
        chidb_DBRecord_getStringLength(record, i, (int *) &text_length);
        buffer = realloc(buffer, header_size + 4);
        data   = buffer + header_size;
        putVarint32(data, 2 * (text_length + 1) + 13);
        header_size += 4;
        break;
    }
  }

  *buffer = header_size; // Store header size in first byte


  // Insert actual field values
  data               = buffer + header_size;
  uint32_t data_size = 0;
  for (uint32_t i = 0; i < record->nfields; ++i) {
    // Primary keys aren't actually stored in the cell
    if (machine->maps[cursor->id].colMap.primary_col >= 0 && (uint32_t) machine->maps[cursor->id].colMap.primary_col == i) {
      continue;
    }

    data = buffer + header_size + data_size;
    int type = chidb_DBRecord_getType(record, i);
    uint32_t text_length = 0;
    int8_t value8;
    int16_t value16;
    int32_t value32;

    switch (type) {
      case SQL_INTEGER_1BYTE:
        buffer     = realloc(buffer, header_size + data_size + 1);
        data       = buffer + header_size + data_size;
        chidb_DBRecord_getInt8(record, i, &value8);
        bzero(data, 4);
        data      += 3;
        *data      = value8;
        data_size += 1;
        break;
      case SQL_INTEGER_2BYTE:
        buffer     = realloc(buffer, header_size + data_size + 2);
        data       = buffer + header_size + data_size;
        chidb_DBRecord_getInt16(record, i, &value16);
        put2byte(data, value16);
        data_size += 2;
        break;
      case SQL_INTEGER_4BYTE:
        buffer     = realloc(buffer, header_size + data_size + 4);
        data       = buffer + header_size + data_size;
        chidb_DBRecord_getInt32(record, i, &value32);
        put4byte(data, value32);
        data_size += 4;
        break;
      case SQL_TEXT:        
        chidb_DBRecord_getStringLength(record, i, (int *) &text_length);
        buffer = realloc(buffer, header_size + data_size + text_length + 1);
        data   = buffer + header_size + data_size;

        char *record_string;
        chidb_DBRecord_getString(record, i, (char **) &record_string);
        strncpy((char *) data, record_string, text_length);
        *(data + text_length) = 0;
        free(record_string);

        data_size += text_length + 1;
        break;
    }
  }

  uint32_t buffer_size = header_size + data_size;

  // Insert B-Tree cell
  rcell.fields.tableLeaf.data      = buffer;
  rcell.fields.tableLeaf.data_size = buffer_size;
  rc = chidb_Btree_insert(machine->db->bt, machine->root_page, &rcell);
  free(buffer);
  return rc;
}



/* Compare two registers for equality
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg1: First register
 * - reg2: Second register
 * - instruction_id: Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_Eq(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id) {
  if (reg1.type != reg2.type) return CHIDB_EMISMATCH;

  switch (reg1.type) {
    case DBM_STRING_REGISTER_TYPE:
      if (reg1.fields.string.len == reg2.fields.string.len) {
        if (0 == strncmp((char *) reg1.fields.string.data, (char *) reg2.fields.string.data, reg1.fields.string.len)) {
          chidb_DBM_jump(machine, instruction_id);
        }
      }
      break;
    case DBM_INTEGER_REGISTER_TYPE:
      if (reg1.fields.integer == reg2.fields.integer) {
        chidb_DBM_jump(machine, instruction_id);
      }
      break;
    case DBM_SMALLINT_REGISTER_TYPE:
      if (reg1.fields.smallint == reg2.fields.smallint) {
        chidb_DBM_jump(machine, instruction_id);
      }
      break;
    case DBM_BYTE_REGISTER_TYPE:
      if (reg1.fields.byte == reg2.fields.byte) {
        chidb_DBM_jump(machine, instruction_id);
      }
      break;
    case DBM_NULL_REGISTER_TYPE:
      chidb_DBM_jump(machine, instruction_id);
      break;
  }

  return CHIDB_OK;
}



/* Compare two registers for inequality
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg1: First register
 * - reg2: Second register
 * - instruction_id: Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_Ne(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id) {
  if (reg1.type != reg2.type) return CHIDB_EMISMATCH;

  switch (reg1.type) {
    case DBM_STRING_REGISTER_TYPE:
      if (reg1.fields.string.len == reg2.fields.string.len) {
        if (0 == strncmp((char *) reg1.fields.string.data, (char *) reg2.fields.string.data, reg1.fields.string.len)) {
          break;
        }
      }
      chidb_DBM_jump(machine, instruction_id);
      break;
    case DBM_INTEGER_REGISTER_TYPE:
      if (reg1.fields.integer != reg2.fields.integer) {
        chidb_DBM_jump(machine, instruction_id);
      }
      break;
    case DBM_SMALLINT_REGISTER_TYPE:
      if (reg1.fields.smallint != reg2.fields.smallint) {
        chidb_DBM_jump(machine, instruction_id);
      }
      break;
    case DBM_BYTE_REGISTER_TYPE:
      if (reg1.fields.byte != reg2.fields.byte) {
        chidb_DBM_jump(machine, instruction_id);
      }
      break;
  }

  return CHIDB_OK;
}



/* Compare two integer registers with "less than"
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg1: First register
 * - reg2: Second register
 * - instruction_id: Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_Lt(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id) {
  if (reg1.type != reg2.type) return CHIDB_EMISMATCH;

  if (DBM_BYTE_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.byte < reg2.fields.byte) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }
  
  if (DBM_SMALLINT_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.smallint < reg2.fields.smallint) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  if (DBM_INTEGER_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.integer < reg2.fields.integer) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  return CHIDB_OK;
}



/* Compare two integer registers with "less than or equal"
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg1: First register
 * - reg2: Second register
 * - instruction_id: Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_Le(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id) {
  if (reg1.type != reg2.type) return CHIDB_EMISMATCH;

  if (DBM_BYTE_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.byte <= reg2.fields.byte) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  if (DBM_SMALLINT_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.smallint <= reg2.fields.smallint) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }
  
  if (DBM_INTEGER_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.integer <= reg2.fields.integer) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }
  
  return CHIDB_OK;
}



/* Compare two integer registers with "greater than"
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg1: First register
 * - reg2: Second register
 * - instruction_id: Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_Gt(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id) {
  if (reg1.type != reg2.type) return CHIDB_EMISMATCH;

  if (DBM_BYTE_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.byte > reg2.fields.byte) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  if (DBM_SMALLINT_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.smallint > reg2.fields.smallint) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  if (DBM_INTEGER_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.integer > reg2.fields.integer) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  return CHIDB_OK;
}



/* Compare two integer registers with "greater than or equal"
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg1: First register
 * - reg2: Second register
 * - instruction_id: Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_Ge(DBM *machine, DBMRegister reg1, DBMRegister reg2, uint32_t instruction_id) {
  if (reg1.type != reg2.type) return CHIDB_EMISMATCH;

  if (DBM_BYTE_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.byte >= reg2.fields.byte) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  if (DBM_SMALLINT_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.smallint >= reg2.fields.smallint) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  if (DBM_INTEGER_REGISTER_TYPE == reg1.type) {
    if (reg1.fields.integer >= reg2.fields.integer) {
      chidb_DBM_jump(machine, instruction_id);
    }
  }

  return CHIDB_OK;
}



/* Compare key of cursor to key in register. 
 * If greater or equal, jump to instruction_id.
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg: Register - must contain a key
 * - cursor: cursor pointing to an index entry
 * - instruction_id:  Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_IdxGe(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id) {
  if (reg.type != DBM_INTEGER_REGISTER_TYPE) return CHIDB_EMISMATCH;
  DBMCell cell = machine->cells[cursor.cell_id];
  BTreeCell btc = cell.entry; 
  switch(btc.type){
	case PGTYPE_INDEX_INTERNAL:
	  if(reg.fields.integer < 0) return CHIDB_OK;
	  if(btc.fields.indexInternal.keyPk >= (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	case PGTYPE_INDEX_LEAF:
	  if(reg.fields.integer < 0) return CHIDB_OK;
	  if(btc.fields.indexLeaf.keyPk >= (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	default:
	  return CHIDB_EMISMATCH;
  }

  return CHIDB_OK;
}



/* Compare key of cursor to key in register. 
 * If greater, jump to instruction_id.
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg: Register - must contain a key
 * - cursor: cursor pointing to an index entry
 * - instruction_id:  Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_IdxGt(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id) {
  if (reg.type != DBM_INTEGER_REGISTER_TYPE) return CHIDB_EMISMATCH;
  DBMCell cell = machine->cells[cursor.cell_id];
  BTreeCell btc = cell.entry; 
  switch(btc.type){
	case PGTYPE_INDEX_INTERNAL:
	  if(reg.fields.integer < 0) return CHIDB_OK;
	  if(btc.fields.indexInternal.keyPk > (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	case PGTYPE_INDEX_LEAF:
	  if(reg.fields.integer < 0) return CHIDB_OK;
	  if(btc.fields.indexLeaf.keyPk > (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	default:
	  return CHIDB_EMISMATCH;
  }

  return CHIDB_OK;
}



/* Compare key of cursor to key in register. 
 * If less, jump to instruction_id.
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg: Register - must contain a key
 * - cursor: cursor pointing to an index entry
 * - instruction_id:  Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Registers have different types
 */
int chidb_DBM_execute_IdxLt(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id) {
  if (reg.type != DBM_INTEGER_REGISTER_TYPE) return CHIDB_EMISMATCH;
  DBMCell cell = machine->cells[cursor.cell_id];
  BTreeCell btc = cell.entry; 
  switch(btc.type){
	case PGTYPE_INDEX_INTERNAL:
	  if(reg.fields.integer < 0) chidb_DBM_jump(machine,instruction_id);
	  if(btc.fields.indexInternal.keyPk < (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	case PGTYPE_INDEX_LEAF:
	  if(reg.fields.integer < 0) chidb_DBM_jump(machine,instruction_id);
	  if(btc.fields.indexLeaf.keyPk < (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	default:
	  return CHIDB_EMISMATCH;
  }

  return CHIDB_OK;
}



/* Compare key of cursor to key in register. 
 * If less or equal, jump to instruction_id.
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg: Register - must contain a key
 * - cursor: cursor pointing to an index entry
 * - instruction_id:  Instruction identifier for success jump
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Cursor points to wrong type or register is 
 *   holding wrong type
 */
int chidb_DBM_execute_IdxLe(DBM *machine, DBMRegister reg, DBMCursor cursor, uint32_t instruction_id) {
  if (reg.type != DBM_INTEGER_REGISTER_TYPE) return CHIDB_EMISMATCH;
  DBMCell cell = machine->cells[cursor.cell_id];
  BTreeCell btc = cell.entry; 
  switch(btc.type){
	case PGTYPE_INDEX_INTERNAL:
	  if(reg.fields.integer < 0) chidb_DBM_jump(machine,instruction_id);
	  if(btc.fields.indexInternal.keyPk <= (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	case PGTYPE_INDEX_LEAF:
	  if(reg.fields.integer < 0) chidb_DBM_jump(machine,instruction_id);
	  if(btc.fields.indexLeaf.keyPk <= (uint32_t)reg.fields.integer)
	    chidb_DBM_jump(machine, instruction_id);
	  break;

	default:
	  return CHIDB_EMISMATCH;
  }

  return CHIDB_OK;
}



/* Store PKey of index pointed to by cursor in register 
 *
 * Parameters
 * - machine: DBM to act upon
 * - cursor: cursor pointing to index entry
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EMISMATCH: Cursor points to wrong type
 */
int chidb_DBM_execute_IdxKey(DBM *machine, DBMCursor cursor, DBMRegister *reg) {
  chidb_DBM_free_register(reg);
  reg->type = DBM_INTEGER_REGISTER_TYPE;
  BTreeCell btc = machine->cells[cursor.cell_id].entry;
  switch(btc.type){
  	case PGTYPE_INDEX_INTERNAL:
	  reg->fields.integer = btc.fields.indexInternal.keyPk;
	  break;
	case PGTYPE_INDEX_LEAF:
	  reg->fields.integer = btc.fields.indexLeaf.keyPk;
	  break;
	default:
	  return CHIDB_EMISMATCH;
  }
  return CHIDB_OK;
}



/* Store a new index entry in the index Btree pointed to by cursor
 *
 * Parameters
 * - machine: DBM to act on
 * - reg1: register where idxKey is stored
 * - reg2: register where pKey is stored
 * - cursor: cursor pointing to btree to insert in
 */
int chidb_DBM_execute_IdxInsert(DBM *machine, DBMRegister reg1, DBMRegister reg2, DBMCursor cursor) {
  if(reg1.type != DBM_INTEGER_REGISTER_TYPE || reg2.type != DBM_INTEGER_REGISTER_TYPE) 
  return CHIDB_EMISMATCH;
  npage_t index_root = 2;
  chidb_Btree_insertInIndex(machine->db->bt,index_root,reg1.fields.integer,reg2.fields.integer);
  return CHIDB_OK;
}



/* Make a shallow copy of one register
 *
 * Parameters
 * - machine: DBM to act upon
 * - reg1: Register to copy
 * - reg2: Register for storage
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_DBM_execute_SCopy(DBM *machine, DBMRegister *reg1, DBMRegister *reg2) {
  reg2->type   = reg1->type;
  reg2->fields = reg1->fields;
  return CHIDB_OK;
}



/* Halt execution of the DBM
 *
 * Parameters
 * - machine: DBM to act upon
 * - err: Error code to return
 * - err_msg: If err > 0, then string description
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 */
int chidb_DBM_execute_Halt(DBM *machine, uint32_t err, const char *err_msg) {
  machine->err        = err;
  machine->halted     = true;

  if (NULL != err_msg) {
    size_t err_msg_len  = strlen(err_msg);
    machine->err_msg    = malloc(err_msg_len * sizeof(char));
    if (NULL == machine->err_msg) return CHIDB_ENOMEM;
    machine->err_msg = strncpy(machine->err_msg, err_msg, err_msg_len);
    if (NULL == machine->err_msg) return CHIDB_ENOMEM;
  }

  return CHIDB_OK;
}