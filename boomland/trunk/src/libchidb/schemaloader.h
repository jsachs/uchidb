#ifndef SCHEMALOADER_H_
#define SCHEMALOADER_H_

#include "parser.h"

/* FROM parser.h *****
	
struct ColumnSchema
{
	char *name;
	uint8_t type;
};

**********************/

typedef struct{
	int ncols;
	struct ColumnSchema *cols;
	int primary_col;
}Schema_ColumnMap;

typedef struct{
    char *name;
	npage_t rootPage;
	Schema_ColumnMap colMap;
}Schema_Table;

typedef struct{
	char *assocName;
	int rootPage;
}Schema_Index;

typedef struct Schema_Node {
   int id;
   char *name;
   bool isTable; //true = table, false = index
   union{
	Schema_Index index;
	Schema_Table table;
   }info;
   struct Schema_Node *right;
   struct Schema_Node *left;
} Schema_Node;

typedef struct{
	struct Schema_Node *table_root;
	struct Schema_Node *index_root;
}Schema;

int chidb_loadSchema(chidb *db, Schema **schema);
//npage_t chidb_lookupTablePage(Schema *schema,char *name);
//npage_t chidb_lookupIndexPage(Schema *schema,char *name);
Schema_Table *chidb_getTable(Schema *schema, const char *tableName);
Schema_ColumnMap *chidb_getColumnMap(Schema *schema, const char *tableName);
void chidb_printSchema(Schema *s);
void chidb_destroySchema(Schema *s);

#endif
