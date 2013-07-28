#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "chidb.h"
#include "chidbInt.h"
#include "util.h"
#include "dbm.h"
#include "gen.h"
#include "sigma.h"

int chidb_Sigma_SelectStmt(SelectStatement *stmt, DBM *dbm, Schema *schema, Schema_Table **schema_tables, Column *cols,  uint32_t *reg, uint32_t *cur)
{
	int8_t nconds=stmt->where_nconds;
	Condition *conds = stmt->where_conds;
	int8_t ncols=stmt->select_ncols;
	int8_t ntables=stmt->from_ntables;

	uint32_t forcond=&reg;
	&reg+=2;
	uint32_t mark = &reg;

	uint32_t rewind_jump = (2*ntables) + (ncols+1) + (2*nconds) + dbm->ninstructions;
	InsertStatement *st = (InsertStatement *) malloc(sizeof(InsertStatement));
	uint32_t inc;
	for(inc=0; inc<ntables; inc++)
	{	
		int newtablesr[ntables];
		int newtablesc[ntables];
		int newncols[ntables];
		chidb_Gen_Rewind(dbm, inc, rewind_jump);
		
		for (int i=0; i<nconds;i++)
		{
			Schema_Table *schema_table=schema_tables[inc];

			int c= chidb_Gen_get_column_no(schema_table, conds[i].op1.table, conds[i].op1.name, ntables);
			&cur=chidb_Gen_get_table_no(schema_table, conds[i].op1.table, ntables);
			uint32_t op_jump = rewind_jump-cur;
			
			if (cond[i].op2Type ==OP2_COL)
			{
				chidb_Gen_column(dbm, cur, c, forcond);
				int c2=chidb_Gen_get_table_no(schema_table, conds[i].op2.col.table, conds[i].op2.col.name, ntables);
				int cur2= chidb_Gen_get_table_no(schema_table, conds[i].op2.col.table, ntables);
				chidb_Gen_Column(dbm, cur2, c2, forcond+1);
				chidb_Gen_cond_op_register(&conds[i], dbm, op_jump, forcond, forcond+1);
			}
			else
			{
				chidb_Gen_Column(dbm, cur, c, forcond);
				chidb_Gen_cond_op_register(&conds[i], dbm, op_jump, forcond, i);
			}
			int mreg=*reg;			
			for (int i=0; i<ncols; i++)
			{
				uint32_t c = chidb_Gen_get_column_no(Schema_Table, cols[i].table, cols[i].name, ntables);
				uint32_t cur = chidb_Gen_get_table_no(Schema_Table, cols[i].table, ntables);
				chidb_Gen_Column(dbm, (*cur)++, c, (*reg)++);
			}
		}

		chidb_Gen_CreateTable(dbm,(*reg)++);
		newtablesr[inc]=reg-1;
		chidb_Gen_OpenWrite(dbm, (*cur)++, reg-1, ncols);
		newtablesc[inc]=cur-1;
		chidb_Gen_Key(dbm, cur-1, (*reg)++);
		chidb_Gen_MakeRecord(dbm, mreg, ncols, (*reg)++);
		chidb_Gen_InsertEntry(dbm, cur-1,reg-1, reg-2); 
		
		chidb_Gen_Next(dbm, inc, dbm->ninstructions-ntables+inc); //CHECK
	}
	int sreg=*reg;
	for(int i = 0; i<ntables; i++)
	{
		chidb_Gen_Rewind(dbm, newtables[i], rewind_jump);
	}
	for(i=0;i<ntables;i++)
	{
		chidb_Gen_column(dbm, newtablesc[i],ncols ,(*reg)++);
	}
	chidb_Gen_ResultRow(dbm, sreg, reg-1); 
	
	for(int i =0; i<ntables; i++)
	{
		chidb_Gen_Next(dbm, inc, dbm->ninstructions-ntables+i);
	}
	

}



