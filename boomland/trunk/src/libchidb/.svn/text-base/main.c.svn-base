/*****************************************************************************
 *
 *                                 chidb
 *
 * This module provides the chidb API.
 *
 * For more details on what each function does, see the chidb Architecture
 * document, or the chidb.h header file.
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/


#include <stdlib.h>
#include <chidb.h>
#include <string.h>
#include "btree.h"
#include "gen.h"
#include "parser.h"
#include "schemaloader.h"
#include "dbm.h"
#include "record.h"
#include "pager.h"



int chidb_open(const char *file, chidb **db) {
  *db = malloc(sizeof(chidb));
  if (*db == NULL) return CHIDB_ENOMEM;

  int rc = chidb_Btree_open(file, *db, &(*db)->bt);
  if (rc == CHIDB_ECORRUPTHEADER) return CHIDB_ECORRUPT;

  (*db)->stats.ntables     = 0;
  (*db)->stats.table_names = NULL;
  (*db)->stats.table_sizes = NULL;


  return CHIDB_OK;
}


int chidb_close(chidb *db) {
  if (db == NULL) return CHIDB_EMISUSE;
  chidb_Btree_close(db->bt);
  free(db);
  db = NULL;
  return CHIDB_OK;
}


int chidb_prepare(chidb *db, const char *sql, chidb_stmt **stmt) {
  int rc;

  chidb_stmt *st = (chidb_stmt *) malloc(sizeof(chidb_stmt));
  if (sql == NULL) return CHIDB_EINVALIDSQL;

  //create dbm
  chidb_DBM_create(db, &st->dbm);
  if (st->dbm == NULL) return CHIDB_ENOMEM;

  //create sql stmt
  chidb_parser(sql, &st->sql);
  if (st->sql == NULL) return CHIDB_ENOMEM;

  //set the schema
  rc = chidb_loadSchema(db, &st->schema);
  if (CHIDB_OK != rc) {
    free(st);
    return rc;
  }

  //generate the code
  rc = chidb_Gen(st->sql, st->dbm, st->schema);
  if (CHIDB_OK != rc) {
    free(st);
    return rc;
  }

  // Look up or start keeping stats on this table
  if (st->sql->type == STMT_SELECT) {
    for (int i = 0; i < st->sql->query.select.from_ntables; ++i) {
      bool found_table = false;
      char *table_name = st->sql->query.select.from_tables[i];

      for (uint32_t j = 0; j < db->stats.ntables; ++j) {
        if (0 == strcmp(table_name, db->stats.table_names[j])) {
          found_table = true;
        }
      }

      if (found_table) continue;
      db->stats.table_names = malloc(sizeof(db->stats.table_names) + sizeof(char *));
      db->stats.table_sizes = malloc(sizeof(db->stats.table_sizes) + sizeof(uint32_t));
      db->stats.table_names[db->stats.ntables] = table_name;
      db->stats.table_sizes[db->stats.ntables] = 0;
      ++db->stats.ntables;
    }
  }

  //set the stmt characteristics to hold these elements
  st->type = st->sql->type;
  *stmt    = st;

  return CHIDB_OK;
}


int chidb_step(chidb_stmt *stmt) {  
  if (stmt == NULL) return CHIDB_EMISUSE;
  return chidb_DBM_step(stmt->dbm);
}


int chidb_finalize(chidb_stmt *stmt) {
  int rc;
  if (stmt == NULL) return CHIDB_EMISUSE;

  rc = chidb_DBM_destroy(stmt->dbm);
  if (CHIDB_OK != rc) return rc;

  free(stmt->sql);

  chidb_destroySchema(stmt->schema);

  free(stmt);
  stmt = NULL;
  return CHIDB_OK;
}


int chidb_column_count(chidb_stmt *stmt) {
  int8_t ncols = 0;
  if (stmt->type == STMT_SELECT) {
    ncols = stmt->sql->query.select.select_ncols;
    if (ncols < 0) { // SELECT * FROM ...
      ncols = 0;
      for (uint32_t i = 0; i < stmt->dbm->nmaps; ++i) {
        ncols += stmt->dbm->maps[i].colMap.ncols;
      }
    }
  }
  return ncols;
}


int chidb_column_type(chidb_stmt *stmt, int col) {
  if (stmt->sql->type == STMT_SELECT) {
	  int ty = stmt->dbm->result->types[col];
    if (ty > SQL_TEXT) return SQL_TEXT;
    return ty;
	}

  return SQL_NOTVALID;
}

const char *chidb_column_name(chidb_stmt* stmt, int col) {
  char *name = NULL;
  if (stmt->type == STMT_SELECT) {
    if (stmt->sql->query.select.select_ncols < 0) { // SELECT * FROM ...

      int ncols = 0;
      for (uint32_t i = 0; i < stmt->dbm->nmaps; ++i) {
        Schema_Table st = stmt->dbm->maps[i];

        if (col < ncols + st.colMap.ncols) {
          return st.colMap.cols[col].name;
        } else {
          ncols += st.colMap.ncols;
        }
      }

    } else {
      name = stmt->sql->query.select.select_cols[col].name;
    }
  } return name;
}


int chidb_column_int(chidb_stmt *stmt, int col) {
  int ty = stmt->dbm->result->types[col];
  
  if (ty == SQL_INTEGER_1BYTE) {
    int8_t val;
    chidb_DBRecord_getInt8(stmt->dbm->result, col, &val);
    return val;
  }

   if (ty == SQL_INTEGER_2BYTE) {
    int16_t val;
    chidb_DBRecord_getInt16(stmt->dbm->result, col, &val);
    return val;
  }

  if (ty == SQL_INTEGER_4BYTE) {
    int32_t val;
    chidb_DBRecord_getInt32(stmt->dbm->result, col, &val);
    return val;
  }

  // SQL_NULL...
  return 0;
}



const char *chidb_column_text(chidb_stmt *stmt, int col) {
  int ty = chidb_DBRecord_getType(stmt->dbm->result, col);
  
  if (ty == SQL_TEXT) {
    int length;
    chidb_DBRecord_getStringLength(stmt->dbm->result, col, &length);

    char *str;
    chidb_DBRecord_getString(stmt->dbm->result, col, &str);
    return str;
  }

  return NULL;
}
