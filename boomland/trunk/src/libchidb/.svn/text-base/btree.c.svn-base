/*****************************************************************************
 * 
 *																 chidb 
 * 
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb 
 * file" and "file of B-Trees" are essentially equivalent terms). 
 * 
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
 *
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/ 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <chidbInt.h>
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"
/* Open a B-Tree file
 * 
 * This function opens a database file and verifies that the file
 * header is correct. If the file is empty (which will happen
 * if the pager is given a filename for a file that does not exist)
 * then this function will (1) initialize the file header using
 * the default page size and (2) create an empty table leaf node
 * in page 1.
 * 
 * Parameters
 * - filename: Database file (might not exist)
 * - db: A chidb struct. Its bt field must be set to the newly
 *			 created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *			 newly created BTree.
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
	int error;
	BTree *newTree = (BTree *) malloc(sizeof(BTree));

	if (newTree == NULL) {
		return CHIDB_ENOMEM;
	}

	error = chidb_Pager_open(&(newTree->pager), filename);
	if (error != CHIDB_OK) {
		free(newTree);
		return error;
	}

	uint8_t header[100];

	if (chidb_Pager_readHeader(newTree->pager, header) == CHIDB_NOHEADER) {
		/* no header, so we have to initialize the file */
		chidb_Pager_setPageSize(newTree->pager, DEFAULT_PAGE_SIZE);
		newTree->pager->n_pages = 1;

		MemPage *firstPage;

		error = chidb_Pager_readPage(newTree->pager, 1, &firstPage);
		if (error != CHIDB_OK) {
			chidb_Pager_releaseMemPage(newTree->pager, firstPage);
			chidb_Pager_close(newTree->pager);
			free(newTree);
			return error;
		}
		chidb_initialize_file_header(firstPage->data);
		chidb_Pager_writePage(newTree->pager, firstPage);
		chidb_Pager_releaseMemPage(newTree->pager, firstPage);

		chidb_Btree_initEmptyNode(newTree, 1, PGTYPE_TABLE_LEAF);
	} else {
		/* otherwise, validate the header */
		newTree->pager->page_size = get2byte(header + 0x10);
		chidb_Pager_getRealDBSize(newTree->pager, &(newTree->pager->n_pages));

		if (chidb_validate_file_header(header) == CHIDB_ECORRUPTHEADER) {
			chidb_Pager_close(newTree->pager);
			free(newTree);
			return CHIDB_ECORRUPTHEADER;
		}
	}

	*bt = newTree;	
	db->bt= newTree;
	return CHIDB_OK;
}

/* 
 * Initializes a chidb file header according to the format specified
 * in "The chidb File Format".
 */
void chidb_initialize_file_header(uint8_t *header) {
	strcpy(header, "SQLite format 3");

	/* page size */
	*(header + 0x10) = (uint16_t) DEFAULT_PAGE_SIZE;
	/* constants (unused by chidb) */
	*(header + 0x12) = 0x01; *(header + 0x13) = 0x01; *(header + 0x14) = 0x0;
	*(header + 0x15) = 0x40; *(header + 0x16) = 0x20; *(header + 0x17) = 0x20;
	memset(header + 0x18, 0, 20); /* file change counter, schema version */
	put4byte(header + 0x2C, 1);
	put4byte(header + 0x30, 20000); /* page cache size */
	put4byte(header + 0x34, 0);
	put4byte(header + 0x38, 1);
	memset(header + 0x3C, 0, 8); /* user cookie */
	
	return;
}

/*
 * Checks that a chidb file header matches the format specified
 * in "The chidb File Format".
 */
int chidb_validate_file_header(uint8_t *header) {
	int valid = 1;
	valid = !strncmp(header, "SQLite format 3", 15);
	
	valid &= (*(header + 0x12) == 0x01);
	valid &= (*(header + 0x13) == 0x01);
	valid &= (*(header + 0x14) == 0x0);
	valid &= (*(header + 0x15) == 0x40);
	valid &= (*(header + 0x16) == 0x20);
	valid &= (*(header + 0x17) == 0x20);
	valid &= (get4byte(header + 0x20) == 0x0);
	valid &= (get4byte(header + 0x24) == 0x0);
	valid &= (get4byte(header + 0x2C) == 0x1);
	valid &= (get4byte(header + 0x30) == 20000);
	valid &= (get4byte(header + 0x34) == 0x0);
	valid &= (get4byte(header + 0x38) == 0x1);
	valid &= (get4byte(header + 0x40) == 0x0);

	if (valid) {
		return CHIDB_OK;
	} else {
		return CHIDB_ECORRUPTHEADER;
	}
}

/* Close a B-Tree file
 * 
 * This function closes a database file, freeing any resource
 * used in memory, such as the pager.
 * 
 * Parameters
 * - bt: B-Tree file to close
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_close(BTree *bt)
{
	chidb_Pager_close(bt->pager);
	free(bt);

	return CHIDB_OK;
}


/* Loads a B-Tree node from disk
 * 
 * Reads a B-Tree node from a page in the disk. All the information regarding
 * the node is stored in a BTreeNode struct (see header file for more details
 * on this struct). *This is the only function that can allocate memory for
 * a BTreeNode struct*. Always use chidb_Btree_freeMemNode to free the memory
 * allocated for a BTreeNode (do not use free() directly on a BTreeNode variable)
 * Any changes made to a BTreeNode variable will not be effective in the database
 * until chidb_Btree_writeNode is called on that BTreeNode.
 * 
 * Parameters
 * - bt: B-Tree file
 * - npage: Page of node to load
 * - btn: Out parameter. Used to return a pointer to newly created BTreeNode
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
	MemPage *page; 
	int error = chidb_Pager_readPage(bt->pager, npage, &page);
	if (error != CHIDB_OK) {
		return error;
	}

	*btn = (BTreeNode *) malloc(sizeof(BTreeNode));
	
	uint8_t *data = page->data;
	if (npage == 1) data += 100; /* skip file header on the first page */

	(*btn)->page = page;
	(*btn)->type = *(data + PGHEADER_PGTYPE_OFFSET);
	(*btn)->free_offset = get2byte(data + PGHEADER_FREE_OFFSET);
	(*btn)->n_cells = get2byte(data + PGHEADER_NCELLS_OFFSET);
	(*btn)->cells_offset = get2byte(data + PGHEADER_CELL_OFFSET);
	
	if ((*btn)->type == PGTYPE_TABLE_INTERNAL || (*btn)->type == PGTYPE_INDEX_INTERNAL) {
		(*btn)->right_page = get4byte(data + PGHEADER_RIGHTPG_OFFSET);
		(*btn)->celloffset_array =  data + 12;
	} else {
		(*btn)->right_page = 0;
		(*btn)->celloffset_array = data + 8;
	}
								   
	return CHIDB_OK;
}


/* Frees the memory allocated to an in-memory B-Tree node
 * 
 * Frees the memory allocated to an in-memory B-Tree node, and 
 * the in-memory page returned by the pages (stored in the
 * "page" field of BTreeNode)
 * 
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to free
 * 
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn)
{
	chidb_Pager_releaseMemPage(bt->pager,btn->page);
	free(btn);

	return CHIDB_OK;
}


/* Create a new B-Tree node
 * 
 * Allocates a new page in the file and initializes it as a B-Tree node.
 * 
 * Parameters
 * - bt: B-Tree file
 * - npage: Out parameter. Returns the number of the page that
 *					was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *				 PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
	int error;
	chidb_Pager_allocatePage(bt->pager, npage);

	error = chidb_Btree_initEmptyNode(bt, *npage, type);
	if (error != CHIDB_OK) {
		bt->pager->n_pages -= 1;
		return error;
	}

	return CHIDB_OK;
}



/* Initialize a B-Tree node
 * 
 * Initializes a database page to contain an empty B-Tree node. The
 * database page is assumed to exist and to have been already allocated
 * by the pager.
 * 
 * Parameters
 * - bt: B-Tree file
 * - npage: Database page where the node will be created.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *				 PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
	int error;
	MemPage *emptyPage;

	error = chidb_Pager_readPage(bt->pager, npage, &emptyPage);
	if (error != CHIDB_OK) {
		return error;
	}

	uint8_t *data = emptyPage->data;
	if (npage == 1) data += 100; /* skip file header on first page */

	*(data + PGHEADER_PGTYPE_OFFSET) = type;
	if (ISLEAF(type)) {
		/* offsets counted from top of page, not beginning of page header */
		put2byte(data + PGHEADER_FREE_OFFSET, 8 + ((npage == 1) ? 100 : 0));
	} else {
		put2byte(data + PGHEADER_FREE_OFFSET, 12 + ((npage == 1) ? 100 : 0));
	}
	put4byte(data + PGHEADER_NCELLS_OFFSET, 0);
	put2byte(data + PGHEADER_CELL_OFFSET, bt->pager->page_size);

	error = chidb_Pager_writePage(bt->pager, emptyPage);
	chidb_Pager_releaseMemPage(bt->pager, emptyPage);

	if (error != CHIDB_OK) {
		return error;
	}

	return CHIDB_OK;
}



/* Write an in-memory B-Tree node to disk
 * 
 * Writes an in-memory B-Tree node to disk. To do this, we need to update
 * the in-memory page according to the chidb page format. Since the cell
 * offset array and the cells themselves are modified directly on the
 * page, the only thing to do is to store the values of "type",
 * "free_offset", "n_cells", "cells_offset" and "right_page" in the
 * in-memory page.
 * 
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to write to disk
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_writeNode(BTree *bt, BTreeNode *btn)
{
	int error;
	uint8_t *data = btn->page->data;

	if (btn->page->npage == 1) data += 100; /* skip file header on the first page */

	*(data + PGHEADER_PGTYPE_OFFSET) = btn->type;
	put2byte(data + PGHEADER_FREE_OFFSET, btn->free_offset);
	if (!ISLEAF(btn->type)) {
		put4byte(data + PGHEADER_RIGHTPG_OFFSET, btn->right_page);
	}
	put2byte(data + PGHEADER_NCELLS_OFFSET, btn->n_cells);
	put2byte(data+ PGHEADER_CELL_OFFSET, btn->cells_offset);
	
	error = chidb_Pager_writePage(bt->pager, btn->page);
	if (error != CHIDB_OK) {
		return error;
	}
	
	return CHIDB_OK;
}


/* Read the contents of a cell
 * 
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *	1. Find out the offset of the requested cell.
 *	2. Read the cell from the in-memory page, and parse its
 *		 contents (refer to The chidb File Format document for
 *		 the format of cells).
 *	
 * Parameters
 * - btn: BTreeNode where cell is contained
 * - ncell: Cell number
 * - cell: BTreeCell where contents must be stored.
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{		
	if(btn->n_cells <= ncell) 
		return CHIDB_ECELLNO;

	uint8_t *rawCell =  btn->page->data + get2byte(btn->celloffset_array + 2*ncell);	

	cell->type = btn->type;

	switch (btn->type) {
	case PGTYPE_TABLE_INTERNAL:
		getVarint32(rawCell + TABLEINTCELL_KEY_OFFSET, &(cell->key)); 
		(cell->fields).tableInternal.child_page = get4byte(rawCell + TABLEINTCELL_CHILD_OFFSET);
		break;
	case PGTYPE_TABLE_LEAF:
	    getVarint32(rawCell + TABLELEAFCELL_KEY_OFFSET, &(cell->key));
		getVarint32(rawCell + TABLELEAFCELL_SIZE_OFFSET, &((cell->fields).tableLeaf.data_size));
		(cell->fields).tableLeaf.data = rawCell + TABLELEAFCELL_DATA_OFFSET;
		break;
	case PGTYPE_INDEX_INTERNAL:
		cell->key = get4byte(rawCell + INDEXINTCELL_KEYIDX_OFFSET);
		(cell->fields).indexInternal.keyPk = get4byte(rawCell + INDEXINTCELL_KEYPK_OFFSET); 
		(cell->fields).indexInternal.child_page = get4byte(rawCell + INDEXINTCELL_CHILD_OFFSET); 
		break;
	case PGTYPE_INDEX_LEAF:
		cell->key = get4byte(rawCell + INDEXLEAFCELL_KEYIDX_OFFSET); 
		(cell->fields).indexLeaf.keyPk = get4byte(rawCell + INDEXLEAFCELL_KEYPK_OFFSET); 
		break;
	default:
		break;
	}
			
	return CHIDB_OK;
}


int chidb_Btree_cellSize(BTreeCell *cell) {
	int cellSize;

	/* we assume that cell and btn have the same type */
	switch(cell->type){	
	case PGTYPE_TABLE_INTERNAL:
		cellSize = TABLEINTCELL_SIZE;
		break;
	case PGTYPE_TABLE_LEAF:
		cellSize = TABLELEAFCELL_SIZE_WITHOUTDATA + (cell->fields).tableLeaf.data_size;
		break;
	case PGTYPE_INDEX_INTERNAL:
		cellSize = INDEXINTCELL_SIZE;
		break;
	case PGTYPE_INDEX_LEAF:
		cellSize = INDEXLEAFCELL_SIZE;
		break;
	default:
		break;

	}

	return cellSize;
}

/* Insert a new cell into a B-Tree node
 * 
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *	1. Add the cell at the top of the cell area. This involves "translating"
 *		 the BTreeCell into the chidb format (refer to The chidb File Format 
 *		 document for the format of cells).
 *	2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *	3. Modify the cell offset array so that all values in positions >= ncell
 *		 are shifted one position forward in the array. Then, set the value of
 *		 position ncell to be the offset of the newly added cell.
 *
 * This function assumes that there is enough space for this cell in this node.
 *	
 * Parameters
 * - btn: BTreeNode to insert cell in
 * - ncell: Cell number
 * - cell: BTreeCell to insert.
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
	if (ncell > btn->n_cells) 
		return CHIDB_ECELLNO;

	int cellSize = chidb_Btree_cellSize(cell);

	if ((btn->cells_offset - btn->free_offset) < (2 + cellSize)) {
		/* there's not enough space to add another cell */
		return CHIDB_ECELLNO;
	}

	uint8_t *rawCell = btn->page->data + btn->cells_offset - cellSize;
	uint32_t dataSize;

	switch (btn->type){
	case PGTYPE_TABLE_INTERNAL:
		putVarint32(rawCell + TABLEINTCELL_KEY_OFFSET, cell->key); 
		put4byte(rawCell + TABLEINTCELL_CHILD_OFFSET, (cell->fields).tableInternal.child_page);
		break;
	case PGTYPE_TABLE_LEAF:
		putVarint32(rawCell + TABLELEAFCELL_KEY_OFFSET,cell->key);
		dataSize = (cell->fields).tableLeaf.data_size;
		putVarint32(rawCell + TABLELEAFCELL_SIZE_OFFSET, dataSize);
		memcpy(rawCell + TABLELEAFCELL_DATA_OFFSET, (cell->fields).tableLeaf.data, dataSize);
		break;
	case PGTYPE_INDEX_INTERNAL:
		put4byte(rawCell + INDEXINTCELL_KEYIDX_OFFSET, 	cell->key);
		put4byte(rawCell + INDEXINTCELL_KEYPK_OFFSET, (cell->fields).indexInternal.keyPk); 
		put4byte(rawCell + INDEXINTCELL_CHILD_OFFSET, (cell->fields).indexInternal.child_page); 
		break;
	case PGTYPE_INDEX_LEAF:
		put4byte(rawCell + INDEXLEAFCELL_KEYIDX_OFFSET, cell->key); 
		put4byte(rawCell + INDEXLEAFCELL_KEYPK_OFFSET, (cell->fields).indexLeaf.keyPk); 
		break;
	default:
		break;
	}	

	memmove(btn->celloffset_array + 2*ncell + 2,
			btn->celloffset_array + 2*ncell,
			2*(btn->n_cells - ncell));

	put2byte(btn->celloffset_array + 2*ncell, (btn->cells_offset - cellSize));

	btn->free_offset += 2;
	btn->n_cells += 1;
	btn->cells_offset -= cellSize;

	return CHIDB_OK;
}


/* Find an entry in a table B-Tree
 * 
 * Finds the data associated for a given key in a table B-Tree
 * 
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want search in
 * - key: Entry key
 * - data: Out-parameter where a copy of the data must be stored
 * - size: Number of bytes of data
 * 
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key was found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, key_t key, 
		     uint8_t **data, uint16_t *size) {
	BTreeNode *btn;
	BTreeCell btc;
	int cellPos, ncells, error;
	uint32_t nextPage;

	error = chidb_Btree_getNodeByPage(bt, nroot, &btn);
	if (error != CHIDB_OK) return error;

	if (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_TABLE_LEAF) {
		while (btn->type == PGTYPE_TABLE_INTERNAL) {
			/* look through keys */
			ncells = btn->n_cells;
			for (cellPos = 0; cellPos < ncells; cellPos++) {
				chidb_Btree_getCell(btn, cellPos, &btc);
				if (key <= btc.key) {
					/* look at left child of this key */
					chidb_Btree_freeMemNode(bt, btn);
					error = chidb_Btree_getNodeByPage(bt, btc.fields.tableInternal.child_page, &btn);
					if (error != CHIDB_OK) return error;
					break;
				}
			}
			if (cellPos == ncells) {
				/* look at rightmost child */
				nextPage = btn->right_page;
				chidb_Btree_freeMemNode(bt, btn);
				error = chidb_Btree_getNodeByPage(bt, nextPage, &btn);
				if (error != CHIDB_OK) return error;
			}
		}

		/* search for key in this leaf */
		for (cellPos = 0; cellPos < btn->n_cells; cellPos++) {
			chidb_Btree_getCell(btn, cellPos, &btc);
			if (key == btc.key) {
				*data = (uint8_t *) malloc(btc.fields.tableLeaf.data_size);
				if (*data == NULL) {
					chidb_Btree_freeMemNode(bt, btn);
					return CHIDB_ENOMEM;
				}
				memcpy(*data, btc.fields.tableLeaf.data, btc.fields.tableLeaf.data_size);
				*size = btc.fields.tableLeaf.data_size;
				return CHIDB_OK;
			}
		}
	} else {
		while (true) {
			/* look through keys */
			ncells = btn->n_cells;
			for (cellPos = 0; cellPos < ncells; cellPos++) {
				chidb_Btree_getCell(btn, cellPos, &btc);
				if (key < btc.key) {
					/* look at left child of this key */
					chidb_Btree_freeMemNode(bt, btn);
					error = chidb_Btree_getNodeByPage(bt, btc.fields.indexInternal.child_page, &btn);
					if (error != CHIDB_OK) return error;
					break;
				} else if (key == btc.key) {
					*data = (uint8_t *) malloc(sizeof(key_t));
					if (*data == NULL) {
						chidb_Btree_freeMemNode(bt, btn);
						return CHIDB_ENOMEM;
					}
					if (ISLEAF(btc.type))
						memcpy(data, &(btc.fields.indexLeaf.keyPk), sizeof(key_t));
					else
						memcpy(data, &(btc.fields.indexInternal.keyPk), sizeof(key_t));
					*size = sizeof(key_t);
					return CHIDB_OK;
				}
			}
			if (cellPos == ncells) {
				if (ISLEAF(btc.type)) {
					/* can't find the key*/
					return CHIDB_ENOTFOUND;
				} else {
					/* look at rightmost child */
					nextPage = btn->right_page;
					chidb_Btree_freeMemNode(bt, btn);
					error = chidb_Btree_getNodeByPage(bt, nextPage, &btn);
					if (error != CHIDB_OK) return error;
				}
			}
		}
	}
	return CHIDB_ENOTFOUND;
}


	
/* Insert an entry into a table B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a key and data, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this entry in.
 * - key: Entry key
 * - data: Pointer to data we want to insert
 * - size: Number of bytes of data
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, key_t key, 
			      uint8_t *data, uint16_t size)
{
	BTreeCell btc;

	btc.type = PGTYPE_TABLE_LEAF;
	btc.key = key;
	btc.fields.tableLeaf.data = data;
	btc.fields.tableLeaf.data_size = size;

	return chidb_Btree_insert(bt, nroot, &btc);
}


/* Insert an entry into an index B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a KeyIdx and a KeyPk, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, key_t keyIdx, key_t keyPk)
{
	BTreeCell btc;

	btc.type = PGTYPE_INDEX_LEAF;
	btc.key = keyIdx;
	btc.fields.indexLeaf.keyPk = keyPk;

	return chidb_Btree_insert(bt, nroot, &btc);
}


/* Insert a BTreeCell into a B-Tree
 *
 * The chidb_Btree_insert and chidb_Btree_insertNonFull functions
 * are responsible for inserting new entries into a B-Tree, although
 * chidb_Btree_insertNonFull is the one that actually does the
 * insertion. chidb_Btree_insert, however, first checks if the root
 * has to be split (a splitting operation that is different from
 * splitting any other node). If so, chidb_Btree_split is called
 * before calling chidb_Btree_insertNonFull.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc)
{
	int error, newPageRight, newPageLeft, headerOffset;
	BTreeNode *root, *newNodeRight;
	error = chidb_Btree_getNodeByPage(bt, nroot, &root);
	if (error != CHIDB_OK) return error;

	int cellSize = chidb_Btree_cellSize(btc);
	
	/* if the root is full, split it */
	if ((root->cells_offset - root->free_offset) < (2 + cellSize)) {
		headerOffset = (nroot == 1) ? 100 : 0;
		/* first, make a new empty page and copy the root to it */
		chidb_Btree_newNode(bt, &newPageRight, root->type);
		chidb_Btree_getNodeByPage(bt, newPageRight, &newNodeRight);
		memcpy(newNodeRight->page->data,
			   root->page->data + headerOffset,
			   bt->pager->page_size - headerOffset);
		newNodeRight->type = root->type;
		newNodeRight->free_offset = root->free_offset - headerOffset;
		newNodeRight->n_cells = root->n_cells;
		newNodeRight->cells_offset = root->cells_offset;
		newNodeRight->right_page = root->right_page;
		memmove(newNodeRight->page->data + newNodeRight->cells_offset,
				newNodeRight->page->data + newNodeRight->cells_offset - headerOffset,
				bt->pager->page_size - newNodeRight->cells_offset);
		chidb_Btree_writeNode(bt, newNodeRight);
		
		/* empty the current root */
		if (root->type == PGTYPE_TABLE_LEAF) root->type = PGTYPE_TABLE_INTERNAL;
		if (root->type == PGTYPE_INDEX_LEAF) root->type = PGTYPE_INDEX_INTERNAL;

		root->free_offset = headerOffset + INTPG_CELLSOFFSET_OFFSET;
		root->n_cells = 0;
		root->cells_offset = bt->pager->page_size;
		root->right_page = newPageRight;
		memset(root->page->data + headerOffset, 0, bt->pager->page_size - headerOffset);
		chidb_Btree_writeNode(bt, root);

		/* split the new right child, which contains the old root data */
		chidb_Btree_split(bt, nroot, newPageRight, 0, &newPageLeft);
		
		chidb_Btree_getNodeByPage(bt, nroot, &root);
		chidb_Btree_getNodeByPage(bt, newPageRight, &newNodeRight);
		BTreeNode *newNodeLeft;
		chidb_Btree_getNodeByPage(bt, newPageLeft, &newNodeLeft);

		chidb_Btree_freeMemNode(bt, root);
		chidb_Btree_freeMemNode(bt, newNodeRight);
		chidb_Btree_freeMemNode(bt, newNodeLeft);
	}	
	error = chidb_Btree_insertNonFull(bt, nroot, btc);

	return error;
}

/* Insert a BTreeCell into a non-full B-Tree node
 *
 * chidb_Btree_insertNonFull inserts a BTreeCell into a node that is
 * assumed not to be full (i.e., does not require splitting). If the
 * node is a leaf node, the cell is directly added in the appropriate
 * position according to its key. If the node is an internal node, the
 * function will determine what child node it must insert it in, and
 * calls itself recursively on that child node. However, before doing so
 * it will check if the child node is full or not. If it is, then it will
 * have to be split first.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *					this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *newCell)
{
	int error, cellPos, cellSize;
	BTreeNode *btn, *childNode;
	BTreeCell btc;
	npage_t childPage, newChild;
	error = chidb_Btree_getNodeByPage(bt, npage, &btn);
	if (error != CHIDB_OK) return error;

	cellSize = chidb_Btree_cellSize(newCell);

	for (cellPos = 0; cellPos < btn->n_cells; cellPos++) {
		chidb_Btree_getCell(btn, cellPos, &btc);
		if (newCell->key < btc.key) {
			break;
		} else if (newCell->key == btc.key) {
			chidb_Btree_freeMemNode(bt, btn);
			return CHIDB_EDUPLICATE;
		}
	}

	/* if this is a leaf, put it here */
	if (ISLEAF(btn->type)) {
		error = chidb_Btree_insertCell(btn, cellPos, newCell);
		chidb_Btree_writeNode(bt, btn);
		chidb_Btree_freeMemNode(bt, btn);
		return error;
	} else {
		if (cellPos < btn->n_cells) {
			childPage = (btc.type == PGTYPE_INDEX_INTERNAL)
				? btc.fields.indexInternal.child_page
				: btc.fields.tableInternal.child_page;
		} else {
			childPage = btn->right_page;
		}
		chidb_Btree_getNodeByPage(bt, childPage, &childNode);

		if ((childNode->cells_offset - childNode->free_offset) < (2 + cellSize)) {
			/* if child is full, split it */
			chidb_Btree_split(bt, btn->page->npage, childPage, cellPos, &newChild);
			chidb_Btree_getNodeByPage(bt, btn->page->npage, &btn);
			chidb_Btree_getCell(btn, cellPos, &btc);
			if (newCell->key < btc.key) childPage = newChild;
		}

		return chidb_Btree_insertNonFull(bt, childPage, newCell);
	}

	return CHIDB_OK;
}


/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *	 cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *	 internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *								 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, 
		      ncell_t parent_ncell, npage_t *npage_child2)
{
	BTreeNode *parentNode, *childNode, *newChildNode;
	BTreeCell medianCell, cell;
	int cellPos, cellSize, medianIdx, medianKeyPk, moveIdx;
	uint16_t offset, largestOffset, newCellsOffset;
	npage_t medianChild;

	chidb_Btree_getNodeByPage(bt, npage_parent, &parentNode);
	chidb_Btree_getNodeByPage(bt, npage_child, &childNode);

	/* create new node */
	chidb_Btree_newNode(bt, npage_child2, childNode->type);
	chidb_Btree_getNodeByPage(bt, *npage_child2, &newChildNode);

	/* find the median cell and bump it up */
	medianIdx = (childNode->n_cells)/2;
	chidb_Btree_getCell(childNode, medianIdx, &medianCell);
	medianKeyPk = (ISLEAF(medianCell.type)) 
		? medianCell.fields.indexLeaf.keyPk
		: medianCell.fields.indexInternal.keyPk;
	

	if (parentNode->type == PGTYPE_INDEX_INTERNAL) {
		medianChild = medianCell.fields.indexInternal.child_page;

		medianCell.type = PGTYPE_INDEX_INTERNAL;
		medianCell.fields.indexInternal.child_page = *npage_child2;
		medianCell.fields.indexInternal.keyPk = medianKeyPk;
	} else {
		medianChild = medianCell.fields.tableInternal.child_page;

		medianCell.type = PGTYPE_TABLE_INTERNAL;
		medianCell.fields.tableInternal.child_page = *npage_child2;
	}

	chidb_Btree_insertCell(parentNode, parent_ncell, &medianCell);

	/* move cells before the median to the new node */
	for (cellPos = 0; cellPos < medianIdx; cellPos++) {
		chidb_Btree_getCell(childNode, cellPos, &cell);
		chidb_Btree_insertCell(newChildNode, cellPos, &cell);
	}
	if (parentNode->type == PGTYPE_TABLE_INTERNAL) {
		chidb_Btree_getCell(childNode, medianIdx, &cell);
		chidb_Btree_insertCell(newChildNode, medianIdx, &cell);
	}
	newChildNode->right_page = medianChild;

	/* shift the cell offset array up */
	childNode->n_cells = (childNode->n_cells - 1)/2;
	memmove(childNode->celloffset_array,
			childNode->celloffset_array + 2*(medianIdx + 1), 2*(medianIdx + 1));
	childNode->free_offset -= 2*(medianIdx + 1);

	/* defragment the remaining cells in original child */
	newCellsOffset = bt->pager->page_size;
	for (int i = 0; i < childNode->n_cells; i++) {
		largestOffset = 0;
		for (cellPos = 0; cellPos < childNode->n_cells; cellPos++) {
			chidb_Btree_getCell(childNode, cellPos, &cell);
			offset = get2byte(childNode->celloffset_array + 2*cellPos);
			if (offset > largestOffset && offset < newCellsOffset) {
				largestOffset = offset;
				moveIdx = cellPos;
			}
		}
		chidb_Btree_getCell(childNode, moveIdx, &cell);
		cellSize = chidb_Btree_cellSize(&cell);
		newCellsOffset -= cellSize;
		childNode->cells_offset = newCellsOffset;
		memmove(childNode->page->data + newCellsOffset, 
				childNode->page->data + largestOffset, cellSize);
		/* update cell offset array */
		put2byte(childNode->celloffset_array + 2*moveIdx, newCellsOffset);
	}

	chidb_Btree_writeNode(bt, parentNode);
	chidb_Btree_writeNode(bt, childNode);
	chidb_Btree_writeNode(bt, newChildNode);

	chidb_Btree_freeMemNode(bt, parentNode);
	chidb_Btree_freeMemNode(bt, childNode);
	chidb_Btree_freeMemNode(bt, newChildNode);

	return CHIDB_OK;
}

void SHOW_ALL_KEYS_AT_NODE(BTreeNode *node)
{                                                                               
	for (int i=0; i<node->n_cells; i++) {
		BTreeCell c;
		int rc = chidb_Btree_getCell(node,i,&c);
		fprintf(stdout,"KEY %i\n",c.key);
	}
}

void SHOW_ALL_KEYS(BTree *bt)
{
	Pager *pgr = bt->pager;
	npage_t n = pgr->n_pages;
	for (int i=1; i<=n; i++) {
		BTreeNode *node;
		int rc = chidb_Btree_getNodeByPage(bt,i,&node);
		fprintf(stdout,"SHOW_ALL_KEYS AT PAGE %i\n", i);
		SHOW_ALL_KEYS_AT_NODE(node);
		fprintf(stderr,"----\n");
	}
}
