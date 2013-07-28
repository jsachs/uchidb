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

/* debug macro */

#define HERE fprintf(stderr,"HERE: currently at %s %i\n", __FUNCTION__, __LINE__);
#define HERE_MSG(m) fprintf(stderr,"HERE (%s): currently at %s %i\n", m, __FUNCTION__, __LINE__);

/* cell size helper */
size_t computeCellSize(BTreeCell *cell)
{
  if      (cell->type == PGTYPE_TABLE_INTERNAL) return 8;
  else if (cell->type == PGTYPE_INDEX_INTERNAL) return 16;
  else if (cell->type == PGTYPE_INDEX_LEAF)     return 12;
  else if (cell->type == PGTYPE_TABLE_LEAF)     return 8 + cell->fields.tableLeaf.data_size;
}

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
  int rc;
  struct BTree *btree = (struct BTree *)malloc(sizeof(struct BTree));
  Pager *pager = (Pager *)malloc(sizeof(Pager));
  
  if (!btree || !pager) return CHIDB_ENOMEM;

  if (CHIDB_EIO == chidb_Pager_open(&pager, filename)) return CHIDB_EIO;
  
  chidb_Pager_setPageSize(pager, DEFAULT_PAGE_SIZE);

  uint8_t header[100];  
  rc = chidb_Pager_readHeader(pager, header);
  
  if( rc == CHIDB_NOHEADER )
  {
  	 // initialize file header
    strncpy(header, "SQLite format 3\0", 16);
    put2byte(header+16, 1024);
    header[18] = 0x01;
    header[19] = 0x01;
    header[20] = 0x00;
    header[21] = 0x40;
    header[22] = 0x20;
    header[23] = 0x20;
    put4byte(header+48, 20000);

    int j;
    for(j=32; j<=39; j++) header[j] = 0x00;
    for(j=44; j<=46; j++) header[j] = 0x00;
    header[47] = 0x01;
    for(j=52; j<=58; j++) header[j] = 0x00;
    header[59] = 0x01;
    for(j=64; j<=67; j++) header[j] = 0x00;
    
    btree->db = db;
  	 btree->pager = pager;
  	 db->bt = btree;
  	 *bt = btree;
    
  	 // create empty table leaf node in page 1
  	 npage_t npage;
  	 chidb_Btree_newNode(*bt, &npage, PGTYPE_TABLE_LEAF);
  	 struct BTreeNode *btn = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
  	 chidb_Btree_getNodeByPage(*bt, 1, &btn);
  	 memcpy(btn->page->data, header, 100);
  	 chidb_Btree_writeNode(*bt, btn);
  	 chidb_Btree_freeMemNode(*bt, btn);
  }

  else 
  { 
    if (strncmp(header, "SQLite format 3\0", 16) != 0) return CHIDB_ECORRUPTHEADER;

    /* check page size */
    if (get2byte(header+16) != pager->page_size) return CHIDB_ECORRUPTHEADER;
  
    /* check page cache size */
    if (get4byte(header+48) != 20000) return CHIDB_ECORRUPTHEADER;

    if (header[18] != 0x01) return CHIDB_ECORRUPTHEADER;
    if (header[19] != 0x01) return CHIDB_ECORRUPTHEADER;
    if (header[20] != 0x00) return CHIDB_ECORRUPTHEADER;
    if (header[21] != 0x40) return CHIDB_ECORRUPTHEADER;
    if (header[22] != 0x20) return CHIDB_ECORRUPTHEADER;
    if (header[23] != 0x20) return CHIDB_ECORRUPTHEADER;

    int j;
    for(j=32; j<=39; j++)
      if (header[j] != 0x00) return CHIDB_ECORRUPTHEADER;
    for(j=44; j<=46; j++)
      if (header[j] != 0x00) return CHIDB_ECORRUPTHEADER;
    if (header[47] != 0x01) return CHIDB_ECORRUPTHEADER;
    for(j=52; j<=58; j++)
      if (header[j] != 0x00) return CHIDB_ECORRUPTHEADER;
 	 if (header[59] != 0x01) return CHIDB_ECORRUPTHEADER;
 	 for(j=64; j<=67; j++)
      if (header[j] != 0x00) return CHIDB_ECORRUPTHEADER;
	
  	 btree->db = db;
  	 btree->pager = pager;
  	 db->bt = btree;
  	 *bt = btree;
  }
  return CHIDB_OK;
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
  int rc;
  
  if((rc = chidb_Pager_close(bt->pager)) != CHIDB_OK)
  {
  	 return CHIDB_EIO;
  }
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
  if (npage > bt->pager->n_pages) return CHIDB_EPAGENO;  
  
  struct BTreeNode *new_btn = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
  MemPage *mpage = (MemPage *)malloc(sizeof(MemPage));
  
  if (!new_btn || !mpage) return CHIDB_ENOMEM;

  int rc = chidb_Pager_readPage(bt->pager, npage, &mpage);
  if (rc == CHIDB_EIO) return CHIDB_EIO;
  
  int page1 = ((npage == 1)? 100 : 0);  
  
  /* set up the BTreeNode */
  new_btn->page = mpage;
  new_btn->type = (mpage->data)[page1];
  new_btn->free_offset = get2byte(page1+(mpage->data)+PGHEADER_FREE_OFFSET);
  new_btn->n_cells = get2byte(page1+(mpage->data)+PGHEADER_NCELLS_OFFSET);
  new_btn->cells_offset = get2byte(page1+(mpage->data)+PGHEADER_CELL_OFFSET);
  
  if (new_btn->type == PGTYPE_TABLE_INTERNAL || new_btn->type == PGTYPE_INDEX_INTERNAL){
  	 new_btn->right_page = get4byte(page1+(mpage->data)+PGHEADER_RIGHTPG_OFFSET);
	 new_btn->celloffset_array = page1 + new_btn->page->data + INTPG_CELLSOFFSET_OFFSET;
  }
  else if (new_btn->type == PGTYPE_TABLE_LEAF || new_btn->type == PGTYPE_INDEX_LEAF){
  	 new_btn->celloffset_array = page1 + new_btn->page->data + LEAFPG_CELLSOFFSET_OFFSET;
  }
  
  *btn = new_btn;
	
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
	if (!btn) return CHIDB_OK;
	
	int rc = chidb_Pager_releaseMemPage(bt->pager, btn->page);
	
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
  //allocate new page
  chidb_Pager_allocatePage(bt->pager, npage);
	
  //initialize as a btree node
  int rc = chidb_Btree_initEmptyNode(bt, *npage, type);	
  return rc;
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
	//allocate space
	
	struct BTreeNode *new_btn = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
	if (!new_btn) return CHIDB_ENOMEM;
	
	//zero out, set page type
	int page1 = ((npage == 1)? 100 : 0);

	new_btn->type=type;
	new_btn->n_cells=0;
	new_btn->cells_offset = bt->pager->page_size;
	if (new_btn->type == PGTYPE_TABLE_INTERNAL || new_btn->type == PGTYPE_INDEX_INTERNAL){
		new_btn->right_page = 0;
		new_btn->free_offset = 12 + page1;
	}
	else {
		new_btn->free_offset = 8 + page1;
	}
	//write with pager, though there are serious memory issues here
	struct MemPage *new_page = (struct MemPage *)malloc(sizeof(struct MemPage));
	if (CHIDB_EIO == chidb_Pager_readPage(bt->pager, npage, &new_page)) return CHIDB_EIO;		
	new_btn->page = new_page;

	chidb_Btree_writeNode(bt, new_btn);
	chidb_Pager_releaseMemPage(bt->pager, new_page);
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
	int page1 = ((btn->page->npage == 1)? 100 : 0);
	
	btn->page->data[page1] = btn->type;
	put2byte(btn->page->data+PGHEADER_FREE_OFFSET+page1 ,btn->free_offset);
	put2byte(btn->page->data+PGHEADER_NCELLS_OFFSET+page1, btn->n_cells);
	put2byte(btn->page->data+PGHEADER_CELL_OFFSET+page1, btn->cells_offset);
	btn->page->data[7 + page1] = 0;

	if(btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL){
		put4byte(btn->page->data+PGHEADER_RIGHTPG_OFFSET+page1, btn->right_page);
	}
	
	if(CHIDB_EIO == chidb_Pager_writePage(bt->pager, btn->page)) return CHIDB_EIO;
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
	if (ncell > btn->n_cells) return CHIDB_ECELLNO;
	// find the offset
	uint16_t offset_num = get2byte(btn->celloffset_array + (2*ncell)); // ncell-1?
	uint8_t *offset = btn->page->data + offset_num;
	// read the cell
	cell->type = btn->type;
	if (cell->type == PGTYPE_INDEX_INTERNAL) {
         getVarint32(offset+8, &(cell->key));
	}
	else{
		getVarint32(offset+4, &(cell->key));
	}
	if (cell->type == PGTYPE_TABLE_INTERNAL){
		cell->fields.tableInternal.child_page = get4byte(offset);
	}
	if (cell->type == PGTYPE_INDEX_INTERNAL){
		cell->fields.indexInternal.child_page = get4byte(offset);
		cell->fields.indexInternal.keyPk=get4byte(offset+12);
	}
	if (cell->type == PGTYPE_INDEX_LEAF){
		cell->fields.indexLeaf.keyPk=get4byte(offset+8);
	}
	else if (cell->type == PGTYPE_TABLE_LEAF){
		getVarint32(offset, &(cell->fields.tableLeaf.data_size));
		cell->fields.tableLeaf.data = offset+8;
	}

	return CHIDB_OK;
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
  if (ncell > btn->n_cells) return CHIDB_ECELLNO;
	if (cell->type == PGTYPE_TABLE_INTERNAL)
	{
		put4byte(btn->page->data + btn->cells_offset - 8 , cell->fields.tableInternal.child_page);
		putVarint32(btn->page->data + btn->cells_offset - 4, cell->key);
		
		btn->cells_offset -= 8;
		btn->n_cells += 1;
		
		int i;
		for (i=2*(btn->n_cells-1); i > 2*ncell; i -= 2 ){
			put2byte(btn->celloffset_array + i, get2byte(btn->celloffset_array + i - 2));
		}
		put2byte(btn->celloffset_array + (2*ncell), btn->cells_offset);
		
		btn->free_offset += 2;
	}
	if (cell->type == PGTYPE_INDEX_INTERNAL)
	{
		put4byte(btn->page->data + btn->cells_offset - 16, cell->fields.indexInternal.child_page);
		put4byte(btn->page->data + btn->cells_offset - 8, cell->key);
		put4byte(btn->page->data + btn->cells_offset - 4, cell->fields.indexInternal.keyPk);
		*(btn->page->data + btn->cells_offset - 12) = 0x0B;
		*(btn->page->data + btn->cells_offset - 11) = 0x03;
		*(btn->page->data + btn->cells_offset - 10) = 0x04;
		*(btn->page->data + btn->cells_offset - 9) = 0x04;

		btn->cells_offset -= 16;
		btn->n_cells += 1;

		int i;
		for (i=2*(btn->n_cells-1); i>2*ncell; i-= 2){
			put2byte(btn->celloffset_array + i, get2byte(btn->celloffset_array + i - 2));
		}
		put2byte(btn->celloffset_array + (2*ncell), btn->cells_offset);
		btn->free_offset += 2;
	}

	 if (cell->type == PGTYPE_TABLE_LEAF)
	{
		uint32_t datasize = cell->fields.tableLeaf.data_size;
		
		putVarint32(btn->page->data + btn->cells_offset - 8 - datasize, datasize);
		putVarint32(btn->page->data + btn->cells_offset - 4 - datasize, cell->key);

		memcpy(btn->page->data + btn->cells_offset - datasize, cell->fields.tableLeaf.data, datasize);
		
		btn->cells_offset -= (8+datasize);
		btn->n_cells += 1;
		
		int i;
		for (i=2*(btn->n_cells-1); i > 2*ncell; i -=2 ){
			put2byte(btn->celloffset_array + i, get2byte(btn->celloffset_array + i - 2));
		}
		put2byte(btn->celloffset_array + (2*ncell), btn->cells_offset);
		btn->free_offset += 2;		
	}
	else if (cell->type == PGTYPE_INDEX_LEAF)
	{
		put4byte(btn->page->data + btn->cells_offset - 8, cell->key);
		put4byte(btn->page->data + btn->cells_offset - 4, cell->fields.indexLeaf.keyPk);
		*(btn->page->data + btn->cells_offset - 12) = 0x0B;
		*(btn->page->data + btn->cells_offset - 11) = 0x03;
		*(btn->page->data + btn->cells_offset - 10) = 0x04;
		*(btn->page->data + btn->cells_offset - 9) = 0x04;

		btn->cells_offset -= 12;
		btn->n_cells += 1;
		int i;
		for (i=2*(btn->n_cells-1); i > 2*ncell; i -= 2){
			put2byte(btn->celloffset_array + i, get2byte(btn->celloffset_array + i - 2));
		}
		put2byte(btn->celloffset_array+(2*ncell), btn->cells_offset);
		btn->free_offset+=2;
	}
	
	return CHIDB_OK;
}

/*
void show_all_keys_at_node(BTreeNode *node)
{
  //int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
  for (int i=0; i<node->n_cells; i++) {
    BTreeCell c;
    int rc = chidb_Btree_getCell(node,i,&c);
    fprintf(stderr,"KEY %i\n", c.key);
  }
}

void SHOW_ALL_KEYS(BTree *bt) 
{
  Pager *pgr = bt->pager;
  npage_t n = pgr->n_pages; 
  for (int i=1; i<=n; i++) {
    BTreeNode *node;
    int rc = chidb_Btree_getNodeByPage(bt,i,&node);
    fprintf(stderr,"SHOW_ALL_KEYS AT PAGE %i\n", i);
    show_all_keys_at_node(node);
    fprintf(stderr,"----\n");
  }
}
*/

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
					
    

	struct BTreeNode *btn = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));			
	struct BTreeCell *btc = (struct BTreeCell *)malloc(sizeof(struct BTreeCell));	
	chidb_Btree_getNodeByPage(bt,nroot, &btn);
 

	uint8_t n=0;


	if (btn->type==PGTYPE_TABLE_INTERNAL)
	{
		while(n<(btn->n_cells))
		  {
		    chidb_Btree_getCell(btn, n, btc);
			if(key<=(btc->key))
			{
				return chidb_Btree_find(bt, btc->fields.tableInternal.child_page, key, data, size);
			}
			if(key>(btc->key) && n+1==(btn->n_cells))
			{
				return chidb_Btree_find(bt, btn->right_page, key, data, size);
			}
			n++;
		}
	}

	if (btn->type==PGTYPE_TABLE_LEAF)
	{
		while(n<(btn->n_cells))
		{
			chidb_Btree_getCell(btn, n, btc);
			if (key==(btc->key))
		{
				*data=btc->fields.tableLeaf.data;
				*size=(btc->fields.tableLeaf.data_size);
				return CHIDB_OK;
			}
			if (key<(btc->key)) {
			  return CHIDB_ENOTFOUND;
			}
			if (key>(btc->key) && (n+1==(btn->n_cells)))
			  return CHIDB_ENOTFOUND;
			
		       	n++;
		}
	}
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
  BTreeCell *btc = (BTreeCell *)malloc(sizeof(BTreeCell));
  
  btc->type = PGTYPE_TABLE_LEAF;
  btc->key = key;
  btc->fields.tableLeaf.data_size = size;
  btc->fields.tableLeaf.data = data;
  
  chidb_Btree_insert(bt, nroot, btc);
  return CHIDB_OK;
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
	BTreeCell *btc = (BTreeCell *)malloc(sizeof(BTreeCell));
	
	btc->type = PGTYPE_INDEX_LEAF;
	btc->key = keyIdx;
	btc->fields.indexLeaf.keyPk = keyPk;
	
	chidb_Btree_insert(bt, nroot, btc);	
	return CHIDB_OK;
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
  struct BTreeNode *btn_root = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
  chidb_Btree_getNodeByPage(bt, nroot, &btn_root);
  
  npage_t npage_child2;
  if((btn_root->cells_offset - btn_root->free_offset) < (computeCellSize(btc) + 2)) // root is full
    {
      struct BTreeNode *new_root = (struct BTreeNode *)malloc(sizeof(struct BTreeNode)); //new root
      
      //create a new page
      struct MemPage *new_page = (struct MemPage *)malloc(sizeof(struct MemPage)); //assign to old root
      npage_t newpagenum;
      chidb_Pager_allocatePage(bt->pager, &newpagenum);
      //copy old root into it
      new_page->npage=newpagenum;

      new_page->data = (uint8_t *)malloc(bt->pager->page_size);
      memcpy(new_page->data, btn_root->page->data, bt->pager->page_size);
      
      btn_root->page = new_page;
      
      //point an empty root node to page 1, have it point to old root as child
      new_root->page = (struct MemPage *)malloc(sizeof(struct MemPage));
      chidb_Pager_readPage(bt->pager, nroot, &new_root->page);
      new_root->page->npage = nroot;
      memcpy(new_root->page->data, btn_root->page->data, 100);    
      
      new_root->right_page = newpagenum;
      //clear data in page 1
      new_root->free_offset = 112;
      new_root->n_cells=0;
      new_root->cells_offset=bt->pager->page_size;
      if (btn_root->type==PGTYPE_INDEX_INTERNAL || btn_root->type==PGTYPE_INDEX_LEAF)
	new_root->type=PGTYPE_INDEX_INTERNAL;
      else
	new_root->type=PGTYPE_TABLE_INTERNAL;
      new_root->celloffset_array = new_root->page->data + 112;
      memset(new_root->page->data + 112, '\0', bt->pager->page_size - 112);
      
      //remove header from new_page
      memset(btn_root->page->data, '\0', 100); 
      
      //switched these two for loops, are you sure you still need the second one?
      int i;
      for (i=0; i<(btn_root->free_offset - 100); i++){
	*(btn_root->page->data + i) =*(btn_root->page->data + i + 100); 	
      } 
      
      btn_root->free_offset -= 100;
      btn_root->celloffset_array -= 100;
      
      memset(btn_root->page->data + btn_root->free_offset, '\0', btn_root->cells_offset - btn_root->free_offset);
      
      chidb_Btree_writeNode(bt, btn_root);
      chidb_Btree_writeNode(bt, new_root);

      chidb_Btree_split(bt, 1, btn_root->page->npage, 0, &npage_child2);
      chidb_Btree_insertNonFull(bt, 1, btc);
    }
  else chidb_Btree_insertNonFull(bt, nroot, btc);
  return CHIDB_OK;
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
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc)
{
  struct BTreeNode *btn = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
  chidb_Btree_getNodeByPage(bt, npage, &btn);
  
  if ((btn->type == PGTYPE_TABLE_LEAF) || (btn->type == PGTYPE_INDEX_LEAF))
  {
    int ncell;
    struct BTreeCell *btc_temp = (struct BTreeCell *)malloc(sizeof(struct BTreeCell));
    for(ncell = 0; ncell < btn->n_cells; ncell++){
      chidb_Btree_getCell(btn, ncell, btc_temp);
      if (btc_temp->key > btc->key) break;
    }
    chidb_Btree_insertCell(btn, ncell, btc);
  }
  else if ((btn->type == PGTYPE_TABLE_INTERNAL) || (btn->type == PGTYPE_INDEX_INTERNAL))
  {
    int ncell;
    struct BTreeCell *btc_temp = (struct BTreeCell *)malloc(sizeof(struct BTreeCell));
    for(ncell = 0; ncell < btn->n_cells; ncell++){
      chidb_Btree_getCell(btn, ncell, btc_temp);
      if (btc_temp->key > btc->key) break;
    }
    // check to see if the child page is full
    // otherwise call recursively
    npage_t child_page;
    if (btc_temp->key > btc->key) child_page = btc_temp->fields.tableInternal.child_page;
    else child_page = btn->right_page;
    struct BTreeNode *btn_child = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
    chidb_Btree_getNodeByPage(bt, child_page, &btn_child);
    
    // compute cell size

    if (btn_child->cells_offset - btn_child->free_offset < (computeCellSize(btc) + 2)) { // child page is full
      npage_t child_page2;
      chidb_Btree_split(bt, btn->page->npage, child_page, ncell, &child_page2);
      chidb_Btree_getNodeByPage(bt, child_page, &btn_child);
      chidb_Btree_getCell(btn_child, 0, btc_temp);
      if (btc_temp->key > btc->key) chidb_Btree_insertNonFull(bt, child_page2, btc);
      else chidb_Btree_insertNonFull(bt, child_page, btc);
    }
    else chidb_Btree_insertNonFull(bt, child_page, btc);
    //chidb_Btree_freeMemNode(bt, btn_child);
  }
  chidb_Btree_writeNode(bt, btn);
  //chidb_Btree_freeMemNode(bt, btn); TODO THESE NEED TO GO BACK!

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
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, 
		      ncell_t parent_ncell, npage_t *npage_child2)
{
	// find the median cell in N
	struct BTreeNode *N = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
	chidb_Btree_getNodeByPage(bt, npage_child, &N);
	npage_t med = (N->n_cells / 2);
	
	// create a new B-Tree node M
	chidb_Btree_newNode(bt, npage_child2, N->type);
	struct BTreeNode *M = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
	chidb_Btree_getNodeByPage(bt, *npage_child2, &M);
	
	// move the cells before the median cell to M
	struct BTreeCell *cell = (struct BTreeCell *)malloc(sizeof(struct BTreeCell));	
	int i;
	//ONLY FOR TESTING->IGNORE NEXT LINE
	chidb_Btree_getCell(N, 0, cell);
	for (i=0;i<med;i++){
		chidb_Btree_getCell(N, i, cell);
		chidb_Btree_insertCell(M, i, cell);
	}
	chidb_Btree_getCell(N, med, cell);
	if ((cell->type == PGTYPE_TABLE_LEAF) || (cell->type == PGTYPE_INDEX_LEAF)) chidb_Btree_insertCell(M, med, cell);
	chidb_Btree_writeNode(bt, M);
	

	// defragment cell N
	int defrag_offset = (cell->type == PGTYPE_TABLE_LEAF)? med + 1: med;

	struct BTreeNode *N_prime = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
	chidb_Btree_getNodeByPage(bt, npage_child, &N_prime);
	//int page1 = (N_prime->page->npage == 1)? 100 : 0;
	N_prime->n_cells = 0;
	N_prime->cells_offset = bt->pager->page_size;
	if ((N_prime->type == PGTYPE_TABLE_INTERNAL) || (N_prime->type == PGTYPE_INDEX_INTERNAL)){
		N_prime->free_offset = 12;// + page1;
	}
	else N_prime->free_offset = 8;// + page1;
	
	for (i=N->cells_offset; i<bt->pager->page_size; i++){
		memset(N_prime->page->data, '\0', 1);
	}
	memset(N_prime->celloffset_array, '\0', 2*(N->n_cells));
	int j = 0;
	for (i=defrag_offset; i<N->n_cells; i++) {
		chidb_Btree_getCell(N, i, cell);
		chidb_Btree_insertCell(N_prime, j, cell);
		j++;
	}
	chidb_Btree_writeNode(bt, N_prime);
	
	// add a cell to the parent
	struct BTreeNode *btnParent = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
	chidb_Btree_getNodeByPage(bt, npage_parent, &btnParent);
	chidb_Btree_getCell(N, med, cell);
	cell->type = btnParent->type;
	if (cell->type == PGTYPE_TABLE_INTERNAL) cell->fields.tableInternal.child_page = M->page->npage;
	else cell->fields.indexInternal.child_page = M->page->npage;
	
	BTreeCell * cell2 = (BTreeCell *)malloc(sizeof(BTreeCell));
	if (parent_ncell >= btnParent->n_cells) btnParent->right_page = N_prime->page->npage;
	else{
		chidb_Btree_getCell(btnParent, parent_ncell, cell2);
		if(cell2->type == PGTYPE_TABLE_INTERNAL) cell2->fields.tableInternal.child_page = N_prime->page->npage;
		else cell->fields.indexInternal.child_page = N_prime->page->npage;
	}
	chidb_Btree_insertCell(btnParent, parent_ncell, cell);	
	chidb_Btree_writeNode(bt, btnParent);
	
	chidb_Btree_freeMemNode(bt, M);
	chidb_Btree_freeMemNode(bt, N_prime);
	chidb_Btree_freeMemNode(bt, N);
	chidb_Btree_freeMemNode(bt, btnParent);
	return CHIDB_OK;
}

