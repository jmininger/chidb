/*
 *  chidb - a didactic relational database management system
 *
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb
 * file" and "file of B-Trees" are essentially equivalent terms).
 *
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
 *
 */

/*
 *  Copyright (c) 2009-2015, The University of Chicago
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or withsend
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  - Neither the name of The University of Chicago nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software withsend specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY send OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <assert.h>
#include <chidb/log.h>
#include "chidbInt.h"
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"


#define FILE_HEADER_SIZE (100)
#define getByte(x)   ((x)[0])
#define putByte(p,v) ((p)[0] = (uint8_t)(v))
#define isInternal(type) (type == PGTYPE_TABLE_INTERNAL || type == PGTYPE_INDEX_INTERNAL)
#define isHeaderPage(npage) (npage == 1)
static FILE *log;

/* Pack a BTree file's header
 *
 * This function takes a buffer representing a page
 * and packs the first 100 bytes with the default values
 * that go into the header
 *
 * Parameters
 * - buff_p: Pointer to the start of the buffer that holds the file header in memory 
 * - page_size: The physical size of the page
 *
 * Return
 * - void
 */
static void chidb_Btree_packFileHeader(uint8_t* buff_p, uint16_t page_size)
{
	enum 
	{
		file_change_ctr = 0,
		schema_version = 0,
		page_cache_size = 20000,
		user_cookie = 0
	};

	char word[] = "SQLite format 3";
	int word_size = 16;
	for(int i = 0; i < word_size; i++)
	{
		*(buff_p + i) = word[i];
	}

	uint8_t* page_size_pos = buff_p + 16;
	put2byte(page_size_pos, page_size);

	uint8_t* fchange_ctr_pos = buff_p + 24;
	put4byte(fchange_ctr_pos, file_change_ctr);

	uint8_t* schema_vers_pos = buff_p + 40;
	put4byte(schema_vers_pos, schema_version);

	uint8_t* page_cache_sz_pos = buff_p + 48;
	put4byte(page_cache_sz_pos, page_cache_size);

	uint8_t* user_cookie_pos = buff_p + 60;
	put4byte(user_cookie_pos, user_cookie);

	put4byte(buff_p+32, 0);
	put4byte(buff_p+36, 0);
	put4byte(buff_p+52, 0);
	put4byte(buff_p+64, 0);
	put4byte(buff_p+44, 1);
	put4byte(buff_p+56, 1);
	memcpy(buff_p+0x12, "\x01\x01\x00\x40\x20\x20", 6);

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
 *       created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *       newly created BTree.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
    /* Your code goes here */
    // log = fopen("log.txt", "a");
    // if(log==NULL){return -1;}
    // fprintf(log, "Opening chidb\n");

	if(filename == NULL || db == NULL || bt == NULL)
	{
		return CHIDB_EMISUSE;
	}
	
	const int file_h_size = FILE_HEADER_SIZE;
    //Open the database file 
	Pager* pgr_p;
	int open_msg = chidb_Pager_open(&pgr_p, filename);
	if(open_msg == CHIDB_OK)
	{
        //Create a btree structure on the heap that will be realllocated when the file is closed
		BTree* bt_p = malloc(sizeof(BTree));
		if(bt_p == NULL)
		{
			return CHIDB_ENOMEM;
		}

        //Fill in default values for the new btree 
		bt_p -> pager = pgr_p;
		bt_p -> db = db;
		db -> bt = bt_p;
		(*bt) = bt_p;
		assert((*bt) == bt_p);

        //read the header and make one if the header doesn't exist
		uint8_t header_buff[file_h_size];
		if(chidb_Pager_readHeader(pgr_p, header_buff) 
			== CHIDB_NOHEADER)
		{
			chidb_Pager_setPageSize(pgr_p, DEFAULT_PAGE_SIZE);			
			npage_t init_page_num;
			chidb_Btree_newNode(bt_p, &init_page_num, PGTYPE_TABLE_LEAF);
			assert(init_page_num == 1);
            //READ IN THE PAGE NOW AND THEN WRITE THE HEADER TO IT

            MemPage *header_page_p;
            int read_msg;
            if((read_msg = chidb_Pager_readPage(bt_p -> pager, 1, &header_page_p))!=CHIDB_OK)
            {
                return read_msg;
            }

            //Packs header in buffer 
            uint8_t *page_buff = header_page_p -> data;
			chidb_Btree_packFileHeader(page_buff, pgr_p->page_size);

            int write_msg;
            if((write_msg = chidb_Pager_writePage(pgr_p, header_page_p)) != CHIDB_OK)
            {
            	return write_msg;
            }

            int free_msg;
            if((free_msg = chidb_Pager_releaseMemPage(pgr_p, header_page_p))!= CHIDB_OK)
            {
                return free_msg;
            }
        }
        else 
        {
        	//Read the page size from the header and set chidb_pager_set_page size
            uint16_t page_size = get2byte(header_buff + 16);
            chidb_Pager_setPageSize(pgr_p, page_size);
            
            //Check for headers that don't follow the template
            if (strcmp("SQLite format 3", (char*)header_buff) != 0 ||
            	get4byte(header_buff+32)!=0 || get4byte(header_buff+36)!=0 ||
            	get4byte(header_buff+52)!=0 || get4byte(header_buff+64)!=0 ||
            	get4byte(header_buff+44)!=1 || get4byte(header_buff+56)!=1 ||
            	getByte(header_buff+18)!=1 || getByte(header_buff+19)!=1   ||
            	getByte(header_buff+20)!=0 || getByte(header_buff+21)!=0x40||
            	getByte(header_buff+22)!=0x20 || getByte(header_buff+23)!=0x20 ||
            	get4byte(header_buff+48)!=20000)
            {
        		return CHIDB_ECORRUPTHEADER;
    		}
       	}
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
    /* Your code goes here */
    if(bt == NULL)
    	return CHIDB_EMISUSE;
	
	int close_msg;
	if((close_msg = chidb_Pager_close(bt -> pager)) == CHIDB_OK)
	{
		free(bt);
	}
    return close_msg;
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
 * - btn: Out parameter. Used to return a pointer to newly creater BTreeNode
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
    /* Your code goes here */
	if(bt==NULL || btn == NULL)
	{
		return CHIDB_EMISUSE;
	}

	//Read in a mempage
	MemPage* mem_page_p = NULL;
	int read_msg = chidb_Pager_readPage(bt->pager, npage, &mem_page_p);
	if(read_msg != CHIDB_OK)
	{
		return read_msg;
	}
	assert(mem_page_p != NULL);
	
	BTreeNode* btn_p = malloc(sizeof(BTreeNode));
	if(btn_p == NULL)
	{
		return CHIDB_ENOMEM;
	}
	//Pack the BTree Node with info from the read-in MemPage
    uint8_t *node_start = isHeaderPage(npage) ? ((mem_page_p -> data)+100) : (mem_page_p -> data);
	
    btn_p -> type = getByte(node_start);
	btn_p -> free_offset = get2byte(node_start + 1);
	btn_p -> n_cells = get2byte(node_start + 3);
	btn_p -> cells_offset = get2byte(node_start + 5);
	if(isInternal(btn_p -> type))
    {
        btn_p -> right_page = get4byte(node_start + 8);
        btn_p -> celloffset_array = (uint8_t*)(node_start + 12);
    }
    else
    {
        btn_p -> right_page = 0;
        btn_p -> celloffset_array = (uint8_t*)(node_start + 8);
    }
	btn_p -> page = mem_page_p;
	
	*btn = btn_p; 
	return read_msg;
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
    /* Your code goes here */
    int free_msg;
    if((free_msg = chidb_Pager_releaseMemPage(bt->pager, btn->page)) != CHIDB_OK)
    {
        return free_msg;
    }

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
 *          was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
    /* Your code goes here */
    int alloc_msg;
    if((alloc_msg = chidb_Pager_allocatePage(bt -> pager, npage)) != CHIDB_OK)
    {
        return alloc_msg;
    }

    int new_nd_msg;
    if((new_nd_msg = chidb_Btree_initEmptyNode(bt, *npage, type)) != CHIDB_OK)
    {
        return new_nd_msg;
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
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
    /* Your code goes here */
    if(bt == NULL)
    {
    	return CHIDB_EMISUSE;
    }
    bool isInternal = isInternal(type);
    bool isHeaderPage = (npage == 1);

    const uint16_t page_size = bt->pager->page_size;
    uint8_t page_buff[page_size];
    uint8_t* node_start = isHeaderPage ? page_buff+FILE_HEADER_SIZE : page_buff;

    uint8_t* type_p = node_start;
    uint8_t* free_off_p = node_start + 1;
    uint8_t* num_cells_p = node_start + 3;
    uint8_t* cell_off_p = node_start + 5;
    // if(isInternal)
    // {
    //     uint8_t* rt_page_p = node_start + 8;
    // }
    putByte(type_p, type);
    if(isInternal)
    {
        (isHeaderPage) ? put2byte(free_off_p, 112) : put2byte(free_off_p, 12);
    }
    else
    {
        (isHeaderPage) ? put2byte(free_off_p, 108) : put2byte(free_off_p, 8);
    }
    put2byte(num_cells_p, 0);
    put2byte(cell_off_p, bt->pager->page_size);
    putByte(node_start+7, 0);

    MemPage new_page;	//is it ok for mem_page and for the buffer below to be on the stack?
    new_page.npage = npage;
    new_page.data = page_buff;
    
    int write_msg;
    if((write_msg = chidb_Pager_writePage(bt->pager, &new_page))!=CHIDB_OK)
    {
        return write_msg;
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
    /* Your code goes here */
    if(bt == NULL || btn == NULL)
    {
    	return CHIDB_EMISUSE;
    }
    uint8_t *data = btn -> page -> data;
    uint8_t* node_start = NULL;
    if(isHeaderPage((btn -> page -> npage)))
    {
    	 node_start = data + FILE_HEADER_SIZE;
    }
    else 
    {
    	node_start = data;
    }

    uint8_t *free_off_p = node_start+1;
    uint8_t *n_cells_p = node_start+3;
    uint8_t *cell_off_p = node_start+5;
    uint8_t *rt_ptr_p = node_start+8;

    putByte(node_start, btn -> type);
   	put2byte(free_off_p, btn -> free_offset);
    put2byte(n_cells_p, btn -> n_cells);
    put2byte(cell_off_p, btn -> cells_offset);
    if(isInternal(btn->type))
    {
    	put4byte(rt_ptr_p, btn -> right_page);
    }
    
    return chidb_Pager_writePage(bt -> pager, btn -> page);
}

/* Read the contents of a cell
 *
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *  1. Find out the offset of the requested cell.
 *  2. Read the cell from the in-memory page, and parse its
 *     contents (refer to The chidb File Format document for
 *     the format of cells).
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
    /* Your code goes here */

    return CHIDB_OK;
}


/* Insert a new cell into a B-Tree node
 *
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *  1. Add the cell at the top of the cell area. This involves "translating"
 *     the BTreeCell into the chidb format (refer to The chidb File Format
 *     document for the format of cells).
 *  2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *  3. Modify the cell offset array so that all values in positions >= ncell
 *     are shifted one position forward in the array. Then, set the value of
 *     position ncell to be the offset of the newly added cell.
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
    /* Your code goes here */

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
 * - size: Out-parameter where the number of bytes of data must be stored
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key way found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t **data, uint16_t *size)
{
    /* Your code goes here */

    return CHIDB_OK;
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
 *          this entry in.
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
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t *data, uint16_t size)
{
    /* Your code goes here */

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
 *          this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, chidb_key_t keyIdx, chidb_key_t keyPk)
{
    /* Your code goes here */

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
 *          this cell in.
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
    /* Your code goes here */

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
 *          this cell in.
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
    /* Your code goes here */

    return CHIDB_OK;
}


/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *   cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *   internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *                 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, ncell_t parent_ncell, npage_t *npage_child2)
{
    /* Your code goes here */

    return CHIDB_OK;
}

