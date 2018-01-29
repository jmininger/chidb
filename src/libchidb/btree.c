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

// REMEMBER TO analyze THIS MODULE WHEN DONE, SO THAT IF ASKED TO IMPLEMENT A BTREE IN AN INTERVIEW, 
// I CAN TAKE THE PARTS OF THE IMPLEMENTATION THAT I LIKE AND TAKE THE PARTS THAT I DONT (IS A PAGER INTERFACE
// THE BEST WAY TO GO ABOUT IT?) AND BE ABLE TO TALK ABOUT HOW I WOULD DESIGN MY OWN
//  


//TODO: Test each function individually using new chilog functions. Do it based on whichever function
// has the least dependencies. Test isNodeFull, and removeCell, first, then split, then insertnonfull

//OFFSETS include +100 if header page
//BIG NOTE: A cell should only remain in scope while the node it is from is still in scope. This is because its
//  data is just a pointer to the page in mem

#define FILE_HEADER_SIZE (100)
#define getByte(x)   ((x)[0])
#define putByte(p,v) ((p)[0] = (uint8_t)(v))
#define isInternal(type) (type == PGTYPE_TABLE_INTERNAL || type == PGTYPE_INDEX_INTERNAL)
#define isLeaf(type) (type == PGTYPE_TABLE_LEAF || type == PGTYPE_INDEX_LEAF)
#define isHeaderPage(npage) (npage == 1)
#define nodeIsEmpty(node_p) node_p->n_cells <= 0
static FILE *log;
static FILE *btreeLog;

void chidb_Btree_printNode(BTreeNode *btn, FILE *log)
{
//      MemPage *page;             /* In-memory page returned by the Pager */
//     uint8_t type;              /* Type of page  */
//     uint16_t free_offset;      /* Byte offset of free space in page */
//     ncell_t n_cells;           /* Number of cells */
//     uint16_t cells_offset;     /* Byte offset of start of cells in page */
//     npage_t right_page;        /* Right page (internal nodes only) */
//     uint8_t *celloffset_array;
    fprintf(log, "Page Number: %d, Type: %d\nNumber of Cells: %d, Free Offset:%d, CellsOffset: %d right_page: %d\n",btn->page->npage, btn->type, btn->n_cells, btn->free_offset, btn->cells_offset, btn->right_page);
    fprintf(log, "Node_start: %p, OffsetArrPtr: %p, Distance: %d\n",btn->page->data, btn->celloffset_array, btn->celloffset_array-btn->page->data);
    for(ncell_t i = 0; i<btn->n_cells; i++)
    {
        BTreeCell cell;
        chidb_Btree_getCell(btn, i, &cell);
        size_t cell_offset = get2byte((btn -> celloffset_array) + (i*2));
        uint32_t cell_size;
        switch(btn -> type)
        {
            case PGTYPE_TABLE_INTERNAL:
                cell_size = 8;
                break;
            case PGTYPE_TABLE_LEAF:
                //8 = bytes to hold (data size + key)
                cell_size = 8 + cell.fields.tableLeaf.data_size;
                break;
            case PGTYPE_INDEX_INTERNAL:
                cell_size = 16;
                break;
            case PGTYPE_INDEX_LEAF:
                cell_size = 12;
        }
        fprintf(log, "Offset:%d Cell Key:%d, Cell Type: %d Cell Size: %d\n", cell_offset, cell.key, cell.type, cell_size);
    }
    fprintf(log, "\n\n");
    fflush(log);
}



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
    log = fopen("log.txt", "a+");
	
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
    fclose(log);
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
    if(btn == NULL || cell == NULL)
    {
        return CHIDB_EMISUSE;
    }
    if(ncell > btn -> n_cells)
    {
        return CHIDB_ECELLNO;
    }

    uint8_t* node_start = btn -> page -> data; //DO I NEED TO CHECK FOR HEADER?
    uint8_t* offset_p = btn -> celloffset_array + (2 * ncell);
    uint8_t* cell_p = node_start + get2byte(offset_p);

    cell -> type = btn -> type;
    switch (btn -> type)
    {
        int rc;
        case PGTYPE_TABLE_INTERNAL:
            rc = getVarint32(cell_p + 4, &(cell -> key));
            cell -> fields.tableInternal.child_page = get4byte(cell_p);
            break;
        case PGTYPE_TABLE_LEAF:
            rc = getVarint32(cell_p + 4,  &(cell -> key));
            rc = getVarint32(cell_p, &(cell -> fields.tableLeaf.data_size));
            cell -> fields.tableLeaf.data = cell_p + 8;
            break;
        case PGTYPE_INDEX_INTERNAL:
            cell -> key = get4byte(cell_p + 8);
            cell -> fields.indexInternal.keyPk = get4byte(cell_p + 12);
            cell -> fields.indexInternal.child_page = get4byte(cell_p);
            break;
        case PGTYPE_INDEX_LEAF:
            cell -> key = get4byte(cell_p + 4);
            cell -> fields.indexLeaf.keyPk = get4byte(cell_p + 8);
            break;
        default:
            break;
    }

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
    //NOTE!!! ncell starts at 0, NOT AT 1
    /* Your code goes here */
    if(btn == NULL || cell == NULL)
    {
        return CHIDB_EMISUSE;
    }

    uint8_t *data_p = btn -> page -> data;
    
    size_t cell_size = 0;
    uint8_t *new_cell_p = NULL;
    switch(btn -> type)
    {
        case PGTYPE_TABLE_INTERNAL:
            cell_size = 8;
            if(!((btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)))){
                fprintf(log, "Size: %d, freeSpace: %d\n",cell_size,btn -> cells_offset - btn -> free_offset);
                fflush(log);
                assert((btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)));
            }

            //Make sure that the free space can hold both the cell and the cell_offset
            new_cell_p = data_p + (btn -> cells_offset - cell_size);
            put4byte(new_cell_p, cell -> fields.tableInternal.child_page);
            putVarint32(new_cell_p + 4, cell -> key);
            break;
        
        case PGTYPE_TABLE_LEAF:
            //8 = bytes to hold (data size + key)
            cell_size = 8 + cell -> fields.tableLeaf.data_size;
            if(!((btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)))){
            fprintf(log, "Size: %d, freeSpace: %d key: %d\n",cell_size,btn -> cells_offset - btn -> free_offset, cell->key);
            fflush(log);
            assert((btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)));

            }
            //Make sure that the free space can hold both the cell and the cell_offset
            new_cell_p = data_p + (btn -> cells_offset - cell_size);
            putVarint32(new_cell_p, cell -> fields.tableLeaf.data_size);
            putVarint32(new_cell_p+4, cell -> key);
            memmove(new_cell_p+8, cell -> fields.tableLeaf.data, cell -> fields.tableLeaf.data_size);
            break;
        
        case PGTYPE_INDEX_INTERNAL:
            cell_size = 16;
            if(!((btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)))){
                fprintf(log, "Size: %d, freeSpace: %d\n",cell_size,btn -> cells_offset - btn -> free_offset);
                fflush(log);
                assert((btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)));
            }
            //Make sure that the free space can hold both the cell and the cell_offset
            
            new_cell_p = data_p + (btn -> cells_offset - cell_size);
            put4byte(new_cell_p, cell -> fields.indexInternal.child_page);
            putByte(new_cell_p+4, 0x0B);
            putByte(new_cell_p+5, 0x03);
            putByte(new_cell_p+6, 0x04);
            putByte(new_cell_p+7, 0x04);
            put4byte(new_cell_p+8, cell -> key);
            put4byte(new_cell_p+12, cell -> fields.indexInternal.keyPk);
            break;

        case PGTYPE_INDEX_LEAF:
            cell_size = 12;
            if((!(btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)))){
                fprintf(log, "Size: %d, freeSpace: %d\n",cell_size,btn -> cells_offset - btn -> free_offset);
                fflush(log);
                assert((btn -> cells_offset - btn -> free_offset) >= (cell_size + sizeof(uint16_t)));

            }
            //Make sure that the free space can hold both the cell and the cell_offset
            new_cell_p = data_p + (btn -> cells_offset - cell_size);
            putByte(new_cell_p, 0x0B);
            putByte(new_cell_p+1, 0x03);
            putByte(new_cell_p+2, 0x04);
            putByte(new_cell_p+3, 0x04);
            put4byte(new_cell_p+4, cell -> key);
            put4byte(new_cell_p+8, cell -> fields.indexLeaf.keyPk);
            break;

        default: 
            break;
    }
    
    //assert(new_cell_p > data_p);
    uint16_t cell_offset = (new_cell_p - data_p);
    if(ncell >= btn -> n_cells)
    {
        uint8_t *cell_offset_p = data_p + (btn -> free_offset);
        put2byte(cell_offset_p, cell_offset);
        //if internal deal with right page ptr
    }
    else
    {
        uint8_t* insert_point = btn -> celloffset_array + (2 * ncell);
        memmove(insert_point + 2, insert_point,  2*(btn->n_cells - ncell));
        put2byte(insert_point, cell_offset);
    }
    put2byte(data_p + 1, get2byte(data_p + 1)+2);
    put2byte(data_p + 3, get2byte(data_p + 3)+1);
    put2byte(data_p + 5, cell_offset);
    btn -> free_offset = btn -> free_offset + 2;
    btn -> n_cells = btn -> n_cells + 1;
    btn -> cells_offset = cell_offset;

    return CHIDB_OK;
}

/* Recursively searches for the node in the tree containing the data
 *
 * Takes a page number and a key, and returns a pointer to the node containing
 *  the data being searched for by recursively calling itself 
 *
 * Parameters
 * - bt: B-Tree file
 * - subRoot: Page number of the node of the BTree to be searched
 * - key: Entry key
 * - ncell: Out-parameter where the cell number is held if an internalIndex page 
        contains the actual data being searced for. This occurs b/c indeces are stored
        in B-trees as opposed to B+ trees
 * - flag: Out-parameter where any error codes are placed. Meant for debugging purposes
 *
 * Return
 * - BTreeNode*: A pointer to the btree node in memory that contains the data
 *     Is NULL on error
 */
static BTreeNode* chidb_Btree_findDataPage(BTree *bt, npage_t subRoot, chidb_key_t key, ncell_t* ncell, int*flag)
{
    //Read in node from provided page
    BTreeNode *node_p = NULL;
    if(chidb_Btree_getNodeByPage(bt, subRoot, &node_p) != CHIDB_OK)
    {
        *flag = CHIDB_ENOMEM;
        return NULL;
    }
    
    uint8_t node_type = node_p -> type;
    
    //If the node is a leaf, return a ptr to the page
    if(isLeaf(node_type))
    {
        return node_p;
    }
    
    //Cycle through the keys that the internal node stores
    for(int i = 0; i < node_p -> n_cells; i++)
    {
        BTreeCell cell;
        int cell_err;
        if((cell_err = chidb_Btree_getCell(node_p, i, &cell))!=CHIDB_OK)
        {
            *flag = cell_err;
            return NULL;
        }
        
        //if the index internal page contains a record
        if(key == cell.key && node_type == PGTYPE_INDEX_INTERNAL)
        {
            *ncell = i;
            return node_p;
        }
        //if the internal page holds the next page
        else if(key <= cell.key)
        {
            npage_t child_page = (node_type == PGTYPE_TABLE_INTERNAL)?
                                cell.fields.tableInternal.child_page:
                                cell.fields.indexInternal.child_page;
            
            int free_err;
            if((free_err = chidb_Btree_freeMemNode(bt, node_p))!=CHIDB_OK)
            {
                *flag = free_err;
                return NULL;
            }            
            return chidb_Btree_findDataPage(bt, child_page, key, ncell, flag);
        }
    }
    //This point is reached when the key is greater than all of the keys stored
    //Hence we go to the page stored in the node's "rightpage" ptr
    
    npage_t child_page = node_p -> right_page;
    int free_err;
    if((free_err = chidb_Btree_freeMemNode(bt, node_p))!=CHIDB_OK)
    {
        *flag = free_err;
        return NULL;
    }
    return chidb_Btree_findDataPage(bt, child_page, key, ncell, flag);

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
    log= fopen("log.txt", "a");

    if(bt == NULL || data == NULL || size == NULL)
    {
        fprintf(log, "CHIDB_EMISUSE 812\n" );
        fflush(log);
        return CHIDB_EMISUSE;
    }

    //This value holds cell number in the case that we come across an indexInternal page
    // that holds a record for our key
    ncell_t ncell;
    int flag = CHIDB_OK;
    BTreeNode *node_p = chidb_Btree_findDataPage(bt, nroot, key, &ncell, &flag);
    if(node_p == NULL)
    {
        fprintf(log, "CHIDB_ENOMEM 824\n" );
        fflush(log);
        return CHIDB_ENOMEM;
    }
    else if(isInternal(node_p -> type))
    {
        BTreeCell cell;
        chidb_Btree_getCell(node_p, ncell, &cell);
        size_t data_size = sizeof(chidb_key_t);
        *data = malloc(data_size);
        if(*data == NULL)
        {
            fprintf(log, "CHIDB_ENOMEM 836\n" );
            fflush(log);
            return CHIDB_ENOMEM;
        }
        *size = (uint16_t)data_size;
        memmove(*data, &(cell.fields.indexInternal.keyPk), data_size);
        chidb_Btree_freeMemNode(bt, node_p);
        return CHIDB_OK;

    }
    else
    {
        assert(node_p->type == PGTYPE_TABLE_LEAF);
        
        for(int i = 0; i < node_p -> n_cells; i++)
        {
            BTreeCell cell;
            chidb_Btree_getCell(node_p, i, &cell);
            if(key == cell.key)
            {
                *size = cell.fields.tableLeaf.data_size;
                *data = malloc(*size);
                if(*data == NULL)
                {
                    fprintf(log, "CHIDB_ENOMEM 860\n");
                    fflush(log);
                    return CHIDB_ENOMEM;
                }
                memmove(*data, cell.fields.tableLeaf.data, *size);
                chidb_Btree_freeMemNode(bt, node_p);
                return CHIDB_OK;
            }
        }
        
        fprintf(log, "NOTFOUND %d\n", key);
        chidb_Btree_printNode(node_p, log);
        fclose(log);
        fflush(log);
        chidb_Btree_freeMemNode(bt, node_p);
        return CHIDB_ENOTFOUND;
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
    if(size > DEFAULT_PAGE_SIZE)
    {
        return CHIDB_ENOMEM;
    }
    BTreeCell cell;
    cell.type = PGTYPE_TABLE_LEAF;
    cell.key = key;
    cell.fields.tableLeaf.data_size = size;
    cell.fields.tableLeaf.data = data;

    int val = chidb_Btree_insert(bt, nroot, &cell);
    if(val != CHIDB_OK)
        fclose(log);
    return val;
    //return CHIDB_OK;
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
    BTreeCell cell;
    cell.type = PGTYPE_INDEX_LEAF;
    cell.key = keyIdx;
    cell.fields.indexLeaf.keyPk = keyPk;

    int val = chidb_Btree_insert(bt, nroot, &cell);
    if(val != CHIDB_OK)
        fclose(log);
    return val;
    return CHIDB_OK;
}

static bool chidb_Btree_isNodeFull(BTreeNode *node, BTreeCell *btc)
{
    bool isFull;

    assert(node -> cells_offset > node -> free_offset);
    size_t free_space = (node -> cells_offset) - (node -> free_offset);
    switch(node -> type)
    {
        case PGTYPE_TABLE_LEAF:
            isFull = (btc -> fields.tableLeaf.data_size + 8 + 2)  > free_space;
            break;
        case PGTYPE_TABLE_INTERNAL:
            isFull = (8 + 2)  > free_space;
            break;
        case PGTYPE_INDEX_LEAF:
            isFull = (12 + 2)  > free_space;
            break;
        case PGTYPE_INDEX_INTERNAL:
            isFull = (16 + 2)  > free_space;
            break;
        default:
            return false;
            break;
    }
    return isFull;
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
/*
 *     InitNewNode: takes a page num--reads in what is currently stored ON DISK, and then writes back the new
 *     page that it inits:
 *      
 */

    if(bt == NULL || btc == NULL)
    {
        return CHIDB_EMISUSE;
    }

    BTreeNode *root_p;
    int rd_msg = chidb_Btree_getNodeByPage(bt, nroot, &root_p);
    if(rd_msg !=CHIDB_OK)
    {
        return rd_msg;
    }

    if(chidb_Btree_isNodeFull(root_p, btc))
    {
        //This node is going to become the root
        uint8_t new_node_type = (root_p->type == PGTYPE_TABLE_INTERNAL || 
                                root_p->type == PGTYPE_TABLE_LEAF)? 
                                PGTYPE_TABLE_INTERNAL:PGTYPE_INDEX_INTERNAL;
        npage_t new_node_npage;
        int alloc_msg = chidb_Btree_newNode(bt, &new_node_npage, new_node_type);
        BTreeNode *new_node_p;
        int rd_msg = chidb_Btree_getNodeByPage(bt, new_node_npage, &new_node_p);
        if(rd_msg !=CHIDB_OK)
        {
            return rd_msg;
        }
        fprintf(log, "We are splitting the root size %d\n",root_p->cells_offset - root_p->free_offset);
        if(isHeaderPage(nroot))
        {   
            root_p -> page -> npage = new_node_npage;
            new_node_p -> page -> npage = nroot;
            
            //Switch the new node and root node pages. Init both of these pages
            //  and then read the values of the full_root (which has been cleared
            //  on disk but remains in memory) into the now empty child
            chidb_Btree_writeNode(bt, root_p);
            chidb_Btree_writeNode(bt, new_node_p);
            chidb_Btree_freeMemNode(bt, new_node_p);
            chidb_Btree_initEmptyNode(bt, nroot, new_node_type);
            chidb_Btree_initEmptyNode(bt, root_p -> page -> npage, root_p->type);

            BTreeNode *new_child_p;
            chidb_Btree_getNodeByPage(bt, new_node_npage, &new_child_p);
            for(ncell_t i = 0; i<root_p->n_cells; i++)
            {
                BTreeCell cell;
                chidb_Btree_getCell(root_p, i, &cell);
                chidb_Btree_insertCell(new_child_p, i, &cell);
            }
            
            new_child_p -> right_page = root_p -> right_page; 

            chidb_Btree_writeNode(bt, new_child_p);
            chidb_Btree_freeMemNode(bt, root_p);
            chidb_Btree_freeMemNode(bt, new_child_p);
        }
        else
        {
            root_p -> page -> npage = new_node_npage;
            new_node_p -> page -> npage = nroot;
            chidb_Btree_writeNode(bt, root_p);
            chidb_Btree_writeNode(bt, new_node_p);
            chidb_Btree_freeMemNode(bt, new_node_p);
            chidb_Btree_freeMemNode(bt, root_p);
        }
        //Note that because the page nums have been flipped, nroot is now the empty parent node,
        // and new_node_npage is the node that is full with data
        npage_t new_child_page;
        int split_msg = chidb_Btree_split(bt, nroot, new_node_npage, 0, &new_child_page);
        if(split_msg != CHIDB_OK)
        {
            return split_msg;
        }

        //Get the parent page(the root) and make sure that the right page points to the
        // old full node
        BTreeNode *new_root_p;
        rd_msg = chidb_Btree_getNodeByPage(bt, nroot, &new_root_p);
        if(rd_msg !=CHIDB_OK)
        {
            return rd_msg;
        }
        new_root_p -> right_page = new_node_npage;
        chidb_Btree_writeNode(bt, new_root_p);
        chidb_Btree_freeMemNode(bt, new_root_p);
        return chidb_Btree_insertNonFull(bt, nroot, btc);
    }

    else
    {
        chidb_Btree_freeMemNode(bt, root_p);
        return chidb_Btree_insertNonFull(bt, nroot, btc);
    }
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
    BTreeNode *node_p;
    int rd_msg = chidb_Btree_getNodeByPage(bt, npage, &node_p);
    if(rd_msg != CHIDB_OK)
    {
        return rd_msg;
    }
    
    int node_type = node_p -> type;
    if(isLeaf(node_type))
    {
        ncell_t insert_point = 0;
        //If node has any cells--otherwise, the insert_point stays at 0
        if(!(nodeIsEmpty(node_p)))
        {
            BTreeCell cell;
            int cell_msg = chidb_Btree_getCell(node_p,(node_p -> n_cells - 1),&cell);
            if(cell_msg != CHIDB_OK)
            {
                return cell_msg;
            }
            
            if(btc->key > cell.key)
            {
                insert_point = node_p -> n_cells;
            }
            else
            {
                for(int i = 0; i < node_p -> n_cells; i++)
                {
                    BTreeCell icell;
                    cell_msg = chidb_Btree_getCell(node_p, i ,&icell);
                    if(cell_msg != CHIDB_OK)
                    {
                        return cell_msg;
                    }
                    if(btc->key == icell.key)
                    {
                        chidb_Btree_freeMemNode(bt, node_p);
                        return CHIDB_EDUPLICATE;
                    }
                    else if(btc->key < icell.key)
                    {
                        insert_point = i;
                        break;
                    }
                }
            }
        }
        chidb_Btree_insertCell(node_p, insert_point, btc);
        chidb_Btree_writeNode(bt, node_p);
        chidb_Btree_freeMemNode(bt, node_p);
        return CHIDB_OK;
    }
    else if(isInternal(node_type))
    {
        npage_t child_page = 0;
        ncell_t insert_point = 0;
        
        //Check to see if its greater than the last cell
        BTreeCell cell; 
        int cell_msg = chidb_Btree_getCell(node_p,(node_p -> n_cells - 1),&cell);
        if(cell_msg != CHIDB_OK)
        {
            return cell_msg;
        }
        if(btc->key > cell.key)
        {
            insert_point = node_p -> n_cells;
            child_page = node_p -> right_page;
        }
        else
        {
            for(int i = 0; i < node_p -> n_cells; i++)
            {
                BTreeCell icell;
                cell_msg = chidb_Btree_getCell(node_p, i ,&icell);
                if(cell_msg != CHIDB_OK)
                {
                    return cell_msg;
                }
                if(btc->key == icell.key)
                {
                    chidb_Btree_freeMemNode(bt, node_p);
                    return CHIDB_EDUPLICATE;
                }
                else if(btc->key < icell.key)
                {
                    child_page = (node_type == PGTYPE_TABLE_INTERNAL)?
                            icell.fields.tableInternal.child_page:
                            icell.fields.indexInternal.child_page;
                    insert_point = i;
                    break;
                }
            }
        }
        chidb_Btree_freeMemNode(bt, node_p);
        //Read in child, check if split, split it, free it, insert into it
        BTreeNode *child_node_p;
        int rd_msg = chidb_Btree_getNodeByPage(bt, child_page, &child_node_p);
        if(rd_msg != CHIDB_OK)
        {
            chidb_Btree_freeMemNode(bt, child_node_p);
            return rd_msg;
        }
        
        if(chidb_Btree_isNodeFull(child_node_p, btc))
        {
            npage_t child2;
            chidb_Btree_split(bt,npage, child_page, insert_point, &child2);
            chidb_Btree_freeMemNode(bt, child_node_p);
            return chidb_Btree_insertNonFull(bt, npage, btc);
        }
        else
        {
            chidb_Btree_freeMemNode(bt, child_node_p);
            return chidb_Btree_insertNonFull(bt, child_page, btc);
        }
        
    }
    //Control should never reach this point
    fprintf(log, "Should never hit this point: line 1183\n");
    fflush(log);    
    return CHIDB_EIO;
}

/*
 *  Removes a cell block from a node, and shifts any cells occuring before it back. 
 *  This function serves as a vaccum of sorts, cleaning the nodes, preventing fragmentation
 */
static int chidb_Btree_removeBlockFromNode(BTreeNode *btn, ncell_t ncell)
{
    BTreeCell rem_cell;
    int gcell_msg = chidb_Btree_getCell(btn, ncell, &rem_cell);
    if(gcell_msg != CHIDB_OK)
    {
        return gcell_msg;
    }

    uint32_t cell_size;
    switch(btn -> type)
    {
        case PGTYPE_TABLE_LEAF:
            cell_size = rem_cell.fields.tableLeaf.data_size + 8;
            break;
        case PGTYPE_TABLE_INTERNAL:
            cell_size = 8;
            break;
        case PGTYPE_INDEX_LEAF:
            cell_size = 12;
            break;
        case PGTYPE_INDEX_INTERNAL:
            cell_size = 16;
            break;
        default:
            cell_size = 0;
            break;
    }
    uint8_t *node_start_p = btn -> page -> data;
    uint8_t *cell_block_p = node_start_p + btn -> cells_offset;
    uint8_t *rem_cell_p = node_start_p + get2byte(btn->celloffset_array + 2*ncell);
    cell_block_p = memmove(cell_block_p + cell_size, cell_block_p, rem_cell_p - cell_block_p);
    btn -> cells_offset = cell_block_p - node_start_p;


    uint16_t rem_cell_offset = rem_cell_p - node_start_p;
    for(int i = ncell+1; i < btn->n_cells; i++)
    {
        uint16_t icell_offset = get2byte(btn->celloffset_array + (2 * i));
        if(icell_offset < rem_cell_offset)
        {
            uint8_t *icell_offset_p = btn->celloffset_array + (2 * i);
            put2byte(icell_offset_p, icell_offset+cell_size);
        }
    }
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
    BTreeNode *parent_p, *child_p, *new_child_p;
    int rd_msg = chidb_Btree_getNodeByPage(bt, npage_parent, &parent_p);
    if(rd_msg != CHIDB_OK)
    {
        fprintf(log, "Rd node error\n");
        return rd_msg;
    }
    rd_msg = chidb_Btree_getNodeByPage(bt, npage_child, &child_p);
    if(rd_msg != CHIDB_OK)
    {
        fprintf(log, "Rd node error\n");
        return rd_msg;
    }
    npage_t npage_new_child;
    int alloc_msg = chidb_Btree_newNode(bt, &npage_new_child, child_p -> type);
    if(alloc_msg != CHIDB_OK)
    {
        fprintf(log, "New Node Error\n");
        return alloc_msg;
    }
    *npage_child2 = npage_new_child;
    rd_msg = chidb_Btree_getNodeByPage(bt, npage_new_child, &new_child_p);
    if(rd_msg != CHIDB_OK)
    {
        fprintf(log, "Rd node error\n");
        return rd_msg;
    }

    //Insert nonfull ---if the full node is the right page, make sure to call the splitFunc with
    //the parent ncell being 1 past the current index of the last offset cell. In other words, make sure 
    //the value of that argument is the node_ncells. Make sure that this is done manually for the root
    //node in the insert function

//Assumes that the parent node is not full. Begins packing the new cell with the first half of child node
    
    /*
     *  Get the middle cell (or one past the middle cell in the case of an even node size)
     *  Then take the information from that cell and put it in a new cell (new_parent_cell),
     *      before inserting this new cell into the parent node
     */
    //This is the middle index of the fullnode which is the child node
    ncell_t index_middle = (child_p -> n_cells)%2 == 0 ? (child_p -> n_cells/2)-1: child_p -> n_cells/2;
    BTreeCell middle_cell;
    int gcell = chidb_Btree_getCell(child_p, index_middle, &middle_cell);
    if(gcell != CHIDB_OK)
    {
        return gcell;
    }
    BTreeCell new_parent_cell;
    new_parent_cell.type = parent_p -> type;
    new_parent_cell.key = middle_cell.key;
    if(new_parent_cell.type == PGTYPE_TABLE_INTERNAL)
    {
        new_parent_cell.fields.tableInternal.child_page = npage_new_child;
    }
    else if(new_parent_cell.type == PGTYPE_INDEX_INTERNAL)
    {
        new_parent_cell.fields.indexInternal.keyPk = 
                middle_cell.fields.indexInternal.keyPk;
        new_parent_cell.fields.indexInternal.child_page = npage_new_child;
        //chidb_Btree_removeBlockFromNode(child_p, mid_index);
        //remove the cell block here?
    }
    else{fprintf(log, "The parent should not be a leaf type\n");fflush(log);}

    /*
     *  Insert the new cell into the parent cell. Since the parent will always be an internal
     *      node, it will always have cells of a set size (not variable size) and it is safe to 
     *      assume that it will fit
     */
    int incell_msg = chidb_Btree_insertCell(parent_p, parent_ncell, &new_parent_cell);
    if(incell_msg != CHIDB_OK)
    {
        fprintf(log, "Insert into parent cell err\n");
        fflush(log);
        return incell_msg;
    }

    //Must be <= instead of < b/c the key in the parent node points to the new_child not the old child
    for(int i = 0; i<index_middle; i++)
    {
        BTreeCell cell;
        //printf("INSERTING THIS CELL: %d\n",cell.key);
        chidb_Btree_getCell(child_p, i, &cell);
        chidb_Btree_insertCell(new_child_p, i, &cell);
        chidb_Btree_removeBlockFromNode(child_p, i);
    }
    if(new_child_p -> type == PGTYPE_TABLE_LEAF)
    {
        BTreeCell cell;
        chidb_Btree_getCell(child_p, index_middle, &cell);
        chidb_Btree_insertCell(new_child_p, index_middle, &cell);
        // chidb_Btree_removeBlockFromNode(child_p, index_middle);
    }
    else if(isInternal(new_child_p -> type))
    {
        npage_t right_page = (new_child_p -> type == PGTYPE_TABLE_INTERNAL)?
                    middle_cell.fields.tableInternal.child_page:
                    middle_cell.fields.indexInternal.child_page;
        new_child_p -> right_page = right_page;
        //Don't need to write the bytes to the data because a nodewrite will automatically do this
    }
    chidb_Btree_removeBlockFromNode(child_p, index_middle);
    size_t bytes_to_move =
        (child_p->page->data + child_p->free_offset) - 
        (child_p->celloffset_array+ 2*(index_middle+1));
    size_t nbytes_removed = (child_p -> n_cells)*2 - bytes_to_move;
    memmove(child_p -> celloffset_array, (child_p -> celloffset_array)+2*(index_middle+1), bytes_to_move);
    child_p -> n_cells = bytes_to_move / 2;
    child_p -> free_offset = child_p -> free_offset - nbytes_removed;
    // for(int i = 0; i<(child_p->n_cells/2)-1; i++){
    //     BTreeCell cell;
    //     chidb_Btree_getCell(child_p, i, &cell);
    //     uint16_t offfset = get2byte(child_p -> celloffset_array + 2*i);
    //     fprintf(log, "key:%d , index: %d, offset:%d\n", cell.key, i, offfset);
    //     fflush(log);
    // }

    //change free offset, and num_cells
    //cell offset changed in prior function

    // fprintf(log, "ParentNodeAfter\n");
    // chidb_Btree_printNode(parent_p, log);
    // fprintf(log, "ChildNodeAfter\n");
    // chidb_Btree_printNode(child_p, log);
    // fprintf(log, "NewChildNodeAfter\n\n");
    // chidb_Btree_printNode(new_child_p, log);
    // if(npage_parent == 1){
    //     fprintf(log, "Root was split: Here are the parents, newNode, and childNode\n");
    //     chidb_Btree_printNode(parent_p,log);
    //     chidb_Btree_printNode(new_child_p,log);
    //     chidb_Btree_printNode(child_p,log);
    // }

    chidb_Btree_writeNode(bt, parent_p);
    chidb_Btree_freeMemNode(bt, parent_p);
    chidb_Btree_writeNode(bt, child_p);
    chidb_Btree_freeMemNode(bt, child_p);
    chidb_Btree_writeNode(bt, new_child_p);
    chidb_Btree_freeMemNode(bt, new_child_p);
    return CHIDB_OK;

// chidb_Btree_removeBlockFromNode(BTreeNode *btn, ncell_t ncell);
//     // chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **node);
//     // int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn);
//     // int chidb_Btree_writeNode(BTree *bt, BTreeNode *node);
//     // int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type);
//     // int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell);
//     // int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell);
}

