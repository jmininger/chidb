/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine operations.
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


#include "dbm.h"
#include "btree.h"
#include "record.h"


/* Function pointer for dispatch table */
typedef int (*handler_function)(chidb_stmt *stmt, chidb_dbm_op_t *op);

/* Single entry in the instruction dispatch table */
struct handler_entry
{
    opcode_t opcode;
    handler_function func;
};

/* This generates all the instruction handler prototypes. It expands to:
 *
 * int chidb_dbm_op_OpenRead(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * int chidb_dbm_op_OpenWrite(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * ...
 * int chidb_dbm_op_Halt(chidb_stmt *stmt, chidb_dbm_op_t *op);
 */
#define HANDLER_PROTOTYPE(OP) int chidb_dbm_op_## OP (chidb_stmt *stmt, chidb_dbm_op_t *op);
FOREACH_OP(HANDLER_PROTOTYPE)


/* Ladies and gentlemen, the dispatch table. */
#define HANDLER_ENTRY(OP) { Op_ ## OP, chidb_dbm_op_## OP},

struct handler_entry dbm_handlers[] =
{
    FOREACH_OP(HANDLER_ENTRY)
};

int chidb_dbm_op_handle (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return dbm_handlers[op->opcode].func(stmt, op);
}


/*** INSTRUCTION HANDLER IMPLEMENTATIONS ***/


int chidb_dbm_op_Noop (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return CHIDB_OK;
}


int chidb_dbm_op_OpenRead (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_OpenWrite (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Close (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Rewind (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Next (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Prev (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Seek (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SeekGt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SeekGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

int chidb_dbm_op_SeekLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SeekLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

int chidb_dbm_op_Column (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Key (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Integer (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    
    if(!EXISTS_REGISTER(stmt, op -> p2))
    {
        int rc = realloc_reg(stmt, op -> p2);
    }
    chidb_dbm_register_t *r1 = &stmt -> reg[op -> p2];
    r1 -> type = REG_INT32;
    r1 -> value.i = op -> p1;
    /*
     *   struct chidb_stmt
     *   {
     *       chidb *db;
     *       chisql_statement_t *sql;
     *       uint32_t pc;
     *       chidb_dbm_op_t *ops;
     *       uint32_t nOps; 
     *       uint32_t endOp;
     *       chidb_dbm_register_t *reg;
     *       uint32_t nReg;
     *       chidb_dbm_cursor_t *cursors;
     *       uint32_t nCursors;
     *       uint32_t startRR;
     *       uint32_t nRR;
     *       char **cols;
     *       uint32_t nCols;
     *       bool explain;
     *   };
     *  typedef struct chidb_dbm_op
     *   {
     *       opcode_t opcode;
     *       int32_t p1;
     *       int32_t p2;
     *       int32_t p3;
     *       char *p4;
     *   } chidb_dbm_op_t;
     */
    return CHIDB_OK;
}


int chidb_dbm_op_String (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
   if(!EXISTS_REGISTER(stmt, op -> p2))
    {
        int rc = realloc_reg(stmt, op -> p2);
    }
    chidb_dbm_register_t *r1 = &stmt -> reg[op -> p2];
    r1 -> type = REG_STRING;
    r1 -> value.s = strndup(op -> p4, op -> p1);

    return CHIDB_OK;
}


int chidb_dbm_op_Null (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if(!EXISTS_REGISTER(stmt, op -> p2))
    {
        int rc = realloc_reg(stmt, op -> p2);
    }
    chidb_dbm_register_t* r = &stmt -> reg[op->p2];
    r -> type = REG_NULL;
    return CHIDB_OK;
}


int chidb_dbm_op_ResultRow (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_MakeRecord (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Insert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Eq (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if(IS_VALID_REGISTER(stmt, op -> p1) && IS_VALID_REGISTER(stmt, op -> p3))
    {
        chidb_dbm_register_t *r1 = &stmt->reg[op->p1];
        chidb_dbm_register_t *r2 = &stmt->reg[op->p3];
        /* 
            The types are assumed to be true according to chidb docs so this is unneccessary:
            if(r1 -> type == r2 -> type)
        */
        bool isRegsEqual = false;
        switch(r1 -> type)
        {
            case REG_INT32:
                isRegsEqual = (r1 -> value.i == r2 -> value.i);
                break;
            case REG_STRING:
                isRegsEqual = (r1 -> value.s == r2 -> value.s);
                break;
            case REG_BINARY:
            /* Check that the sizes are equal and then compare the data */
                isRegsEqual = (r1 -> value.bin.nbytes == r2 -> value.bin.nbytes) &&
                                        (0 == memcmp(
                                                    r1 -> value.bin.bytes, 
                                                    r2 -> value.bin.bytes, 
                                                    r1 -> value.bin.nbytes));
                break;
            default:
                break;
        }
        if(isRegsEqual)
        {
            stmt -> pc = op -> p2;
        }
        return CHIDB_OK;
    }
    else 
        return CHIDB_EMISUSE;
}


int chidb_dbm_op_Ne (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if(!IS_VALID_REGISTER(stmt, op -> p1) || !IS_VALID_REGISTER(stmt, op -> p3))
    {
        return CHIDB_EMISUSE;
    }
    else
    {
        chidb_dbm_register_t *r1 = &stmt->reg[op->p1];
        chidb_dbm_register_t *r2 = &stmt->reg[op->p3];
        /* 
            The types are assumed to be true according to chidb docs so this is unneccessary:
            if(r1 -> type == r2 -> type)
        */
        bool isRegsNEqual = false;
        switch(r1 -> type)
        {
            case REG_INT32:
                isRegsNEqual = (r1 -> value.i != r2 -> value.i);
                break;
            case REG_STRING:
                isRegsNEqual = (r1 -> value.s != r2 -> value.s);
                break;
            case REG_BINARY:
                isRegsNEqual = !((r1 -> value.bin.nbytes == r2 -> value.bin.nbytes) &&
                    (0 == memcmp(
                            r1 -> value.bin.bytes, 
                            r2 -> value.bin.bytes, 
                            r1 -> value.bin.nbytes)));
                break;
            default:
                break;
        }
        if(isRegsNEqual)
        {
            stmt -> pc = op -> p2;
        }
        return CHIDB_OK;
    } 
}


int chidb_dbm_op_Lt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    if(!IS_VALID_REGISTER(stmt, op -> p1) || !IS_VALID_REGISTER(stmt, op -> p3))
    {
        return CHIDB_EMISUSE;
    }
    else
    {
        chidb_dbm_register_t *r1 = &stmt->reg[op->p1];
        chidb_dbm_register_t *r2 = &stmt->reg[op->p3];

        bool isRegs2LtReg1 = false;
        switch(r1 -> type)
        {
            case REG_INT32:
                isRegs2LtReg1 = (r1 -> value.i != r2 -> value.i);
                break;
            case REG_STRING:
                isRegs2LtReg1 = (r1 -> value.s != r2 -> value.s);
                break;
            case REG_BINARY:
                isRegs2LtReg1 = !((r1 -> value.bin.nbytes == r2 -> value.bin.nbytes) &&
                    (0 == memcmp(
                        r1 -> value.bin.bytes, 
                        r2 -> value.bin.bytes, 
                        r1 -> value.bin.nbytes)));
                break;
            default:
                break;
        }
        if(isRegs2LtReg1)
        {
            stmt -> pc = op -> p2;
        }
        return CHIDB_OK;
    } 
}


int chidb_dbm_op_Le (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Gt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Ge (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


/* IdxGt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 * 
 * if (idxkey at cursor p1) > k, jump
 */
int chidb_dbm_op_IdxGt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxGt\n");
  exit(1);
}

/* IdxGe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 * 
 * if (idxkey at cursor p1) >= k, jump
 */
int chidb_dbm_op_IdxGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxGe\n");
  exit(1);
}

/* IdxLt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 * 
 * if (idxkey at cursor p1) < k, jump
 */
int chidb_dbm_op_IdxLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxLt\n");
  exit(1);
}

/* IdxLe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 * 
 * if (idxkey at cursor p1) <= k, jump
 */
int chidb_dbm_op_IdxLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxLe\n");
  exit(1);
}


/* IdxPKey p1 p2 * *
 *
 * p1: cursor
 * p2: register
 *
 * store pkey from (cell at cursor p1) in (register at p2)
 */
int chidb_dbm_op_IdxPKey (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxKey\n");
  exit(1);
}

/* IdxInsert p1 p2 p3 *
 *
 * p1: cursor
 * p2: register containing IdxKey
 * p2: register containing PKey
 *
 * add new (IdkKey,PKey) entry in index BTree pointed at by cursor at p1
 */
int chidb_dbm_op_IdxInsert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
  fprintf(stderr,"todo: chidb_dbm_op_IdxInsert\n");
  exit(1);
}


int chidb_dbm_op_CreateTable (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_CreateIndex (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Copy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SCopy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Halt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

