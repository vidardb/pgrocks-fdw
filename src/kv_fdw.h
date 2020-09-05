/* Copyright 2019-present VidarDB Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef KV_FDW_H_
#define KV_FDW_H_

#include <stdbool.h>
#include <semaphore.h>

#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "utils/hsearch.h"


/* Defines */
#define KVFDWNAME             "kv_fdw"
#define OPTION_FILENAME       "filename"
#define BUFSIZE               65536
#define HEADERBUFFSIZE        10
#ifdef VIDARDB
#define OPTION_STORAGE_FORMAT "storage"
#define OPTION_BATCH_CAPACITY "batch"
#define COLUMNSTORE           "column"
#define BATCHCAPACITY         8*1024*1024
#endif


/* Holds the option values to be used when reading or writing files.
 * To resolve these values, we first check foreign table's options,
 * and if not present, we then fall back to the default values.
 */
typedef struct KVFdwOptions {
    char *filename;
    #ifdef VIDARDB
    bool useColumn;
    int32 batchCapacity;
    #endif
} KVFdwOptions;

#ifdef VIDARDB
typedef struct TablePlanState {
    KVFdwOptions *fdwOptions;
    int attrCount;        /* total attributes in a table */
    List *targetAttrs;    /* attributes in select, where, groupby */
    bool toUpdateDelete;  /* any update or delete? indicate when to delete */
} TablePlanState;
#endif

/*
 * The scan state is for maintaining state for a scan, either for a
 * SELECT or UPDATE or DELETE.
 *
 * It is set up in BeginForeignScan and stashed in node->fdw_state and
 * subsequently used in IterateForeignScan, EndForeignScan and ReScanForeignScan.
 */
typedef struct TableReadState {
    bool isKeyBased;
    uint64 operationId;
    bool done;
    StringInfo key;
    char *buf;     /* shared mem for data returned by RangeQuery or ReadBatch */
    size_t bufLen; /* shared mem length, no next batch if it is 0 */
    char *next;    /* pointer to the next data entry for IterateForeignScan */
    bool hasNext;  /* whether a next batch from RangeQuery or ReadBatch*/

    #ifdef VIDARDB
    bool useColumn;
    List *targetAttrs;    /* attributes in select, where, group */
    #endif

    bool execExplainOnly;
} TableReadState;

/*
 * The modify state is for maintaining state of modify operations.
 *
 * It is set up in BeginForeignModify and stashed in
 * rinfo->ri_FdwState and subsequently used in ExecForeignInsert,
 * ExecForeignUpdate, ExecForeignDelete and EndForeignModify.
 */
typedef struct TableWriteState {
    CmdType operation;
} TableWriteState;


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);

extern void _PG_fini(void);

/* Functions used across files in kv_fdw */
extern KVFdwOptions *KVGetOptions(Oid foreignTableId);

extern void SerializeNullAttribute(TupleDesc tupleDescriptor, Index index,
                                   StringInfo buffer);

extern void SerializeAttribute(TupleDesc tupleDescriptor, Index index,
                               Datum datum, StringInfo buffer);

extern char *KVGetOptionValue(Oid foreignTableId, const char *optionName);

extern Datum ShortVarlena(Datum datum, int typeLength, char storage);

#endif
