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
#include "kv_api.h"
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
    char* filename;
    #ifdef VIDARDB
    bool  useColumn;
    int32 batchCapacity;
    #endif
} KVFdwOptions;

/* Functions used across files in kv_fdw */
extern KVFdwOptions* KVGetOptions(Oid foreignTableId);
extern void SerializeNullAttribute(TupleDesc tupleDescriptor, Index index,
                                   StringInfo buffer);
extern void SerializeAttribute(TupleDesc tupleDescriptor, Index index,
                               Datum datum, StringInfo buffer);
extern int  DeserializeAttribute(TupleDesc tupleDescriptor, Index index,
                                 int offset, char* key, char* val, char* limit,
                                 Datum* values, bool* nulls);
extern void SetRelationComparatorOpts(Relation relation, ComparatorOpts* opts);

#endif  /* KV_FDW_H_ */
