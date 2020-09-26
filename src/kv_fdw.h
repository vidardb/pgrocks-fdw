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

/*
 * This header file is to cover kv_fdw.c and kv_utility.c. Anything
 * (functions, variables, defines) used across fdw and utility should be exposed
 * here.
 */


#ifndef KV_FDW_H_
#define KV_FDW_H_


#include "kv_api.h"

#include "lib/stringinfo.h"
#include "utils/relcache.h"


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
