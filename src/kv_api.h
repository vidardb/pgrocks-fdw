/* Copyright 2020-present VidarDB Inc. All rights reserved.
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

#ifndef KV_API_H_
#define KV_API_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "utils/relcache.h"
#include "access/attnum.h"


#define KVAllRelationId InvalidOid

typedef Oid    KVDatabaseId;
typedef Oid    KVRelationId;
typedef Oid    KVWorkerId;
typedef uint64 KVCursorId;

typedef enum
{
    KVOpDummy = 0, /* placeholder */
    KVOpOpen,
    KVOpClose,
    KVOpCount,
    KVOpPut,
    KVOpGet,
    KVOpDel,
    KVOpLoad,
    KVOpReadBatch,
    KVOpDelCursor,
    #ifdef VIDARDB
    KVOpRangeQuery,
    KVOpClearRangeQuery,
    #endif
    KVOpLaunch,
    KVOpTerminate,
} KVOperation;

typedef struct ComparatorOpts
{
    Oid   cmpFuncOid;
    Oid   attrCollOid;
    bool  attrByVal;
    int16 attrLength;
} ComparatorOpts;

typedef struct OpenArgs
{
    ComparatorOpts opts;
    #ifdef VIDARDB
    bool           useColumn;
    int            attrCount;
    #endif
    char*          path;
} OpenArgs;

typedef struct PutArgs
{
    uint64 keyLen;
    uint64 valLen;
    char*  key;
    char*  val;
} PutArgs;

typedef struct DeleteArgs
{
    uint64 keyLen;
    char*  key;
} DeleteArgs;

typedef struct GetArgs
{
    uint64  keyLen;
    char*   key;
    uint64* valLen;
    char**  val;
} GetArgs;

typedef struct ReadBatchArgs
{
    KVCursorId cursor;
    char**     buf;
    uint64*    bufLen;
} ReadBatchArgs;

typedef struct CloseCursorArgs
{
    KVCursorId cursor;
    void*      buf;
} CloseCursorArgs;

#ifdef VIDARDB
typedef struct RangeQueryOpts
{
    uint64      startLen;
    uint64      limitLen;
    char*       start;
    char*       limit;
    int         attrCount;
    AttrNumber* attrs;
    uint64      batchCapacity;
} RangeQueryOpts;

typedef struct RangeQueryArgs
{
    KVCursorId      cursor;
    char**          buf;
    uint64*         bufLen;
    RangeQueryOpts* opts;
} RangeQueryArgs;
#endif

/*
 * Communication API between kv client and kv worker
 */

extern void   KVOpenRequest(KVRelationId rid, OpenArgs* args);
extern void   KVCloseRequest(KVRelationId rid);
extern uint64 KVCountRequest(KVRelationId rid);
extern bool   KVPutRequest(KVRelationId rid, PutArgs* args);
extern bool   KVDeleteRequest(KVRelationId rid, DeleteArgs* args);
extern void   KVLoadRequest(KVRelationId rid, PutArgs* args);
extern bool   KVGetRequest(KVRelationId rid, GetArgs* args);
extern void   KVTerminateRequest(KVRelationId rid, KVDatabaseId dbId);
extern bool   KVReadBatchRequest(KVRelationId rid, ReadBatchArgs* args);
extern void   KVCloseCursorRequest(KVRelationId rid, CloseCursorArgs* args);
#ifdef VIDARDB
extern bool   KVRangeQueryRequest(KVRelationId rid, RangeQueryArgs* args);
extern void   KVClearRangeQueryRequest(KVRelationId rid, RangeQueryArgs* args);
#endif

/*
 * Utility API for kv manager and kv worker
 */

extern void  StartKVManager(void);
extern void  TerminateKVManager(void);
extern void* LaunchKVWorker(KVWorkerId workerId, KVDatabaseId dbId);
extern void  StartKVWorker(KVWorkerId workerId, KVDatabaseId dbId);

/*
 * Utility API for string format, error report and other tools
 */

extern int   StringFormat(char *str, size_t count, const char *fmt, ...);
extern void  ErrorReport(int level, int code, const char* msg);
extern void  SetRelationComparatorOpts(Relation relation, ComparatorOpts *opts);
extern void* AllocMemory(uint64 size);
extern void  FreeMemory(void* ptr);
extern uint8 EncodeVarintLength(uint64 len, char* buf);
extern uint8 DecodeVarintLength(char* start, char* limit, uint64* len);

#ifdef __cplusplus
}
#endif

#endif
