/* Copyright 2020 VidarDB Inc.
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

#include "postgres.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef Oid KVDatabaseId;
typedef Oid KVRelationId;
typedef Oid KVWorkerId;

typedef enum
{
    KVOpDummy = 0, /* placeholder */
    KVOpOpen,
    KVOpClose,
    KVOpLaunch,
    KVOpTerminate,
} KVOperation;

typedef struct ComparatorOpt
{
    Oid   cmpFuncOid;
    Oid   attrCollOid;
    bool  attrByVal;
    int16 attrLength;
} ComparatorOpt;

typedef struct OpenArgs
{
    ComparatorOpt opt;
    #ifdef VIDARDB
    bool          useColumn;
    int           attrCount;
    #endif
    char*         path;
} OpenArgs;

/*
 * Communication API between kv client and kv worker
 */

extern bool KVOpenRequest(KVRelationId rid, OpenArgs* args);

/*
 * Utility API for kv manager and kv worker
 */

extern void  LaunchKVManager(void);
extern void  KVManagerMain(Datum arg);
extern void  StartKVManager(void);
extern void  TerminateKVManager(void);
extern void* LaunchKVWorker(KVWorkerId workerId, KVDatabaseId dbId);
extern void  KVWorkerMain(Datum arg);
extern void  StartKVWorker(KVWorkerId workerId, KVDatabaseId dbId);
extern void  TerminateKVWorker(void* worker);

/*
 * Utility API for string format and error report
 */

extern int  StringFormat(char *str, size_t count, const char *fmt, ...);
extern void ErrorReport(int level, int code, const char* msg);

#ifdef __cplusplus
}
#endif

#endif
