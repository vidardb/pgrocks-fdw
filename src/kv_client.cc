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

#include <unordered_map>

#include "kv_db.h"
#include "postgres.h"
#include "miscadmin.h"

/*
 * In backend process scope
 */

static KVManagerClient* manager = NULL;
static std::unordered_map<KVWorkerId, KVWorkerClient*> workers;

/*
 * Implementation for kv client
 */

static void
InitKVManagerClient()
{
    manager = new KVManagerClient();
}

static KVWorkerClient*
GetKVWorkerClient(KVWorkerId workerId)
{
    std::unordered_map<KVWorkerId, KVWorkerClient*>::iterator it =
        workers.find(workerId);
    if (it != workers.end())
    {
        return it->second;
    }

    if (!manager)
    {
        InitKVManagerClient();
    }

    bool success = manager->Launch(workerId);
    if (success)
    {
        KVWorkerClient* worker = new KVWorkerClient(workerId);
        workers.insert({workerId, worker});
        return worker;
    }

    ErrorReport(ERROR, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
                "too many background workers (consider increasing the "
                "configuration parameter \"max_worker_processes\")");
    return NULL;
}

void
KVOpenRequest(KVRelationId rid, OpenArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    worker->Open(rid, args);
}

void
KVCloseRequest(KVRelationId rid)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    worker->Close(rid);
}

uint64
KVCountRequest(KVRelationId rid)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    return worker->Count(rid);
}

bool
KVPutRequest(KVRelationId rid, PutArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    return worker->Put(rid, args);
}

bool
KVDeleteRequest(KVRelationId rid, DeleteArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    return worker->Delete(rid, args);
}

void
KVLoadRequest(KVRelationId rid, PutArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    worker->Load(rid, args);
}

bool
KVGetRequest(KVRelationId rid, GetArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    return worker->Get(rid, args);
}

void
KVTerminateRequest(KVRelationId rid, KVDatabaseId dbId)
{
    if (!manager)
    {
        InitKVManagerClient();
    }

    manager->Terminate(rid, dbId);
    workers.erase(rid);
}

bool
KVReadBatchRequest(KVRelationId rid, ReadBatchArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    return worker->ReadBatch(rid, args);
}

void
KVCloseCursorRequest(KVRelationId rid, CloseCursorArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    worker->CloseCursor(rid, args);
}

#ifdef VIDARDB
bool
KVRangeQueryRequest(KVRelationId rid, RangeQueryArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    return worker->RangeQuery(rid, args);;
}

void
KVClearRangeQueryRequest(KVRelationId rid, RangeQueryArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    worker->ClearRangeQuery(rid, args);
}
#endif
