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

#include <map>

#include "kv_db.h"
#include "postgres.h"
#include "miscadmin.h"

static KVManagerClient* manager = NULL;
static map<KVWorkerId, KVWorkerClient*> workers;

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
    map<KVWorkerId, KVWorkerClient*>::iterator it = workers.find(workerId);
    if (it != workers.end())
    {
        return it->second;
    }

    if (!manager)
    {
        InitKVManagerClient();
    }

    bool success = manager->launch(workerId);
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

bool
KVOpenRequest(KVRelationId rid, OpenArgs* args)
{
    KVWorkerClient* worker = GetKVWorkerClient(rid);
    return worker->open(rid, args);
}
