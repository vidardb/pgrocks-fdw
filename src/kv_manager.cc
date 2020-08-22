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

#include <assert.h>

#include "kv_db.h"
#include "miscadmin.h"

static char const *MANAGER = "Manager";
static KVManager *manager = NULL;

/*
 * Implementation for kv manager client
 */

KVManagerClient::KVManagerClient()
{
    channel = new KVMessageQueue(InvalidOid, MANAGER, false);
}

KVManagerClient::~KVManagerClient()
{
    delete channel;
}

bool
KVManagerClient::Launch(KVWorkerId const& workerId)
{
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpLaunch, workerId, MyDatabaseId);
    channel->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

bool
KVManagerClient::Terminate(KVWorkerId const& workerId, KVDatabaseId const& dbId)
{
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpTerminate, workerId, dbId);
    channel->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

void
KVManagerClient::Notify(KVCtrlType type)
{
    channel->Notify(type);
}

/*
 * Implementation for kv manager
 */

KVManager::KVManager()
{
    channel = new KVMessageQueue(InvalidOid, MANAGER, true);
}

KVManager::~KVManager()
{
    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
    for (it = workers.begin(); it != workers.end(); it++)
    {
        delete it->second;
    }
    delete channel;
}

void
KVManager::Start()
{
    running = true;
}

void
KVManager::Run()
{
    while (running)
    {
        KVMessage msg;
        channel->Recv(msg);

        switch (msg.hdr.op)
        {
            case KVOpDummy:
                break;
            case KVOpLaunch:
                Launch(msg.hdr.relId, msg);
                break;
            case KVOpTerminate:
                Terminate(msg.hdr.relId, msg);
                break;
            default:
                ErrorReport(WARNING, ERRCODE_WARNING,
                    "unsupported op in kv manager");
        }
    }
}

void
KVManager::Stop()
{
    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
    for (it = workers.begin(); it != workers.end(); it++)
    {
        it->second->client->Terminate(it->first);
        TerminateKVWorker(it->second->handle);
        delete it->second;
    }

    running = false;
    channel->Terminate();
}

void
KVManager::Launch(KVWorkerId const& workerId, KVMessage const& msg)
{
    if (workers.find(workerId) != workers.end())
    {
        channel->Send(SimpleSuccessMessage(msg.hdr.resChan));
        return;
    }

    void* handle = LaunchKVWorker(workerId, msg.hdr.dbId);
    if (!handle)
    {
        channel->Send(SimpleFailureMessage(msg.hdr.resChan));
        return;
    }

    /* wait kv worker be ready */
    channel->Wait(WorkerReady);

    KVDatabaseId dbId = msg.hdr.dbId;
    KVWorkerClient* client = new KVWorkerClient(workerId);
    KVWorkerHandle* worker = new KVWorkerHandle(workerId, dbId, client, handle);
    workers.insert({workerId, worker});
    channel->Send(SimpleSuccessMessage(msg.hdr.resChan));
}

void
KVManager::Terminate(KVWorkerId const& workerId, KVMessage const& msg)
{
    if (workerId == KVAllRelationId)
    {
        std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
        for (it = workers.begin(); it != workers.end(); it++)
        {
            if (it->second->dbId != msg.hdr.dbId)
            {
                continue;
            }

            KVWorkerHandle* handle = it->second;
            handle->client->Terminate(handle->workerId);
            TerminateKVWorker(handle->handle);

            workers.erase(it);
            delete it->second;
        }

        channel->Send(SimpleSuccessMessage(msg.hdr.resChan));
        return;
    }

    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it =
        workers.find(workerId);
    if (it == workers.end())
    {
        channel->Send(SimpleSuccessMessage(msg.hdr.resChan));
        return;
    }

    KVWorkerHandle* handle = it->second;
    handle->client->Terminate(handle->workerId);
    TerminateKVWorker(handle->handle);

    workers.erase(it);
    delete it->second;

    channel->Send(SimpleSuccessMessage(msg.hdr.resChan));
}

/*
 * Start kv manager
 */
void
StartKVManager(void)
{
    manager = new KVManager();
    manager->Start();
    manager->Run();
    delete manager;
}

/*
 * Terminate kv manager
 */
void
TerminateKVManager(void)
{
    assert(manager);
    manager->Stop();
}
