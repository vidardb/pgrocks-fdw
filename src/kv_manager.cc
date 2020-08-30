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

/*
 * In kv manager process scope
 */

static char const *MANAGER = "Manager";
static KVManager *manager = NULL;

/*
 * Implementation for kv manager client
 */

KVManagerClient::KVManagerClient()
{
    channel_ = new KVMessageQueue(InvalidOid, MANAGER, false);
}

KVManagerClient::~KVManagerClient()
{
    delete channel_;
}

bool
KVManagerClient::Launch(KVWorkerId const& workerId)
{
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpLaunch, workerId, MyDatabaseId);
    channel_->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

bool
KVManagerClient::Terminate(KVWorkerId const& workerId, KVDatabaseId const& dbId)
{
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpTerminate, workerId, dbId);
    channel_->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

void
KVManagerClient::Notify(KVCtrlType type)
{
    channel_->Notify(type);
}

/*
 * Implementation for kv manager
 */

KVManager::KVManager() : running_(false)
{
    channel_ = new KVMessageQueue(InvalidOid, MANAGER, true);

    workers_.clear();
}

KVManager::~KVManager()
{
    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
    for (it = workers_.begin(); it != workers_.end(); it++)
    {
        delete it->second;
    }
    delete channel_;
}

void
KVManager::Start()
{
    running_ = true;
}

void
KVManager::Run()
{
    while (running_)
    {
        KVMessage msg;
        channel_->Recv(msg);

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
    for (it = workers_.begin(); it != workers_.end(); it++)
    {
        it->second->client->Terminate(it->first);
        /* wait destroyed event */
        channel_->Wait(WorkerDesty);
        TerminateKVWorker(it->second->handle);
    }

    running_ = false;
    channel_->Terminate();
}

void
KVManager::Launch(KVWorkerId const& workerId, KVMessage const& msg)
{
    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it =
        workers_.find(workerId);
    if (it != workers_.end())
    {
        if (CheckKVWorkerAlive(it->second->handle))
        {
            channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
            return;
        }
        else
        {
            delete it->second;
            workers_.erase(it);
        }
    }

    void* handle = LaunchKVWorker(workerId, msg.hdr.dbId);
    if (!handle)
    {
        channel_->Send(SimpleFailureMessage(msg.hdr.resChan));
        return;
    }

    /* wait kv worker be ready */
    channel_->Wait(WorkerReady);

    KVDatabaseId dbId = msg.hdr.dbId;
    KVWorkerClient* client = new KVWorkerClient(workerId);
    KVWorkerHandle* worker = new KVWorkerHandle(workerId, dbId, client, handle);
    workers_.insert({workerId, worker});
    channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
}

void
KVManager::Terminate(KVWorkerId const& workerId, KVMessage const& msg)
{
    if (workerId == KVAllRelationId)
    {
        std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
        for (it = workers_.begin(); it != workers_.end();)
        {
            if (it->second->dbId != msg.hdr.dbId)
            {
                it++;
                continue;
            }

            KVWorkerHandle* handle = it->second;
            if (CheckKVWorkerAlive(handle->handle))
            {
                handle->client->Terminate(handle->workerId);
                /* wait destroyed event */
                channel_->Wait(WorkerDesty);
            }

            TerminateKVWorker(handle->handle);
            it = workers_.erase(it);
            delete handle;
        }

        channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
        return;
    }

    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it =
        workers_.find(workerId);
    if (it == workers_.end())
    {
        channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
        return;
    }

    KVWorkerHandle* handle = it->second;
    if (CheckKVWorkerAlive(handle->handle))
    {
        handle->client->Terminate(handle->workerId);
        /* wait destroyed event */
        channel_->Wait(WorkerDesty);
    }

    TerminateKVWorker(handle->handle);
    workers_.erase(it);
    delete handle;

    channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
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
