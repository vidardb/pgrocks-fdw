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
KVManagerClient::launch(KVWorkerId const& workerId)
{
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpLaunch, workerId, MyDatabaseId);
    channel->sendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

bool
KVManagerClient::terminate(KVWorkerId const& workerId)
{
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpTerminate, workerId, MyDatabaseId);
    channel->sendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
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
    map<KVWorkerId, KVWorkerHandle*>::iterator it;
    for (it = workers.begin(); it != workers.end(); it++)
    {
        delete it->second;
    }
    delete channel;
}

void
KVManager::start()
{
    running = true;
}

void
KVManager::run()
{
    while (running)
    {
        KVMessage msg;
        channel->recv(msg);

        switch (msg.hdr.op)
        {
            case KVOpDummy:
                break;
            case KVOpLaunch:
                launch(msg.hdr.relId, msg);
                break;
            case KVOpTerminate:
                terminate(msg.hdr.relId, msg);
                break;
            default:
                ErrorReport(WARNING, ERRCODE_WARNING,
                    "unsupported op in kv manager");
        }
    }
}

void
KVManager::stop()
{
    map<KVWorkerId, KVWorkerHandle*>::iterator it;
    for (it = workers.begin(); it != workers.end(); it++)
    {
        it->second->client->terminate(it->first);
        // TerminateKVWorker(it->second->handle);
        delete it->second;
    }

    running = false;
    channel->terminate(); 
}

void
KVManager::launch(KVWorkerId const& workerId, KVMessage const& msg)
{
    if (workers.find(workerId) != workers.end())
    {
        channel->send(SimpleSuccessMessageWithChannel(msg.hdr.resChan));
        return;
    }

    void* handle = LaunchKVWorker(workerId, msg.hdr.dbId);
    if (!handle)
    {
        channel->send(SimpleFailureMessageWithChannel(msg.hdr.resChan));
        return;
    }

    /* wait kv worker be ready */
    channel->wait(WorkerReady);

    KVWorkerClient* client = new KVWorkerClient(workerId);
    KVWorkerHandle* worker = new KVWorkerHandle(workerId, client, handle);
    workers.insert({workerId, worker});
    channel->send(SimpleSuccessMessageWithChannel(msg.hdr.resChan));
}

void
KVManager::terminate(KVWorkerId const& workerId, KVMessage const& msg)
{
    map<KVWorkerId, KVWorkerHandle*>::iterator it = workers.find(workerId);
    if (it == workers.end())
    {
        channel->send(SimpleSuccessMessageWithChannel(msg.hdr.resChan));
        return;
    }

    KVWorkerHandle* handle = it->second;
    handle->client->terminate(handle->workerId);
    // TerminateKVWorker(handle->handle);

    workers.erase(it);
    delete it->second;

    channel->send(SimpleSuccessMessageWithChannel(msg.hdr.resChan));
}

/*
 * Start kv manager
 */
void
StartKVManager(void)
{
    manager = new KVManager();
    manager->start();
    manager->run();
    delete manager;
}

/*
 * Terminate kv manager
 */
void
TerminateKVManager(void)
{
    assert(manager);
    manager->stop();
}
