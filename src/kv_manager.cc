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

void
KVManagerClient::notify()
{
    channel->ready();
}

bool
KVManagerClient::launch(KVWorkerId const& workerId)
{
    KVMessage msg;
    channel->write(SimpleMessage(KVOpLaunch, workerId, MyDatabaseId));
    channel->read(msg);
    return msg.hdr.status == KVStatusSuccess;
}

bool
KVManagerClient::terminate(KVWorkerId const& workerId)
{
    KVMessage msg;
    channel->write(SimpleMessage(KVOpTerminate, workerId, MyDatabaseId));
    channel->read(msg);
    return msg.hdr.status == KVStatusSuccess;
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
        channel->read(msg);

        switch (msg.hdr.op)
        {
            case KVOpDummy:
                break;
            case KVOpLaunch:
                launch(msg.hdr.relId, msg.hdr.dbId);
                break;
            case KVOpTerminate:
                terminate(msg.hdr.relId);
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
        TerminateKVWorker(it->second->handle);
        delete it->second;
    }

    running = false;
    channel->interrupt(); 
}

void
KVManager::wait()
{
    channel->wait();
}

void
KVManager::launch(KVWorkerId const& workerId, KVDatabaseId const& dbId)
{
    if (workers.find(workerId) != workers.end())
    {
        channel->write(SimpleSuccessMessage());
        return;
    }

    void* handle = LaunchKVWorker(workerId, dbId);
    if (!handle)
    {
        channel->write(SimpleFailureMessage());
        return;
    }

    wait(); /* wait kv worker be ready */

    KVWorkerClient* client = new KVWorkerClient(workerId);
    KVWorkerHandle* worker = new KVWorkerHandle(workerId, client, handle);
    workers.insert({workerId, worker});
    channel->write(SimpleSuccessMessage());
}

void
KVManager::terminate(KVWorkerId const& workerId)
{
    map<KVWorkerId, KVWorkerHandle*>::iterator it = workers.find(workerId);
    if (it == workers.end())
    {
        channel->write(SimpleSuccessMessage());
        return;
    }

    KVWorkerHandle* handle = it->second;
    handle->client->terminate(handle->workerId);
    TerminateKVWorker(handle->handle);

    workers.erase(it);
    delete it->second;

    channel->write(SimpleSuccessMessage());
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
