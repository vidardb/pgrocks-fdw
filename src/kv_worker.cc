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

#include "kv_db.h"
#include "miscadmin.h"

static const char* WORKER = "Worker";

/*
 * Implementation for kv worker
 */

KVWorkerHandle::~KVWorkerHandle()
{
    delete client;
}

KVWorkerClient::KVWorkerClient(KVWorkerId workerId)
{
    channel = new KVMessageQueue(workerId, WORKER, false);
}

KVWorkerClient::~KVWorkerClient()
{
    delete channel;
}

void WriteOpenArgs(CircularQueueChannel* channel, uint64* offset, void* entity,
    uint64 size)
{
    OpenArgs* args = (OpenArgs*) entity;

    channel->write(offset, (char*) &args->opt, sizeof(args->opt));
    #ifdef VIDARDB
    channel->write(offset, (char*) &args->useColumn, sizeof(args->useColumn));
    channel->write(offset, (char*) &args->attrCount, sizeof(args->attrCount));
    #endif
    channel->write(offset, args->path, strlen(args->path));
}

void ReadOpenArgs(CircularQueueChannel* channel, uint64* offset, void* entity,
    uint64 size)
{
    OpenArgs* args = (OpenArgs*) entity;
    uint64 delta = sizeof(args->opt);

    channel->read(offset, (char*) &args->opt, sizeof(args->opt));
    #ifdef VIDARDB
    channel->read(offset, (char*) &args->useColumn, sizeof(args->useColumn));
    channel->read(offset, (char*) &args->attrCount, sizeof(args->attrCount));
    delta += (sizeof(args->useColumn) + sizeof(args->attrCount));
    #endif
    channel->read(offset, args->path, size - delta);
}

bool
KVWorkerClient::open(KVWorkerId const& workerId, OpenArgs* args)
{
    KVMessage msg;

    uint64 size = sizeof(args->opt) + strlen(args->path);
    #ifdef VIDARDB
    size += (sizeof(args->useColumn) + sizeof(args->attrCount))
    #endif
    channel->write(SimpleMessageWithEntity(KVOpOpen, workerId, MyDatabaseId,
        args, size, ReadOpenArgs, WriteOpenArgs));
    channel->read(msg);
    return msg.hdr.status == KVStatusSuccess;
}

void
KVWorkerClient::terminate(KVWorkerId const& workerId)
{
    channel->write(SimpleMessage(KVOpTerminate, workerId, MyDatabaseId));
}

KVWorker::KVWorker(KVWorkerId workerId, KVDatabaseId dbId) :
    workerId(workerId), dbId(dbId)
{
    channel = new KVMessageQueue(workerId, WORKER, true);
    connection = new KVConnection();
}

KVWorker::~KVWorker()
{
    delete channel;
    delete connection;
}

void
KVWorker::start()
{
    running = true;
}

void
KVWorker::run()
{
    char buf[MSGBUFSIZE];

    while (running)
    {
        KVMessage msg;
        channel->read(msg, MSGHEADER);

        switch (msg.hdr.op)
        {
            case KVOpDummy:
                break;
            case KVOpOpen:
                OpenArgs args;
                args.path = buf;

                msg.readFunc = ReadOpenArgs;
                msg.bdy = &args;
                channel->read(msg, MSGBODY);

                open(msg.hdr.relId, (OpenArgs*) msg.bdy);
                break;
            case KVOpClose:
                
                break;
            case KVOpTerminate:
                terminate(msg.hdr.relId);
                break;
            default:
                ErrorReport(WARNING, ERRCODE_WARNING,
                    "unsupported op in kv worker");
        }
    }
}

void
KVWorker::stop()
{
    running = false;
    channel->interrupt();
}

void
KVWorker::open(KVWorkerId const& workerId, OpenArgs* args)
{
    #ifdef VIDARDB
    connection->open(args->path, &args->opt, args->useColumn, args->attrCount);
    #else
    connection->open(args->path, &args->opt);
    #endif

    channel->write(SimpleSuccessMessage());
}

void
KVWorker::terminate(KVWorkerId const& workerId)
{
    stop(); /* terminate loop */
    delete channel; /* free shm */
}

void
StartKVWorker(KVWorkerId workerId, KVDatabaseId dbId)
{
    KVWorker* worker = new KVWorker(workerId, dbId);
    worker->start();

    /* notify kv worker be ready */
    KVManagerClient* manager = new KVManagerClient();
    manager->notify();
    delete manager;

    worker->run();
    delete worker;
}
