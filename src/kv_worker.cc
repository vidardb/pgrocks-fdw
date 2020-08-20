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
    uint64 size = sizeof(args->opt) + strlen(args->path);
    #ifdef VIDARDB
    size += (sizeof(args->useColumn) + sizeof(args->attrCount))
    #endif

    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessageWithEntity(KVOpOpen, workerId,
        MyDatabaseId, args, size, ReadOpenArgs, WriteOpenArgs);
    channel->sendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

void
KVWorkerClient::terminate(KVWorkerId const& workerId)
{
    channel->send(SimpleMessage(KVOpTerminate, workerId, MyDatabaseId));
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
    while (running)
    {
        KVMessage msg;
        channel->recv(msg, MSGHEADER);

        switch (msg.hdr.op)
        {
            case KVOpDummy:
                break;
            case KVOpOpen:
                open(msg.hdr.relId, msg);
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
    channel->terminate();
}

void
KVWorker::open(KVWorkerId const& workerId, KVMessage& msg)
{
    OpenArgs args;
    char buf[MSGBUFSIZE];

    args.path = buf;
    msg.readFunc = ReadOpenArgs;
    msg.bdy = &args;

    channel->recv(msg, MSGENTITY);

    #ifdef VIDARDB
    connection->open(args.path, &args.opt, args.useColumn, args.attrCount);
    #else
    connection->open(args.path, &args.opt);
    #endif

    channel->send(SimpleSuccessMessageWithChannel(msg.hdr.resChan));
}

void
KVWorker::terminate(KVWorkerId const& workerId)
{
    stop();

    delete channel;
}

void
StartKVWorker(KVWorkerId workerId, KVDatabaseId dbId)
{
    KVWorker* worker = new KVWorker(workerId, dbId);
    worker->start();

    /* notify kv worker be ready */
    KVManagerClient* manager = new KVManagerClient();
    manager->channel->notify(WorkerReady);
    delete manager;

    worker->run();
    delete worker;
}
