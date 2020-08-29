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
#include "kv_posix.h"
#include "kv_storage.h"
#include "miscadmin.h"
#include "fcntl.h"

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
    channel_ = new KVMessageQueue(workerId, WORKER, false);
}

KVWorkerClient::~KVWorkerClient()
{
    delete channel_;
}

void WriteOpenArgs(void* channel, uint64* offset, void* entity, uint64 size)
{
    OpenArgs* args = (OpenArgs*) entity;

    ((KVChannel*) channel)->Write(offset, (char*) &args->opts,
                                  sizeof(args->opts));
    #ifdef VIDARDB
    ((KVChannel*) channel)->Write(offset, (char*) &args->useColumn,
                                  sizeof(args->useColumn));
    ((KVChannel*) channel)->Write(offset, (char*) &args->attrCount,
                                  sizeof(args->attrCount));
    #endif
    ((KVChannel*) channel)->Write(offset, args->path, strlen(args->path));
}

void ReadOpenArgs(void* channel, uint64* offset, void* entity, uint64 size)
{
    OpenArgs* args = (OpenArgs*) entity;
    uint64 delta = sizeof(args->opts);

    ((KVChannel*) channel)->Read(offset, (char*) &args->opts,
                                 sizeof(args->opts));
    #ifdef VIDARDB
    ((KVChannel*) channel)->Read(offset, (char*) &args->useColumn,
                                 sizeof(args->useColumn));
    ((KVChannel*) channel)->Read(offset, (char*) &args->attrCount,
                                 sizeof(args->attrCount));
    delta += (sizeof(args->useColumn) + sizeof(args->attrCount));
    #endif
    ((KVChannel*) channel)->Read(offset, args->path, size - delta);
}

bool
KVWorkerClient::Open(KVWorkerId const& workerId, OpenArgs* args)
{
    uint64 size = sizeof(args->opts) + strlen(args->path);
    #ifdef VIDARDB
    size += (sizeof(args->useColumn) + sizeof(args->attrCount));
    #endif

    KVMessage sendmsg = SimpleMessage(KVOpOpen, workerId, MyDatabaseId);
    sendmsg.bdy = args;
    sendmsg.hdr.bdySize = size;
    sendmsg.writeFunc = WriteOpenArgs;

    KVMessage recvmsg;
    channel_->SendWithResponse(sendmsg, recvmsg);

    return recvmsg.hdr.status == KVStatusSuccess;
}

void WritePutArgs(void* channel, uint64* offset, void* entity, uint64 size)
{
    PutArgs* args = (PutArgs*) entity;

    ((KVChannel*) channel)->Write(offset, (char*) &args->keyLen,
                                  sizeof(args->keyLen));
    ((KVChannel*) channel)->Write(offset, args->key, args->keyLen);
    ((KVChannel*) channel)->Write(offset, args->val, args->valLen);
}

bool
KVWorkerClient::Put(KVWorkerId const& workerId, PutArgs* args)
{
    uint64 size = args->keyLen + args->valLen + sizeof(args->keyLen);

    KVMessage sendmsg = SimpleMessage(KVOpPut, workerId, MyDatabaseId);
    sendmsg.bdy = args;
    sendmsg.hdr.bdySize = size;
    sendmsg.writeFunc = WritePutArgs;

    KVMessage recvmsg;
    channel_->SendWithResponse(sendmsg, recvmsg);

    return recvmsg.hdr.status == KVStatusSuccess;
}

bool
KVWorkerClient::Delete(KVWorkerId const& workerId, DeleteArgs* args)
{
    KVMessage sendmsg = SimpleMessage(KVOpDel, workerId, MyDatabaseId);
    sendmsg.bdy = args->key;
    sendmsg.hdr.bdySize = args->keyLen;
    sendmsg.writeFunc = CommonWriteEntity;

    KVMessage recvmsg;
    channel_->SendWithResponse(sendmsg, recvmsg);

    return recvmsg.hdr.status == KVStatusSuccess;
}

void
KVWorkerClient::Load(KVWorkerId const& workerId, PutArgs* args)
{
    uint64 size = args->keyLen + args->valLen + sizeof(args->keyLen);

    KVMessage sendmsg = SimpleMessage(KVOpLoad, workerId, MyDatabaseId);
    sendmsg.bdy = args;
    sendmsg.hdr.bdySize = size;
    sendmsg.writeFunc = WritePutArgs;

    channel_->Send(sendmsg);
}

bool
KVWorkerClient::Get(KVWorkerId const& workerId, GetArgs* args)
{
    KVMessage sendmsg = SimpleMessage(KVOpGet, workerId, MyDatabaseId);
    sendmsg.bdy = args->key;
    sendmsg.hdr.bdySize = args->keyLen;
    sendmsg.writeFunc = CommonWriteEntity;

    KVMessage recvmsg;
    uint32 chan = channel_->LeaseResponseQueue();
    sendmsg.hdr.resChan = chan;
    recvmsg.hdr.resChan = chan;
    channel_->Send(sendmsg);
    channel_->Recv(recvmsg, MSGHEADER);

    *(args->valLen) = recvmsg.hdr.bdySize;
    *(args->val) = (char*) AllocMemory(*(args->valLen));
    recvmsg.bdy = *(args->val);
    recvmsg.readFunc = CommonReadEntity;
    channel_->Recv(recvmsg, MSGENTITY);
    channel_->UnleaseResponseQueue(chan);

    return recvmsg.hdr.status == KVStatusSuccess;
}

bool
KVWorkerClient::Close(KVWorkerId const& workerId)
{
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpClose, workerId, MyDatabaseId);
    channel_->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

uint64
KVWorkerClient::Count(KVWorkerId const& workerId)
{
    uint64 count;

    KVMessage recvmsg;
    recvmsg.bdy = &count;
    recvmsg.hdr.bdySize = sizeof(count);
    recvmsg.readFunc = CommonReadEntity;

    KVMessage sendmsg = SimpleMessage(KVOpCount, workerId, MyDatabaseId);
    channel_->SendWithResponse(sendmsg, recvmsg);

    return count;
}

void
KVWorkerClient::Terminate(KVWorkerId const& workerId)
{
    channel_->Send(SimpleMessage(KVOpTerminate, workerId, MyDatabaseId));
}

void WriteReadBatchArgs(void* channel, uint64* offset, void* entity, uint64 size)
{
    ReadBatchArgs* args = (ReadBatchArgs*) entity;

    pid_t pid = getpid();
    ((KVChannel*) channel)->Write(offset, (char*) &pid, sizeof(pid_t));
    ((KVChannel*) channel)->Write(offset, (char*) &args->cursor,
                                  sizeof(args->cursor));
}

bool
KVWorkerClient::ReadBatch(KVWorkerId const& workerId, ReadBatchArgs* args)
{
    if (*(args->buf) != NULL)
    {
        Munmap(*(args->buf), READBATCHSIZE, __func__);
    }

    KVMessage sendmsg = SimpleMessage(KVOpReadBatch, workerId, MyDatabaseId);
    sendmsg.bdy = args;
    sendmsg.hdr.bdySize = sizeof(pid_t) + sizeof(args->cursor);
    sendmsg.writeFunc = WriteReadBatchArgs;

    char      buf[MSGBUFSIZE];
    KVMessage recvmsg;
    recvmsg.bdy = buf;
    recvmsg.readFunc = CommonReadEntity;
    channel_->SendWithResponse(sendmsg, recvmsg);

    if (recvmsg.hdr.status != KVStatusSuccess)
    {
        return false;
    }

    bool next = *((bool*) buf);
    *(args->bufLen) = *((uint64*) (buf + sizeof(next)));
    if (*(args->bufLen) == 0)
    {
        *(args->buf) = NULL;
    }
    else
    {
        char  name[MAXPATHLENGTH];
        pid_t pid = getpid();

        StringFormat(name, MAXPATHLENGTH, "%s%d%d%lu", READBATCHPATH, pid,
                     workerId, args->cursor);
        int fd = ShmOpen(name, O_RDWR, 0777, __func__);
        *(args->buf) = (char*) Mmap(NULL, READBATCHSIZE, PROT_READ | PROT_WRITE,
                                    MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
    }

    return next;
}

void WriteDelCursorArgs(void* channel, uint64* offset, void* entity, uint64 size)
{
    DelCursorArgs* args = (DelCursorArgs*) entity;

    pid_t pid = getpid();
    ((KVChannel*) channel)->Write(offset, (char*) &pid, sizeof(pid_t));
    ((KVChannel*) channel)->Write(offset, (char*) &args->cursor,
                                  sizeof(args->cursor));
}

bool
KVWorkerClient::CloseCursor(KVWorkerId const& workerId, DelCursorArgs* args)
{
    if (args->buf)
    {
        Munmap(args->buf, READBATCHSIZE, __func__);
    }

    char  name[MAXPATHLENGTH];
    pid_t pid = getpid();

    StringFormat(name, MAXPATHLENGTH, "%s%d%d%lu", READBATCHPATH, pid,
                 workerId, args->cursor);
    ShmUnlink(name, __func__);

    KVMessage sendmsg = SimpleMessage(KVOpDelCursor, workerId, MyDatabaseId);
    sendmsg.bdy = args;
    sendmsg.hdr.bdySize = sizeof(pid_t) + sizeof(args->cursor);
    sendmsg.writeFunc = WriteDelCursorArgs;

    KVMessage recvmsg;
    channel_->SendWithResponse(sendmsg, recvmsg);

    return recvmsg.hdr.status == KVStatusSuccess;
}

#ifdef VIDARDB
void WriteRangeQueryArgs(void* channel, uint64* offset, void* entity,
    uint64 size)
{
    RangeQueryArgs* args = (RangeQueryArgs*) entity;
    RangeQueryOpts* opts = args->opts;

    pid_t pid = getpid();
    ((KVChannel*) channel)->Write(offset, (char*) &pid, sizeof(pid_t));
    ((KVChannel*) channel)->Write(offset, (char*) &args->cursor,
                                  sizeof(args->cursor));

    if (opts)
    {
        ((KVChannel*) channel)->Write(offset, (char*) &(opts->startLen),
                       sizeof(opts->startLen));
        if (opts->startLen > 0)
        {
            ((KVChannel*) channel)->Write(offset, opts->start, opts->startLen);
        }

        ((KVChannel*) channel)->Write(offset, (char*) &(opts->limitLen),
                       sizeof(opts->limitLen));
        if (opts->limitLen > 0)
        {
            ((KVChannel*) channel)->Write(offset, opts->limit, opts->limitLen);
        }

        ((KVChannel*) channel)->Write(offset, (char*) &opts->batchCapacity,
                       sizeof(opts->batchCapacity));
        ((KVChannel*) channel)->Write(offset, (char*) &opts->attrCount,
                       sizeof(opts->attrCount));
        if (opts->attrCount > 0)
        {
            ((KVChannel*) channel)->Write(offset, (char*) opts->attrs,
                           opts->attrCount * sizeof(*(opts->attrs)));
        }
    }
}

bool
KVWorkerClient::RangeQuery(KVWorkerId const& workerId, RangeQueryArgs* args)
{
    if (*(args->buf) && *(args->bufLen) > 0)
    {
        Munmap(*(args->buf), *(args->bufLen), __func__);
    }

    KVMessage sendmsg = SimpleMessage(KVOpRangeQuery, workerId, MyDatabaseId);
    sendmsg.bdy = args;
    sendmsg.hdr.bdySize = sizeof(pid_t) + sizeof(args->cursor);
    if (args->opts)
    {
        sendmsg.hdr.bdySize += sizeof(args->opts->startLen);
        sendmsg.hdr.bdySize += args->opts->startLen;
        sendmsg.hdr.bdySize += sizeof(args->opts->limitLen);
        sendmsg.hdr.bdySize += args->opts->limitLen;
        sendmsg.hdr.bdySize += sizeof(args->opts->attrCount);
        sendmsg.hdr.bdySize += args->opts->attrCount *
                               sizeof(*(args->opts->attrs));
        sendmsg.hdr.bdySize += sizeof(args->opts->batchCapacity);
    }
    sendmsg.writeFunc = WriteRangeQueryArgs;

    char      buf[MSGBUFSIZE];
    KVMessage recvmsg;
    recvmsg.bdy = buf;
    recvmsg.readFunc = CommonReadEntity;
    channel_->SendWithResponse(sendmsg, recvmsg);

    if (recvmsg.hdr.status != KVStatusSuccess)
    {
        return false;
    }

    bool next = *((bool*) buf);
    *(args->bufLen) = *((uint64*) (buf + sizeof(next)));
    if (*(args->bufLen) == 0)
    {
        *(args->buf) = NULL;
    }
    else
    {
        char  name[MAXPATHLENGTH];
        pid_t pid = getpid();

        StringFormat(name, MAXPATHLENGTH, "%s%d%d%lu", RANGEQUERYPATH, pid,
                     workerId, args->cursor);
        int fd = ShmOpen(name, O_RDWR, 0777, __func__);
        *(args->buf) = (char*) Mmap(NULL, *(args->bufLen),
                                    PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0,
                                    __func__);
        Fclose(fd, __func__);
    }

    return next;
}

void
KVWorkerClient::ClearRangeQuery(KVWorkerId const& workerId, RangeQueryArgs* args)
{
    if (*(args->buf) && *(args->bufLen) > 0)
    {
        Munmap(*(args->buf), *(args->bufLen), __func__);
    }

    KVMessage sendmsg = SimpleMessage(KVOpClearRangeQuery, workerId,
                                      MyDatabaseId);
    args->opts = NULL;
    sendmsg.bdy = args;
    sendmsg.hdr.bdySize = sizeof(pid_t) + sizeof(args->cursor);
    sendmsg.writeFunc = WriteRangeQueryArgs;

    channel_->Send(sendmsg);
}
#endif

KVWorker::KVWorker(KVWorkerId workerId, KVDatabaseId dbId) :
    workerId_(workerId), running_(false), conn_(NULL), ref_(0)
{
    channel_ = new KVMessageQueue(workerId, WORKER, true);

    cursors_.clear();
    #ifdef VIDARDB
    ranges_.clear();
    #endif
}

KVWorker::~KVWorker()
{
    if (conn_)
    {
        CloseConn(conn_);
        conn_ = NULL;
    }

    delete channel_;
}

void
KVWorker::Start()
{
    running_ = true;
}

void
KVWorker::Run()
{
    while (running_)
    {
        KVMessage msg;
        channel_->Recv(msg, MSGHEADER);

        switch (msg.hdr.op)
        {
            case KVOpDummy:
                break;
            case KVOpOpen:
                Open(msg.hdr.relId, msg);
                break;
            case KVOpClose:
                Close(msg.hdr.relId, msg);
                break;
            case KVOpCount:
                Count(msg.hdr.relId, msg);
                break;
            case KVOpPut:
                Put(msg.hdr.relId, msg);
                break;
            case KVOpGet:
                Get(msg.hdr.relId, msg);
                break;
            case KVOpDel:
                Delete(msg.hdr.relId, msg);
                break;
            case KVOpLoad:
                Load(msg.hdr.relId, msg);
                break;
            case KVOpTerminate:
                Terminate(msg.hdr.relId, msg);
                break;
            case KVOpReadBatch:
                ReadBatch(msg.hdr.relId, msg);
                break;
            case KVOpDelCursor:
                CloseCursor(msg.hdr.relId, msg);
                break;
            #ifdef VIDARDB
            case KVOpRangeQuery:
                RangeQuery(msg.hdr.relId, msg);
                break;
            case KVOpClearRangeQuery:
                ClearRangeQuery(msg.hdr.relId, msg);
                break;
            #endif
            default:
                ErrorReport(WARNING, ERRCODE_WARNING,
                    "unsupported op in kv worker");
        }
    }
}

void
KVWorker::Stop()
{
    running_ = false;

    channel_->Terminate();
}

void
KVWorker::Open(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    OpenArgs args;
    args.path = buf;
    msg.bdy = &args;
    msg.readFunc = ReadOpenArgs;
    channel_->Recv(msg, MSGENTITY);

    if (conn_)
    {
        ref_++;
    }
    else
    {
        #ifdef VIDARDB
        conn_ = OpenConn(args.path, args.useColumn, args.attrCount, &args.opts);
        #else
        conn_ = OpenConn(args.path, &args.opts);
        #endif
    }

    channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
}

void
KVWorker::Put(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    msg.bdy = buf;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    PutArgs args;
    args.keyLen = *((uint64*) buf);
    args.valLen = msg.hdr.bdySize - args.keyLen - sizeof(args.keyLen);
    args.key = buf + sizeof(args.keyLen);
    args.val = buf + sizeof(args.keyLen) + args.keyLen;

    bool success = PutRecord(conn_, args.key, args.keyLen, args.val, args.valLen);
    if (success)
    {
        channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
    }
    else
    {
        channel_->Send(SimpleFailureMessage(msg.hdr.resChan));
    }
}

void
KVWorker::Delete(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    msg.bdy = buf;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    bool success = DelRecord(conn_, buf, msg.hdr.bdySize);
    if (success)
    {
        channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
    }
    else
    {
        channel_->Send(SimpleFailureMessage(msg.hdr.resChan));
    }
}

void
KVWorker::Load(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    msg.bdy = buf;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    PutArgs args;
    args.keyLen = *((uint64*) buf);
    args.valLen = msg.hdr.bdySize - args.keyLen - sizeof(args.keyLen);
    args.key = buf + sizeof(args.keyLen);
    args.val = buf + sizeof(args.keyLen) + args.keyLen;

    PutRecord(conn_, args.key, args.keyLen, args.val, args.valLen);
}

void
KVWorker::Get(KVWorkerId const& workerId, KVMessage& msg)
{
    char   key[MSGBUFSIZE], *val;
    uint64 valLen;

    msg.bdy = key;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    bool success = GetRecord(conn_, key, msg.hdr.bdySize, &val, &valLen);
    if (success)
    {
        KVMessage sendmsg = SimpleSuccessMessage(msg.hdr.resChan);
        sendmsg.hdr.bdySize = valLen;
        sendmsg.bdy = val;
        sendmsg.writeFunc = CommonWriteEntity;
        channel_->Send(sendmsg);
    }
    else
    {
        channel_->Send(SimpleFailureMessage(msg.hdr.resChan));
    }
}

void
KVWorker::Close(KVWorkerId const& workerId, KVMessage& msg)
{
    channel_->Recv(msg, MSGDISCARD);

    if (conn_)
    {
        ref_--;
    }

    channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
}

void
KVWorker::Count(KVWorkerId const& workerId, KVMessage& msg)
{
    channel_->Recv(msg, MSGDISCARD);

    uint64 count = GetCount(conn_);

    KVMessage sendmsg;
    sendmsg.bdy = &count;
    sendmsg.hdr.bdySize = sizeof(count);
    sendmsg.hdr.resChan = msg.hdr.resChan;
    sendmsg.writeFunc = CommonWriteEntity;

    channel_->Send(sendmsg);
}

void
KVWorker::Terminate(KVWorkerId const& workerId, KVMessage& msg)
{
    channel_->Recv(msg, MSGDISCARD);

    Stop();
}

struct ReadBatchState
{
    bool   next;
    uint64 size;
};

void WriteReadBatchState(void* channel, uint64* offset, void* entity,
    uint64 size)
{
    ReadBatchState* state = (ReadBatchState*) entity;

    ((KVChannel*) channel)->Write(offset, (char*) &state->next,
                                  sizeof(state->next));
    ((KVChannel*) channel)->Write(offset, (char*) &state->size,
                                  sizeof(state->size));
}

void
KVWorker::ReadBatch(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    msg.bdy = buf;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    key.pid = *((pid_t*) buf);
    key.cursor = *((KVCursorId*) (buf + sizeof(key.pid)));

    void* cursor = NULL;
    std::unordered_map<KVCursorKey, void*, KVCursorKeyHashFunc>::iterator it;
    it = cursors_.find(key);
    if (it == cursors_.end())
    {
        cursor = GetIter(conn_);
        cursors_.insert({key, cursor});
    }
    else
    {
        cursor = it->second;
    }

    char name[MAXPATHLENGTH];
    StringFormat(name, MAXPATHLENGTH, "%s%d%d%lu", READBATCHPATH, key.pid,
                 workerId, key.cursor);
    ShmUnlink(name, __func__);
    int fd = ShmOpen(name, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, READBATCHSIZE, __func__);
    char* shm = (char*) Mmap(NULL, READBATCHSIZE, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    ReadBatchState state;
    state.next = BatchRead(conn_, cursor, shm, &state.size);

    KVMessage sendmsg = SimpleSuccessMessage(msg.hdr.resChan);
    sendmsg.hdr.bdySize = sizeof(state);
    sendmsg.bdy = &state;
    sendmsg.writeFunc = WriteReadBatchState;

    channel_->Send(sendmsg);
}

void
KVWorker::CloseCursor(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    msg.bdy = buf;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    key.pid = *((pid_t*) buf);
    key.cursor = *((KVCursorId*) (buf + sizeof(key.pid)));

    std::unordered_map<KVCursorKey, void*, KVCursorKeyHashFunc>::iterator it;
    it = cursors_.find(key);
    if (it == cursors_.end())
    {
        channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
        return;
    }

    DelIter(it->second);
    cursors_.erase(it);

    channel_->Send(SimpleSuccessMessage(msg.hdr.resChan));
}

#ifdef VIDARDB
void
KVWorker::RangeQuery(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    msg.bdy = buf;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    char* current = buf;

    key.pid = *((pid_t*) current);
    current += sizeof(key.pid);
    key.cursor = *((KVCursorId*) current);
    current += sizeof(key.cursor);

    KVRangeQueryEntry entry;
    std::unordered_map<KVCursorKey, KVRangeQueryEntry, KVCursorKeyHashFunc>::iterator it;
    it = ranges_.find(key);
    if (it == ranges_.end())
    {
        RangeQueryOpts opts;

        opts.startLen = *((uint64*) current);
        current += sizeof(opts.startLen);

        if (opts.startLen > 0)
        {
            opts.start = (char*) AllocMemory(opts.startLen);
            memcpy(opts.start, current, opts.startLen);
            current += opts.startLen;
        }

        opts.limitLen = *((uint64*) current);
        current += sizeof(opts.limitLen);

        if (opts.limitLen > 0)
        {
            opts.limit = (char*) AllocMemory(opts.limitLen);
            memcpy(opts.limit, current, opts.limitLen);
            current += opts.limitLen;
        }

        opts.batchCapacity = *((uint64*) current);
        current += sizeof(opts.batchCapacity);

        opts.attrCount = *((int*) current);
        current += sizeof(opts.attrCount);

        if (opts.attrCount > 0)
        {
            opts.attrs = (AttrNumber*) current;
        }

        ParseRangeQueryOptions(&opts, &entry.range, &entry.readOpts);
        ranges_.insert({key, entry});
    }
    else
    {
        entry = it->second;
    }

    void* result = NULL;
    ReadBatchState state;

    do
    {
        state.next = RangeQueryRead(conn_, entry.range, &entry.readOpts,
                                    &state.size, &result);
    } while (state.next && state.size == 0);

    char name[MAXPATHLENGTH];
    StringFormat(name, MAXPATHLENGTH, "%s%d%d%lu", RANGEQUERYPATH, key.pid,
                 workerId_, key.cursor);
    ShmUnlink(name, __func__);

    char* shm = NULL;
    if (state.size > 0)
    {
        int fd = ShmOpen(name, O_CREAT | O_RDWR, 0777, __func__);
        Ftruncate(fd, state.size, __func__);
        shm = (char*) Mmap(NULL, state.size, PROT_READ | PROT_WRITE, MAP_SHARED,
                           fd, 0, __func__);
        Fclose(fd, __func__);
    }

    ParseRangeQueryResult(result, shm);
    if (state.size > 0)
    {
        Munmap(shm, state.size, __func__);
    }

    KVMessage sendmsg = SimpleSuccessMessage(msg.hdr.resChan);
    sendmsg.hdr.bdySize = sizeof(state);
    sendmsg.bdy = &state;
    sendmsg.writeFunc = WriteReadBatchState;

    channel_->Send(sendmsg);
}

void
KVWorker::ClearRangeQuery(KVWorkerId const& workerId, KVMessage& msg)
{
    char buf[MSGBUFSIZE];

    msg.bdy = buf;
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    key.pid = *((pid_t*) buf);
    key.cursor = *((KVCursorId*) (buf + sizeof(key.pid)));

    std::unordered_map<KVCursorKey, KVRangeQueryEntry, KVCursorKeyHashFunc>::iterator it;
    it = ranges_.find(key);
    if (it == ranges_.end())
    {
        return;
    }

    ClearRangeQueryMeta(it->second.range, it->second.readOpts);
    ranges_.erase(it);

    char name[MAXPATHLENGTH];
    StringFormat(name, MAXPATHLENGTH, "%s%d%d%lu", RANGEQUERYPATH, key.pid,
                 workerId, key.cursor);
    ShmUnlink(name, __func__);
}
#endif

void
StartKVWorker(KVWorkerId workerId, KVDatabaseId dbId)
{
    KVWorker* worker = new KVWorker(workerId, dbId);
    KVManagerClient* manager = new KVManagerClient();

    worker->Start();
    /* notify ready event */
    manager->Notify(WorkerReady);

    worker->Run();

    /* notify destroyed event */
    manager->Notify(WorkerDesty);

    delete worker;
    delete manager;
}
