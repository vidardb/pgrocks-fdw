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

#include "kv_worker.h"
#include "kv_manager.h"
#include "../ipc/kv_posix.h"
#include "kv_storage.h"
#include "fcntl.h"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
}


#define READBATCHPATH     "/KVReadBatch"
#ifdef VIDARDB
#define RANGEQUERYPATH    "/KVRangeQuery"
#endif

static const char* WORKER = "Worker";

/*
 * Implementation for kv worker client
 */

KVWorkerClient::KVWorkerClient(KVWorkerId workerId) {
    channel_ = new KVMessageQueue(workerId, WORKER, false);
}

KVWorkerClient::~KVWorkerClient() {
    delete channel_;
}

void WriteOpenArgs(void* channel, uint64* offset, void* entity, uint64 size) {
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

void ReadOpenArgs(void* channel, uint64* offset, void* entity, uint64 size) {
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

void KVWorkerClient::Open(KVWorkerId workerId, OpenArgs* args) {
    uint64 size = sizeof(args->opts) + strlen(args->path);
    #ifdef VIDARDB
    size += (sizeof(args->useColumn) + sizeof(args->attrCount));
    #endif

    KVMessage sendmsg = SimpleMessage(KVOpOpen, workerId, MyDatabaseId);
    sendmsg.ety = args;
    sendmsg.hdr.etySize = size;
    sendmsg.writeFunc = WriteOpenArgs;

    channel_->Send(sendmsg);
}

void WritePutArgs(void* channel, uint64* offset, void* entity, uint64 size) {
    PutArgs* args = (PutArgs*) entity;

    ((KVChannel*) channel)->Write(offset, (char*) &args->keyLen,
                                  sizeof(args->keyLen));
    ((KVChannel*) channel)->Write(offset, args->key, args->keyLen);
    ((KVChannel*) channel)->Write(offset, args->val, args->valLen);
}

bool KVWorkerClient::Put(KVWorkerId workerId, PutArgs* args) {
    uint64 size = args->keyLen + args->valLen + sizeof(args->keyLen);

    KVMessage sendmsg = SimpleMessage(KVOpPut, workerId, MyDatabaseId);
    sendmsg.ety = args;
    sendmsg.hdr.etySize = size;
    sendmsg.writeFunc = WritePutArgs;

    KVMessage recvmsg;
    channel_->SendWithResponse(sendmsg, recvmsg);

    return recvmsg.hdr.status == KVStatusSuccess;
}

bool KVWorkerClient::Delete(KVWorkerId workerId, DeleteArgs* args) {
    KVMessage sendmsg = SimpleMessage(KVOpDel, workerId, MyDatabaseId);
    sendmsg.ety = args->key;
    sendmsg.hdr.etySize = args->keyLen;
    sendmsg.writeFunc = CommonWriteEntity;

    KVMessage recvmsg;
    channel_->SendWithResponse(sendmsg, recvmsg);

    return recvmsg.hdr.status == KVStatusSuccess;
}

void KVWorkerClient::Load(KVWorkerId workerId, PutArgs* args) {
    uint64 size = args->keyLen + args->valLen + sizeof(args->keyLen);

    KVMessage sendmsg = SimpleMessage(KVOpLoad, workerId, MyDatabaseId);
    sendmsg.ety = args;
    sendmsg.hdr.etySize = size;
    sendmsg.writeFunc = WritePutArgs;

    channel_->Send(sendmsg);
}

bool KVWorkerClient::Get(KVWorkerId workerId, GetArgs* args) {
    KVMessage sendmsg = SimpleMessage(KVOpGet, workerId, MyDatabaseId);
    sendmsg.ety = args->key;
    sendmsg.hdr.etySize = args->keyLen;
    sendmsg.writeFunc = CommonWriteEntity;

    KVMessage recvmsg;
    uint32 chan = channel_->LeaseResponseQueue();
    sendmsg.hdr.resChan = chan;
    recvmsg.hdr.resChan = chan;
    channel_->Send(sendmsg);
    channel_->Recv(recvmsg, MSGHEADER);

    *(args->valLen) = recvmsg.hdr.etySize;
    *(args->val) = (char*) palloc0(*(args->valLen));
    recvmsg.ety = *(args->val);
    recvmsg.readFunc = CommonReadEntity;
    channel_->Recv(recvmsg, MSGENTITY);
    channel_->UnleaseResponseQueue(chan);

    return recvmsg.hdr.status == KVStatusSuccess;
}

void KVWorkerClient::Close(KVWorkerId workerId) {
    channel_->Send(SimpleMessage(KVOpClose, workerId, MyDatabaseId));
}

uint64 KVWorkerClient::Count(KVWorkerId workerId) {
    uint64 count;

    KVMessage recvmsg;
    recvmsg.ety = &count;
    recvmsg.hdr.etySize = sizeof(count);
    recvmsg.readFunc = CommonReadEntity;

    KVMessage sendmsg = SimpleMessage(KVOpCount, workerId, MyDatabaseId);
    channel_->SendWithResponse(sendmsg, recvmsg);

    return count;
}

void KVWorkerClient::Terminate(KVWorkerId workerId) {
    channel_->Send(SimpleMessage(KVOpTerminate, workerId, MyDatabaseId));
}

void WriteReadBatchArgs(void* channel, uint64* offset, void* entity, uint64 size) {
    ReadBatchArgs* args = (ReadBatchArgs*) entity;

    pid_t pid = getpid();
    ((KVChannel*) channel)->Write(offset, (char*) &pid, sizeof(pid_t));
    ((KVChannel*) channel)->Write(offset, (char*) &args->cursor,
                                  sizeof(args->cursor));
}

bool KVWorkerClient::ReadBatch(KVWorkerId workerId, ReadBatchArgs* args) {
    if (*(args->buf) != NULL) {
        Munmap(*(args->buf), READBATCHSIZE, __func__);
    }

    KVMessage sendmsg = SimpleMessage(KVOpReadBatch, workerId, MyDatabaseId);
    sendmsg.ety = args;
    sendmsg.hdr.etySize = sizeof(pid_t) + sizeof(args->cursor);
    sendmsg.writeFunc = WriteReadBatchArgs;

    char buf[sizeof(bool) + sizeof(uint64)];
    KVMessage recvmsg;
    recvmsg.ety = buf;
    recvmsg.readFunc = CommonReadEntity;
    channel_->SendWithResponse(sendmsg, recvmsg);

    if (recvmsg.hdr.status != KVStatusSuccess) {
        return false;
    }

    bool next = *((bool*) buf);
    *(args->bufLen) = *((uint64*) (buf + sizeof(next)));
    if (*(args->bufLen) == 0) {
        *(args->buf) = NULL;
    } else {
        char  name[MAXPATHLENGTH];
        pid_t pid = getpid();

        snprintf(name, MAXPATHLENGTH, "%s%d%d%lu", READBATCHPATH, pid, workerId,
                 args->cursor);
        int fd = ShmOpen(name, O_RDWR, 0777, __func__);
        *(args->buf) = (char*) Mmap(NULL, READBATCHSIZE, PROT_READ | PROT_WRITE,
                                    MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
    }

    return next;
}

void WriteDelCursorArgs(void* channel, uint64* offset, void* entity, uint64 size) {
    CloseCursorArgs* args = (CloseCursorArgs*) entity;

    pid_t pid = getpid();
    ((KVChannel*) channel)->Write(offset, (char*) &pid, sizeof(pid_t));
    ((KVChannel*) channel)->Write(offset, (char*) &args->cursor,
                                  sizeof(args->cursor));
}

void KVWorkerClient::CloseCursor(KVWorkerId workerId, CloseCursorArgs* args) {
    if (args->buf) {
        Munmap(args->buf, READBATCHSIZE, __func__);
    }

    char  name[MAXPATHLENGTH];
    pid_t pid = getpid();

    snprintf(name, MAXPATHLENGTH, "%s%d%d%lu", READBATCHPATH, pid, workerId,
             args->cursor);
    ShmUnlink(name, __func__);

    KVMessage sendmsg = SimpleMessage(KVOpDelCursor, workerId, MyDatabaseId);
    sendmsg.ety = args;
    sendmsg.hdr.etySize = sizeof(pid_t) + sizeof(args->cursor);
    sendmsg.writeFunc = WriteDelCursorArgs;

    channel_->Send(sendmsg);
}

#ifdef VIDARDB
void WriteRangeQueryArgs(void* channel, uint64* offset, void* entity,
                         uint64 size) {
    RangeQueryArgs* args = (RangeQueryArgs*) entity;
    RangeQueryOpts* opts = args->opts;

    pid_t pid = getpid();
    ((KVChannel*) channel)->Write(offset, (char*) &pid, sizeof(pid_t));
    ((KVChannel*) channel)->Write(offset, (char*) &args->cursor,
                                  sizeof(args->cursor));

    if (opts) {
        ((KVChannel*) channel)->Write(offset, (char*) &(opts->startLen),
                       sizeof(opts->startLen));
        if (opts->startLen > 0) {
            ((KVChannel*) channel)->Write(offset, opts->start, opts->startLen);
        }

        ((KVChannel*) channel)->Write(offset, (char*) &(opts->limitLen),
                       sizeof(opts->limitLen));
        if (opts->limitLen > 0) {
            ((KVChannel*) channel)->Write(offset, opts->limit, opts->limitLen);
        }

        ((KVChannel*) channel)->Write(offset, (char*) &opts->batchCapacity,
                       sizeof(opts->batchCapacity));
        ((KVChannel*) channel)->Write(offset, (char*) &opts->attrCount,
                       sizeof(opts->attrCount));
        if (opts->attrCount > 0) {
            ((KVChannel*) channel)->Write(offset, (char*) opts->attrs,
                           opts->attrCount * sizeof(*(opts->attrs)));
        }
    }
}

bool KVWorkerClient::RangeQuery(KVWorkerId workerId, RangeQueryArgs* args) {
    if (*(args->buf) && *(args->bufLen) > 0) {
        Munmap(*(args->buf), *(args->bufLen), __func__);
    }

    KVMessage sendmsg = SimpleMessage(KVOpRangeQuery, workerId, MyDatabaseId);
    sendmsg.ety = args;
    sendmsg.hdr.etySize = sizeof(pid_t) + sizeof(args->cursor);
    if (args->opts) {
        sendmsg.hdr.etySize += sizeof(args->opts->startLen);
        sendmsg.hdr.etySize += args->opts->startLen;
        sendmsg.hdr.etySize += sizeof(args->opts->limitLen);
        sendmsg.hdr.etySize += args->opts->limitLen;
        sendmsg.hdr.etySize += sizeof(args->opts->attrCount);
        sendmsg.hdr.etySize += args->opts->attrCount *
                               sizeof(*(args->opts->attrs));
        sendmsg.hdr.etySize += sizeof(args->opts->batchCapacity);
    }
    sendmsg.writeFunc = WriteRangeQueryArgs;

    char buf[sizeof(bool) + sizeof(uint64)];
    KVMessage recvmsg;
    recvmsg.ety = buf;
    recvmsg.readFunc = CommonReadEntity;
    channel_->SendWithResponse(sendmsg, recvmsg);

    if (recvmsg.hdr.status != KVStatusSuccess) {
        return false;
    }

    bool next = *((bool*) buf);
    *(args->bufLen) = *((uint64*) (buf + sizeof(next)));
    if (*(args->bufLen) == 0) {
        *(args->buf) = NULL;
    } else {
        char  name[MAXPATHLENGTH];
        pid_t pid = getpid();

        snprintf(name, MAXPATHLENGTH, "%s%d%d%lu", RANGEQUERYPATH, pid,
                 workerId, args->cursor);
        int fd = ShmOpen(name, O_RDWR, 0777, __func__);
        *(args->buf) = (char*) Mmap(NULL, *(args->bufLen),
            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
    }

    return next;
}

void KVWorkerClient::ClearRangeQuery(KVWorkerId workerId, RangeQueryArgs* args) {
    if (*(args->buf) && *(args->bufLen) > 0) {
        Munmap(*(args->buf), *(args->bufLen), __func__);
    }

    KVMessage sendmsg = SimpleMessage(KVOpClearRangeQuery, workerId, MyDatabaseId);
    args->opts = NULL;
    sendmsg.ety = args;
    sendmsg.hdr.etySize = sizeof(pid_t) + sizeof(args->cursor);
    sendmsg.writeFunc = WriteRangeQueryArgs;

    channel_->Send(sendmsg);
}
#endif

/*
 * Implementation for kv worker
 */

KVWorker::KVWorker(KVWorkerId workerId, KVDatabaseId dbId) :
    running_(false), conn_(NULL), ref_(0) {

    channel_ = new KVMessageQueue(workerId, WORKER, true);

    cursors_.clear();
    #ifdef VIDARDB
    ranges_.clear();
    #endif
}

KVWorker::~KVWorker() {
    if (conn_) {
        CloseConn(conn_);
        conn_ = NULL;
    }
    delete channel_;
}

void KVWorker::Start() {
    running_ = true;
}

void KVWorker::Run() {
    while (running_) {
        KVMessage msg;
        channel_->Recv(msg, MSGHEADER);

        switch (msg.hdr.op) {
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
                ereport(WARNING, (errmsg("invalid operation: %d", msg.hdr.op)));
        }
    }
}

void KVWorker::Stop() {
    running_ = false;
    channel_->Terminate();
}

void KVWorker::Open(KVWorkerId workerId, KVMessage& msg) {
    OpenArgs args;
    args.path = (char*) palloc0(msg.hdr.etySize);

    msg.ety = &args;
    msg.readFunc = ReadOpenArgs;
    channel_->Recv(msg, MSGENTITY);

    if (conn_) {
        ref_++;
    } else {
        #ifdef VIDARDB
        conn_ = OpenConn(args.path, args.useColumn, args.attrCount, &args.opts);
        #else
        conn_ = OpenConn(args.path, &args.opts);
        #endif
        ref_++;
    }

    pfree(args.path);
}

void KVWorker::Put(KVWorkerId workerId, KVMessage& msg) {
    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    PutArgs args;
    args.keyLen = *((uint64*) msg.ety);
    args.valLen = msg.hdr.etySize - args.keyLen - sizeof(args.keyLen);
    args.key = (char*) msg.ety + sizeof(args.keyLen);
    args.val = (char*) msg.ety + sizeof(args.keyLen) + args.keyLen;

    bool success = PutRecord(conn_, args.key, args.keyLen, args.val, args.valLen);
    if (success) {
        channel_->Send(SuccessMessage(msg.hdr.resChan));
    } else {
        channel_->Send(FailureMessage(msg.hdr.resChan));
    }

    pfree(msg.ety);
}

void KVWorker::Delete(KVWorkerId workerId, KVMessage& msg) {
    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    bool success = DelRecord(conn_, (char*) msg.ety, msg.hdr.etySize);
    if (success) {
        channel_->Send(SuccessMessage(msg.hdr.resChan));
    } else {
        channel_->Send(FailureMessage(msg.hdr.resChan));
    }

    pfree(msg.ety);
}

void KVWorker::Load(KVWorkerId workerId, KVMessage& msg) {
    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    PutArgs args;
    args.keyLen = *((uint64*) msg.ety);
    args.valLen = msg.hdr.etySize - args.keyLen - sizeof(args.keyLen);
    args.key = (char*) msg.ety + sizeof(args.keyLen);
    args.val = (char*) msg.ety + sizeof(args.keyLen) + args.keyLen;

    PutRecord(conn_, args.key, args.keyLen, args.val, args.valLen);

    pfree(msg.ety);
}

void KVWorker::Get(KVWorkerId workerId, KVMessage& msg) {
    char*  val;
    uint64 valLen;

    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    bool success = GetRecord(conn_, (char*) msg.ety, msg.hdr.etySize, &val, &valLen);
    if (success) {
        KVMessage sendmsg = SuccessMessage(msg.hdr.resChan);
        sendmsg.hdr.etySize = valLen;
        sendmsg.ety = val;
        sendmsg.writeFunc = CommonWriteEntity;
        channel_->Send(sendmsg);
    } else {
        channel_->Send(FailureMessage(msg.hdr.resChan));
    }

    pfree(msg.ety);
}

void KVWorker::Close(KVWorkerId workerId, KVMessage& msg) {
    channel_->Recv(msg, MSGDISCARD);

    if (conn_) {
        ref_--;
    }
}

void KVWorker::Count(KVWorkerId workerId, KVMessage& msg) {
    channel_->Recv(msg, MSGDISCARD);

    uint64 count = GetCount(conn_);

    KVMessage sendmsg;
    sendmsg.ety = &count;
    sendmsg.hdr.etySize = sizeof(count);
    sendmsg.hdr.resChan = msg.hdr.resChan;
    sendmsg.writeFunc = CommonWriteEntity;

    channel_->Send(sendmsg);
}

void KVWorker::Terminate(KVWorkerId workerId, KVMessage& msg) {
    channel_->Recv(msg, MSGDISCARD);
    Stop();
}

struct ReadBatchState {
    bool   next;
    uint64 size;
};

void WriteReadBatchState(void* channel, uint64* offset, void* entity,
                         uint64 size) {
    ReadBatchState* state = (ReadBatchState*) entity;

    ((KVChannel*) channel)->Write(offset, (char*) &state->next,
                                  sizeof(state->next));
    ((KVChannel*) channel)->Write(offset, (char*) &state->size,
                                  sizeof(state->size));
}

void KVWorker::ReadBatch(KVWorkerId workerId, KVMessage& msg) {
    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    key.pid = *((pid_t*) msg.ety);
    key.cursor = *((KVCursorId*) ((char*) msg.ety + sizeof(key.pid)));

    void* cursor = NULL;
    std::unordered_map<KVCursorKey, void*, KVCursorKeyHashFunc>::iterator it;
    it = cursors_.find(key);
    if (it == cursors_.end()) {
        cursor = GetIter(conn_);
        cursors_.insert({key, cursor});
    } else {
        cursor = it->second;
    }

    char name[MAXPATHLENGTH];
    snprintf(name, MAXPATHLENGTH, "%s%d%d%lu", READBATCHPATH, key.pid, workerId,
             key.cursor);
    ShmUnlink(name, __func__);
    int fd = ShmOpen(name, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, READBATCHSIZE, __func__);
    char* shm = (char*) Mmap(NULL, READBATCHSIZE, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    ReadBatchState state;
    state.next = BatchRead(conn_, cursor, shm, &state.size);

    KVMessage sendmsg = SuccessMessage(msg.hdr.resChan);
    sendmsg.hdr.etySize = sizeof(state);
    sendmsg.ety = &state;
    sendmsg.writeFunc = WriteReadBatchState;

    channel_->Send(sendmsg);

    pfree(msg.ety);
}

void KVWorker::CloseCursor(KVWorkerId workerId, KVMessage& msg) {
    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    key.pid = *((pid_t*) msg.ety);
    key.cursor = *((KVCursorId*) ((char*) msg.ety + sizeof(key.pid)));

    std::unordered_map<KVCursorKey, void*, KVCursorKeyHashFunc>::iterator it;
    it = cursors_.find(key);
    if (it == cursors_.end()) {
        pfree(msg.ety);
        return;
    }

    DelIter(it->second);
    cursors_.erase(it);
    pfree(msg.ety);
}

#ifdef VIDARDB
void KVWorker::RangeQuery(KVWorkerId workerId, KVMessage& msg) {
    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    char* current = (char*) msg.ety;
    key.pid = *((pid_t*) current);
    current += sizeof(key.pid);
    key.cursor = *((KVCursorId*) current);
    current += sizeof(key.cursor);

    KVRangeQueryEntry entry;
    std::unordered_map<KVCursorKey, KVRangeQueryEntry, KVCursorKeyHashFunc>::iterator it;
    it = ranges_.find(key);
    if (it == ranges_.end()) {
        RangeQueryOpts opts;

        opts.startLen = *((uint64*) current);
        current += sizeof(opts.startLen);

        if (opts.startLen > 0) {
            opts.start = (char*) palloc0(opts.startLen);
            memcpy(opts.start, current, opts.startLen);
            current += opts.startLen;
        }

        opts.limitLen = *((uint64*) current);
        current += sizeof(opts.limitLen);

        if (opts.limitLen > 0) {
            opts.limit = (char*) palloc0(opts.limitLen);
            memcpy(opts.limit, current, opts.limitLen);
            current += opts.limitLen;
        }

        opts.batchCapacity = *((uint64*) current);
        current += sizeof(opts.batchCapacity);

        opts.attrCount = *((int*) current);
        current += sizeof(opts.attrCount);

        if (opts.attrCount > 0) {
            opts.attrs = (AttrNumber*) current;
        }

        ParseRangeQueryOptions(&opts, &entry.range, &entry.readOpts);
        ranges_.insert({key, entry});
    } else {
        entry = it->second;
    }

    void* result = NULL;
    ReadBatchState state;

    do {
        state.next = RangeQueryRead(conn_, entry.range, &entry.readOpts,
                                    &state.size, &result);
    } while (state.next && state.size == 0);

    char name[MAXPATHLENGTH];
    snprintf(name, MAXPATHLENGTH, "%s%d%d%lu", RANGEQUERYPATH, key.pid,
             workerId, key.cursor);
    ShmUnlink(name, __func__);

    char* shm = NULL;
    if (state.size > 0) {
        int fd = ShmOpen(name, O_CREAT | O_RDWR, 0777, __func__);
        Ftruncate(fd, state.size, __func__);
        shm = (char*) Mmap(NULL, state.size, PROT_READ | PROT_WRITE, MAP_SHARED,
                           fd, 0, __func__);
        Fclose(fd, __func__);
    }

    ParseRangeQueryResult(result, shm);
    if (state.size > 0) {
        Munmap(shm, state.size, __func__);
    }

    KVMessage sendmsg = SuccessMessage(msg.hdr.resChan);
    sendmsg.hdr.etySize = sizeof(state);
    sendmsg.ety = &state;
    sendmsg.writeFunc = WriteReadBatchState;

    channel_->Send(sendmsg);

    pfree(msg.ety);
}

void KVWorker::ClearRangeQuery(KVWorkerId workerId, KVMessage& msg) {
    msg.ety = palloc0(msg.hdr.etySize);
    msg.readFunc = CommonReadEntity;
    channel_->Recv(msg, MSGENTITY);

    KVCursorKey key;
    key.pid = *((pid_t*) msg.ety);
    key.cursor = *((KVCursorId*) ((char*) msg.ety + sizeof(key.pid)));

    std::unordered_map<KVCursorKey, KVRangeQueryEntry, KVCursorKeyHashFunc>::iterator it;
    it = ranges_.find(key);
    if (it == ranges_.end()) {
        pfree(msg.ety);
        return;
    }

    ClearRangeQueryMeta(it->second.range, it->second.readOpts);
    ranges_.erase(it);

    char name[MAXPATHLENGTH];
    snprintf(name, MAXPATHLENGTH, "%s%d%d%lu", RANGEQUERYPATH, key.pid,
             workerId, key.cursor);
    ShmUnlink(name, __func__);
    pfree(msg.ety);
}
#endif

static void StartKVWorker(KVWorkerId workerId, KVDatabaseId dbId) {
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

/*
 * Entrypoint for kv worker
 */
extern "C" void KVWorkerMain(Datum arg) {
    KVDatabaseId dbId = (KVDatabaseId) DatumGetObjectId(arg);
    KVWorkerId workerId = *((KVWorkerId*) (MyBgworkerEntry->bgw_extra));

    /* Connect to our database */
    BackgroundWorkerInitializeConnectionByOid(dbId, InvalidOid, 0);

    /* Start kv worker */
    StartKVWorker(workerId, dbId);
}

/*
 * Launch kv worker process
 */
void* LaunchKVWorker(KVWorkerId workerId, KVDatabaseId dbId) {
    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "kv_fdw");
    sprintf(worker.bgw_function_name, "KVWorkerMain");
    snprintf(worker.bgw_name, BGW_MAXLEN, "KV Worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "KV Worker");
    worker.bgw_main_arg = ObjectIdGetDatum(dbId);
    memcpy(worker.bgw_extra, &workerId, sizeof(workerId));
    /* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
    worker.bgw_notify_pid = MyProcPid;

    BackgroundWorkerHandle* handle;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        return NULL;
    }

    pid_t pid;
    BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status == BGWH_STOPPED) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("could not start background process"),
                errhint("More details may be available in the server log.")));
    }
    if (status == BGWH_POSTMASTER_DIED) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("cannot start background processes without postmaster"),
                errhint("Kill all remaining database processes and restart "
                        "the database.")));
    }
    Assert(status == BGWH_STARTED);

    return handle;
}
