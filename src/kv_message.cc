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

#include <fcntl.h>

#include "kv_db.h"
#include "kv_posix.h"

/*
 * Implementation for kv circular queue channel
 */

static char const *CRLCHANNEL = "Ctrl";
static char const *REQCHANNEL = "Request";
static char const *RESCHANNEL = "Response";

static uint64
GetKVMessageSize(KVMessage const& msg)
{
    uint64 hdrSize = sizeof(msg.hdr);
    return hdrSize + msg.hdr.bdySize;
}

KVCircularQueueChannel::KVCircularQueueChannel(KVRelationId rid, const char* tag,
    bool create) : create_(create), running_(true)
{
    StringFormat(name_, MAXPATHLENGTH, "%s%s%u", MSGPATHPREFIX, tag, rid);

    if (!create)
    {
        int fd = ShmOpen(name_, O_RDWR, 0777, __func__);
        channel_ = (volatile KVCircularQueueData*) Mmap(NULL,
            sizeof(KVCircularQueueData), PROT_READ | PROT_WRITE, MAP_SHARED,
            fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    ShmUnlink(name_, __func__);
    int fd = ShmOpen(name_, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(KVCircularQueueData), __func__);
    channel_ = (volatile KVCircularQueueData*) Mmap(NULL,
        sizeof(KVCircularQueueData), PROT_READ | PROT_WRITE, MAP_SHARED,
        fd, 0, __func__);
    Fclose(fd, __func__);

    channel_->getPos = channel_->putPos = 0;
    SemInit(&channel_->mutex, 1, 1, __func__);
    SemInit(&channel_->empty, 1, 0, __func__);
    SemInit(&channel_->full, 1, 0, __func__);
}

KVCircularQueueChannel::~KVCircularQueueChannel()
{
    if (!create_)
    {
        Munmap((void*) channel_, sizeof(*channel_), __func__);
        return;
    }

    SemDestroy(&channel_->mutex, __func__);
    SemDestroy(&channel_->empty, __func__);
    SemDestroy(&channel_->full, __func__);
    Munmap((void*) channel_, sizeof(*channel_), __func__);
    ShmUnlink(name_, __func__);
}

void
KVCircularQueueChannel::Send(KVMessage const& msg)
{
    uint64 size = GetKVMessageSize(msg);

    while (true)
    {
        uint64 empty = 0;

        SemWait(&channel_->mutex, __func__);
        if (channel_->getPos > channel_->putPos)
        {
            empty = channel_->getPos - channel_->putPos;
        }
        else
        {
            empty = MSGBUFSIZE - (channel_->putPos - channel_->getPos);
        }
        SemPost(&channel_->mutex, __func__);

        /*
         * reserve an empty slot to avoid that the putPos offset is equal to
         * the getPos offset when the buf is full, namely, the putPos offset
         * will never catch up with the getPos offset
         */
        if (empty < size + 1)
        {
            SemWait(&channel_->full, __func__);
            /*
             * maybe the empty slots is still not enough even if has read
             * some small tuples, so re-check is necessary
             */
        }
        else
        {
            break; /* enough */
        }
    }

    uint64 offset = channel_->putPos; /* current write position */
    Write(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    if (msg.writeFunc)
    {
        (*msg.writeFunc) (this, &offset, msg.bdy, msg.hdr.bdySize);
    }

    SemWait(&channel_->mutex, __func__);
    channel_->putPos = offset;
    SemPost(&channel_->mutex, __func__);
    SemPost(&channel_->empty, __func__);
}

void
KVCircularQueueChannel::Recv(KVMessage& msg, int flag)
{
    if (flag & MSGDISCARD)
    {
        SemPost(&channel_->full, __func__);
        return;
    }

    while (true)
    {
        if (!running_)
        {
            return;
        }

        SemWait(&channel_->mutex, __func__);
        uint64 delta = channel_->putPos - channel_->getPos;
        SemPost(&channel_->mutex, __func__);

        if (delta == 0)
        {
            SemWait(&channel_->empty, __func__);
        }
        else
        {
            break;
        }
    }

    uint64 offset = channel_->getPos; /* current read position */
    if (flag & MSGHEADER)
    {
        Read(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    }
    if ((flag & MSGENTITY) && msg.readFunc)
    {
        (*msg.readFunc) (this, &offset, msg.bdy, msg.hdr.bdySize);
    }

    SemWait(&channel_->mutex, __func__);
    channel_->getPos = offset;
    SemPost(&channel_->mutex, __func__);

    if (flag & MSGENTITY)
    {
        SemPost(&channel_->full, __func__);
    }
}

void
KVCircularQueueChannel::Terminate()
{
    running_ = false;

    SemPost(&channel_->empty, __func__);
}

void
KVCircularQueueChannel::Read(uint64 *offset, char *str, uint64 size)
{
    if (size == 0)
    {
        return;
    }

    volatile char* getPos = channel_->data + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (delta < size)
    {
        memcpy(str, (char*) getPos, delta);
        getPos = channel_->data;
        *offset = 0;
        str += delta;

        memcpy(str, (char*) getPos, size - delta);
        (*offset) += size - delta;
    }
    else
    {
        memcpy(str, (char*) getPos, size);
        (*offset) += size;

        if (*offset == MSGBUFSIZE)
        {
            *offset = 0;
        }
    }
}

void
KVCircularQueueChannel::Write(uint64 *offset, char *str, uint64 size)
{
    if (size == 0)
    {
        return;
    }

    volatile char* putPos = channel_->data + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (size > delta)
    {
        memcpy((char*) putPos, str, delta);
        putPos = channel_->data;
        *offset = 0;
        str += delta;

        memcpy((char*) putPos, str, size - delta);
        (*offset) += size - delta;
    }
    else
    {
        memcpy((char*) putPos, str, size);
        (*offset) += size;

        if (*offset == MSGBUFSIZE)
        {
            *offset = 0;
        }
    }
}

/*
 * Implementation for kv simple queue channel
 */

KVSimpleQueueChannel::KVSimpleQueueChannel(KVRelationId rid, const char* tag,
    bool create) : create_(create)
{
    StringFormat(name_, MAXPATHLENGTH, "%s%s%u", MSGPATHPREFIX, tag, rid);

    if (!create)
    {
        int fd = ShmOpen(name_, O_RDWR, 0777, __func__);
        channel_ = (volatile KVSimpleQueueData*) Mmap(NULL,
            sizeof(KVSimpleQueueData), PROT_READ | PROT_WRITE, MAP_SHARED,
            fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    ShmUnlink(name_, __func__);
    int fd = ShmOpen(name_, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(KVSimpleQueueData), __func__);
    channel_ = (volatile KVSimpleQueueData*) Mmap(NULL,
        sizeof(KVSimpleQueueData), PROT_READ | PROT_WRITE, MAP_SHARED,
        fd, 0, __func__);
    Fclose(fd, __func__);

    SemInit(&channel_->mutex, 1, 1, __func__);
    SemInit(&channel_->ready, 1, 0, __func__);
}

KVSimpleQueueChannel::~KVSimpleQueueChannel()
{
    if (!create_)
    {
        Munmap((void*) channel_, sizeof(*channel_), __func__);
        return;
    }

    SemDestroy(&channel_->mutex, __func__);
    SemDestroy(&channel_->ready, __func__);
    Munmap((void*) channel_, sizeof(*channel_), __func__);
    ShmUnlink(name_, __func__);
}

void
KVSimpleQueueChannel::Send(KVMessage const& msg)
{
    uint64 offset = 0; /* from start position */

    Write(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));

    if (msg.writeFunc)
    {
        (*msg.writeFunc) (this, &offset, msg.bdy, msg.hdr.bdySize);
    }

    SemPost(&channel_->ready, __func__);
}

void
KVSimpleQueueChannel::Recv(KVMessage& msg, int flag)
{
    uint64 offset = 0; /* from start position */

    if (flag & MSGHEADER)
    {
        SemWait(&channel_->ready, __func__);

        Read(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));

        channel_->getPos = offset;
    }

    if ((flag & MSGENTITY) && msg.readFunc)
    {
        offset = channel_->getPos;

        (*msg.readFunc) (this, &offset, msg.bdy, msg.hdr.bdySize);
    }
}

void
KVSimpleQueueChannel::Read(uint64 *offset, char *str, uint64 size)
{
    if (size == 0)
    {
        return;
    }

    volatile char* getPos = channel_->data + *offset;
    memcpy(str, (char*) getPos, size);
    (*offset) += size;
}

void
KVSimpleQueueChannel::Write(uint64 *offset, char *str, uint64 size)
{
    if (size == 0)
    {
        return;
    }

    volatile char* putPos = channel_->data + *offset;
    memcpy((char*) putPos, str, size);
    (*offset) += size;
}

bool
KVSimpleQueueChannel::Lease()
{
    return SemTryWait(&channel_->mutex, __func__) == 0;
}

void
KVSimpleQueueChannel::Unlease()
{
    SemPost(&channel_->mutex, __func__);
}

/*
 * Implementation for kv ctrl queue channel
 */

KVCtrlChannel::KVCtrlChannel(KVRelationId rid, const char* tag, bool create) :
    create_(create)
{
    StringFormat(name_, MAXPATHLENGTH, "%s%s%s%u", MSGPATHPREFIX, tag,
        CRLCHANNEL, rid);

    if (!create)
    {
        int fd = ShmOpen(name_, O_RDWR, 0777, __func__);
        channel_ = (volatile KVCtrlData*) Mmap(NULL, sizeof(KVCtrlData),
            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    ShmUnlink(name_, __func__);
    int fd = ShmOpen(name_, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(KVCtrlData), __func__);
    channel_ = (volatile KVCtrlData*) Mmap(NULL, sizeof(KVCtrlData),
        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    SemInit(&channel_->workerReady, 1, 0, __func__);
    SemInit(&channel_->workerDesty, 1, 0, __func__);
}

KVCtrlChannel::~KVCtrlChannel()
{
    if (!create_)
    {
        Munmap((void*) channel_, sizeof(*channel_), __func__);
        return;
    }

    SemDestroy(&channel_->workerReady, __func__);
    SemDestroy(&channel_->workerDesty, __func__);
    Munmap((void*) channel_, sizeof(*channel_), __func__);
    ShmUnlink(name_, __func__);
}

void
KVCtrlChannel::Wait(KVCtrlType type)
{
    switch (type)
    {
        case WorkerReady:
            SemWait(&channel_->workerReady, __func__);
            break;
        case WorkerDesty:
            SemWait(&channel_->workerDesty, __func__);
            break;
    }
}

void
KVCtrlChannel::Notify(KVCtrlType type)
{
    switch (type)
    {
        case WorkerReady:
            SemPost(&channel_->workerReady, __func__);
            break;
        case WorkerDesty:
            SemPost(&channel_->workerDesty, __func__);
            break;
    }
}

/*
 * Implementation for kv message queue
 */

KVMessageQueue::KVMessageQueue(KVRelationId rid, const char* name,
    bool isServer) : isServer_(isServer)
{
    char temp[MAXPATHLENGTH];

    ctrl_ = new KVCtrlChannel(rid, name, isServer);
    StringFormat(temp, MAXPATHLENGTH, "%s%s", name, REQCHANNEL);
    request_ = new KVCircularQueueChannel(rid, temp, isServer);
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        StringFormat(temp, MAXPATHLENGTH, "%s%s%d", name, RESCHANNEL, i);
        response_[i] = new KVSimpleQueueChannel(rid, temp, isServer);
    }
}

KVMessageQueue::~KVMessageQueue()
{
    delete request_;

    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        delete response_[i];
    }

    delete ctrl_;
}

void
KVMessageQueue::Send(KVMessage const& msg)
{
    KVChannel* channel = NULL;

    if (isServer_)
    {
        if (msg.hdr.resChan == 0)
        {
            ErrorReport(WARNING, ERRCODE_WARNING, "invalid response channel");
            return;
        }
        
        channel = response_[msg.hdr.resChan - 1];
    }
    else
    {
        channel = request_;
    }

    channel->Send(msg);
}

void
KVMessageQueue::SendWithResponse(KVMessage& sendmsg, KVMessage& recvmsg)
{
    uint32 chan = LeaseResponseQueue();

    sendmsg.hdr.resChan = chan;
    recvmsg.hdr.resChan = chan;

    Send(sendmsg);
    Recv(recvmsg);

    UnleaseResponseQueue(chan);
}

void
KVMessageQueue::Recv(KVMessage& msg, int flag)
{
    KVChannel* channel = NULL;

    if (isServer_)
    {
        channel = request_;
    }
    else
    {
        if (msg.hdr.resChan == 0)
        {
            ErrorReport(WARNING, ERRCODE_WARNING, "invalid response channel");
            return;
        }
        
        channel = response_[msg.hdr.resChan - 1];
    }

    channel->Recv(msg, flag);
}

void
KVMessageQueue::Terminate()
{
    request_->Terminate();

    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        response_[i]->Terminate();
    }
}

uint32
KVMessageQueue::LeaseResponseQueue()
{
    while (true)
    {
        for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
        {
            if (response_[i]->Lease())
            {
                return i + 1;
            }
        }
    }
}

void
KVMessageQueue::UnleaseResponseQueue(uint32 index)
{
    response_[index-1]->Unlease();
}

void 
KVMessageQueue::Wait(KVCtrlType type)
{
    ctrl_->Wait(type);
}

void
KVMessageQueue::Notify(KVCtrlType type)
{
    ctrl_->Notify(type);
}

/*
 * Implementation for kv message
 */

KVMessage
SimpleSuccessMessage(uint32 channel)
{
    KVMessage msg;
    msg.hdr.status = KVStatusSuccess;
    msg.hdr.resChan = channel;
    return msg;
}

KVMessage
SimpleFailureMessage(uint32 channel)
{
    KVMessage msg;
    msg.hdr.status = KVStatusFailure;
    msg.hdr.resChan = channel;
    return msg;
}

KVMessage
SimpleMessage(KVOperation op, KVRelationId rid, KVDatabaseId dbId)
{
    KVMessage msg;
    msg.hdr.op = op;
    msg.hdr.relId = rid;
    msg.hdr.dbId = dbId;
    return msg;
}

void
CommonWriteEntity(void* channel, uint64* offset, void* entity, uint64 size)
{
    ((KVChannel*) channel)->Write(offset, (char*) entity, size);
}

void
CommonReadEntity(void* channel, uint64* offset, void* entity, uint64 size)
{
    ((KVChannel*) channel)->Read(offset, (char*) entity, size);
}
