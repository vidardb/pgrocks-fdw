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
 * Implementation for kv message queue
 */

static char const *CRLCHANNEL = "Ctrl";
static char const *REQCHANNEL = "Request";
static char const *RESCHANNEL = "Response";

CircularQueueChannel::CircularQueueChannel(KVRelationId rid, const char* tag,
    bool create) : create(create)
{
    StringFormat(name, MAXPATHLENGTH, "%s%s%u", MSGPATHPREFIX, tag, rid);

    if (!create)
    {
        int fd = ShmOpen(name, O_RDWR, 0777, __func__);
        channel = (volatile CircularQueueData*) Mmap(NULL,
            sizeof(CircularQueueData), PROT_READ | PROT_WRITE, MAP_SHARED,
            fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    ShmUnlink(name, __func__);
    int fd = ShmOpen(name, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(CircularQueueData), __func__);
    channel = (volatile CircularQueueData*) Mmap(NULL,
        sizeof(CircularQueueData), PROT_READ | PROT_WRITE, MAP_SHARED,
        fd, 0, __func__);
    Fclose(fd, __func__);

    channel->getPos = channel->putPos = 0;
    SemInit(&channel->mutex, 1, 1, __func__);
    SemInit(&channel->empty, 1, 0, __func__);
    SemInit(&channel->full, 1, 0, __func__);
}

CircularQueueChannel::~CircularQueueChannel()
{
    if (!create)
    {
        Munmap((void*) channel, sizeof(*channel), __func__);
        return;
    }

    SemDestroy(&channel->mutex, __func__);
    SemDestroy(&channel->empty, __func__);
    SemDestroy(&channel->full, __func__);
    Munmap((void*) channel, sizeof(*channel), __func__);
    ShmUnlink(name, __func__);
}

void
CircularQueueChannel::read(uint64 *offset, char *str, uint64 size)
{
    if (size == 0)
    {
        return;
    }

    volatile char* getPos = channel->data + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (delta < size)
    {
        memcpy(str, (char*) getPos, delta);
        getPos = channel->data;
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
CircularQueueChannel::write(uint64 *offset, char *str, uint64 size)
{
    if (size == 0)
    {
        return;
    }

    volatile char* putPos = channel->data + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (size > delta)
    {
        memcpy((char*) putPos, str, delta);
        putPos = channel->data;
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

CtrlChannel::CtrlChannel(KVRelationId rid, const char* tag, bool create) :
    create(create)
{
    StringFormat(name, MAXPATHLENGTH, "%s%s%s%u", MSGPATHPREFIX, tag,
        CRLCHANNEL, rid);

    if (!create)
    {
        int fd = ShmOpen(name, O_RDWR, 0777, __func__);
        channel = (volatile CtrlData*) Mmap(NULL, sizeof(CtrlData),
            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    ShmUnlink(name, __func__);
    int fd = ShmOpen(name, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(CtrlData), __func__);
    channel = (volatile CtrlData*) Mmap(NULL, sizeof(CtrlData),
        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    SemInit(&channel->workerReady, 1, 0, __func__);
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        SemInit(&channel->responseMutex[i], 1, 1, __func__);
    }
}

CtrlChannel::~CtrlChannel()
{
    if (!create)
    {
        Munmap((void*) channel, sizeof(*channel), __func__);
        return;
    }

    SemDestroy(&channel->workerReady, __func__);
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        SemDestroy(&channel->responseMutex[i], __func__);
    }
    Munmap((void*) channel, sizeof(*channel), __func__);
    ShmUnlink(name, __func__);
}

void
CtrlChannel::wait(volatile sem_t* sem)
{
    SemWait(sem, __func__);
}

void
CtrlChannel::notify(volatile sem_t* sem)
{
    SemPost(sem, __func__);
}

uint32
CtrlChannel::leaseResponseQueue()
{
    while (true)
    {
        for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
        {
            int res = SemTryWait(&channel->responseMutex[i], __func__);
            if (res == 0)
            {
                return i + 1;
            }
        }
    }
}

void
CtrlChannel::unleaseResponseQueue(uint32 index)
{
    SemPost(&channel->responseMutex[index-1], __func__);
}

KVMessageQueue::KVMessageQueue(KVRelationId rid, const char* name,
    bool isServer) : isServer(isServer)
{
    char temp[MAXPATHLENGTH];

    ctrl = new CtrlChannel(rid, name, isServer);
    StringFormat(temp, MAXPATHLENGTH, "%s%s", name, REQCHANNEL);
    request = new CircularQueueChannel(rid, temp, isServer);
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        StringFormat(temp, MAXPATHLENGTH, "%s%s%d", name, RESCHANNEL, i);
        response[i] = new CircularQueueChannel(rid, temp, isServer);
    }
    running = true;
}

KVMessageQueue::~KVMessageQueue()
{
    delete request;
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        delete response[i];
    }
    delete ctrl;
}

static uint64
GetKVMessageSize(KVMessage const& msg)
{
    uint64 hdrSize = sizeof(msg.hdr);
    return hdrSize + msg.hdr.bdySize;
}

void
KVMessageQueue::send(KVMessage const& msg)
{
    CircularQueueChannel* queue = NULL;

    if (isServer)
    {
        if (msg.hdr.resChan == 0)
        {
            ErrorReport(WARNING, ERRCODE_WARNING, "invalid response channel");
            return;
        }
        
        queue = response[msg.hdr.resChan - 1];
    }
    else
    {
        queue = request;
    }

    uint64 size = GetKVMessageSize(msg);

    while (true)
    {
        uint64 empty = 0;

        SemWait(&queue->channel->mutex, __func__);
        if (queue->channel->getPos > queue->channel->putPos)
        {
            empty = queue->channel->getPos - queue->channel->putPos;
        }
        else
        {
            empty = MSGBUFSIZE - (queue->channel->putPos -
                queue->channel->getPos);
        }
        SemPost(&queue->channel->mutex, __func__);

        /*
         * reserve an empty slot to avoid that the putPos offset is equal to
         * the getPos offset when the buf is full, namely, the putPos offset
         * will never catch up with the getPos offset
         */
        if (empty < size + 1)
        {
            SemWait(&queue->channel->full, __func__);
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

    uint64 offset = queue->channel->putPos; /* current write position */
    queue->write(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    if (msg.writeFunc)
    {
        (*msg.writeFunc) (queue, &offset, msg.bdy, msg.hdr.bdySize);
    }

    SemWait(&queue->channel->mutex, __func__);
    queue->channel->putPos = offset;
    SemPost(&queue->channel->mutex, __func__);
    SemPost(&queue->channel->empty, __func__);
}

void
KVMessageQueue::sendWithResponse(KVMessage& sendmsg, KVMessage& recvmsg)
{
    uint32 chan = leaseResponseQueue();

    sendmsg.hdr.resChan = chan;
    recvmsg.hdr.resChan = chan;

    send(sendmsg);
    recv(recvmsg);

    unleaseResponseQueue(chan);
}

void
KVMessageQueue::recv(KVMessage& msg)
{
    recv(msg, MSGHEADER | MSGENTITY);
}

void
KVMessageQueue::recv(KVMessage& msg, int flag)
{
    CircularQueueChannel* queue = NULL;

    if (isServer)
    {
        queue = request;
    }
    else
    {
        if (msg.hdr.resChan == 0)
        {
            ErrorReport(WARNING, ERRCODE_WARNING, "invalid response channel");
            return;
        }
        
        queue = response[msg.hdr.resChan - 1];
    }

    while (true)
    {
        if (!running)
        {
            return;
        }

        SemWait(&queue->channel->mutex, __func__);
        uint64 delta = queue->channel->putPos - queue->channel->getPos;
        SemPost(&queue->channel->mutex, __func__);

        if (delta == 0)
        {
            SemWait(&queue->channel->empty, __func__);
        }
        else
        {
            break;
        }
    }

    uint64 offset = queue->channel->getPos; /* current read position */
    if (flag & MSGHEADER)
    {
        queue->read(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    }
    if ((flag & MSGENTITY) && msg.readFunc)
    {
        (*msg.readFunc) (queue, &offset, msg.bdy, msg.hdr.bdySize);
    }

    SemWait(&queue->channel->mutex, __func__);
    queue->channel->getPos = offset;
    SemPost(&queue->channel->mutex, __func__);
    SemPost(&queue->channel->full, __func__);
}

void
KVMessageQueue::wait(CtrlType type)
{
    switch (type)
    {
        case WorkerReady:
            ctrl->wait(&ctrl->channel->workerReady);
            break;
    }
}

void
KVMessageQueue::notify(CtrlType type)
{
    switch (type)
    {
        case WorkerReady:
            ctrl->notify(&ctrl->channel->workerReady);
            break;
    }
}

void
KVMessageQueue::terminate()
{
    running = false;

    SemPost(&request->channel->empty, __func__);
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++)
    {
        SemPost(&response[i]->channel->empty, __func__);
    }
}

uint32
KVMessageQueue::leaseResponseQueue()
{
    return ctrl->leaseResponseQueue();
}

void
KVMessageQueue::unleaseResponseQueue(uint32 index)
{
    ctrl->unleaseResponseQueue(index);
}

/*
 * Implementation for kv message
 */

KVMessage
SimpleSuccessMessage()
{
    KVMessage msg;
    msg.hdr.status = KVStatusSuccess;
    return msg;
}

KVMessage
SimpleFailureMessage()
{
    KVMessage msg;
    msg.hdr.status = KVStatusFailure;
    return msg;
}

KVMessage
SimpleSuccessMessageWithChannel(uint32 channel)
{
    KVMessage msg = SimpleSuccessMessage();
    msg.hdr.resChan = channel;
    return msg;
}

KVMessage
SimpleFailureMessageWithChannel(uint32 channel)
{
    KVMessage msg = SimpleFailureMessage();
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

KVMessage
SimpleMessageWithEntity(KVOperation op, KVRelationId rid, KVDatabaseId dbId,
    void* entity, uint64 size)
{
    return SimpleMessageWithEntity(op, rid, dbId, entity, size,
        CommonReadMessage, CommonWriteMessage);
}

KVMessage
SimpleMessageWithEntity(KVOperation op, KVRelationId rid, KVDatabaseId dbId,
    void* entity, uint64 size, ReadMessageFunc readFunc,
    WriteMessageFunc writeFunc)
{
    KVMessage msg = SimpleMessage(op, rid, dbId);
    msg.hdr.bdySize = size;
    msg.bdy = entity;
    msg.readFunc = readFunc;
    msg.writeFunc = writeFunc;
    return msg;
}

void
CommonWriteMessage(CircularQueueChannel* channel, uint64* offset, void* entity,
    uint64 size)
{
    channel->write(offset, (char*) entity, size);
}

void
CommonReadMessage(CircularQueueChannel* channel, uint64* offset, void* entity,
    uint64 size)
{
    channel->read(offset, (char*) entity, size);
}
