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

static char const *REQCHANNEL = "Request";
static char const *RESCHANNEL = "Response";

CircularQueueChannel::CircularQueueChannel(KVRelationId rid, const char* role,
    const char* type, bool create) : create(create)
{
    StringFormat(name, MAXPATHLENGTH, "%s%s%s%u", MSGPATHPREFIX, role, type, rid);

    if (!create)
    {
        int fd = ShmOpen(name, O_RDWR, 0777, __func__);
        channel = (CircularQueueData*) Mmap(NULL, sizeof(CircularQueueData),
            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    ShmUnlink(name, __func__);
    int fd = ShmOpen(name, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(CircularQueueData), __func__);
    channel = (CircularQueueData*) Mmap(NULL, sizeof(CircularQueueData),
        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    channel->getPos = channel->putPos = 0;
    SemInit(&channel->mutex, 1, 1, __func__);
    SemInit(&channel->empty, 1, 0, __func__);
    SemInit(&channel->full, 1, 0, __func__);
    SemInit(&channel->ready, 1, 0, __func__);
}

CircularQueueChannel::~CircularQueueChannel()
{
    if (!create)
    {
        Munmap(channel, sizeof(*channel), __func__);
        return;
    }

    SemDestroy(&channel->mutex, __func__);
    SemDestroy(&channel->empty, __func__);
    SemDestroy(&channel->full, __func__);
    Munmap(channel, sizeof(*channel), __func__);
    ShmUnlink(name, __func__);
}

void
CircularQueueChannel::read(uint64 *offset, char *str, uint64 size)
{
    if (size == 0)
    {
        return;
    }

    char* getPos = channel->data + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (delta < size)
    {
        memcpy(str, getPos, delta);
        getPos = channel->data;
        *offset = 0;
        str += delta;

        memcpy(str, getPos, size - delta);
        (*offset) += size - delta;
    }
    else
    {
        memcpy(str, getPos, size);
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

    char* putPos = channel->data + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (size > delta)
    {
        memcpy(putPos, str, delta);
        putPos = channel->data;
        *offset = 0;
        str += delta;

        memcpy(putPos, str, size - delta);
        (*offset) += size - delta;
    }
    else
    {
        memcpy(putPos, str, size);
        (*offset) += size;

        if (*offset == MSGBUFSIZE)
        {
            *offset = 0;
        }
    }
}

KVMessageQueue::KVMessageQueue(KVRelationId rid, const char* role, bool svrMode)
{
    if (svrMode)
    {
        request = new CircularQueueChannel(rid, role, REQCHANNEL, true);
        response = new CircularQueueChannel(rid, role, RESCHANNEL, true);
    }
    else
    {
        request = new CircularQueueChannel(rid, role, RESCHANNEL, false);
        response = new CircularQueueChannel(rid, role, REQCHANNEL, false);
    }
    running = true;
}

KVMessageQueue::~KVMessageQueue()
{
    delete request;
    delete response;
}

static uint64
GetKVMessageSize(KVMessage const& msg)
{
    uint64 hdrSize = sizeof(msg.hdr);
    return hdrSize + msg.hdr.bdySize;
}

void
KVMessageQueue::write(KVMessage const& msg)
{
    uint64 size = GetKVMessageSize(msg);

    while (true)
    {
        uint64 empty = 0;

        SemWait(&response->channel->mutex, __func__);
        if (response->channel->getPos > response->channel->putPos)
        {
            empty = response->channel->getPos - response->channel->putPos;
        }
        else
        {
            empty = MSGBUFSIZE - (response->channel->putPos -
                response->channel->getPos);
        }
        SemPost(&response->channel->mutex, __func__);

        /*
         * reserve an empty slot to avoid that the putPos offset is equal to
         * the getPos offset when the buf is full, namely, the putPos offset
         * will never catch up with the getPos offset
         */
        if (empty < size + 1)
        {
            SemWait(&response->channel->full, __func__);
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

    uint64 offset = response->channel->putPos; /* current write position */
    response->write(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    if (msg.writeFunc)
    {
        (*msg.writeFunc) (response, &offset, msg.bdy, msg.hdr.bdySize);
    }

    SemWait(&response->channel->mutex, __func__);
    response->channel->putPos = offset;
    SemPost(&response->channel->mutex, __func__);
    SemPost(&response->channel->empty, __func__);
}

void
KVMessageQueue::read(KVMessage& msg)
{
    read(msg, MSGHEADER | MSGBODY);
}

void
KVMessageQueue::read(KVMessage& msg, int flag)
{
    while (true)
    {
        if (!running)
        {
            return;
        }

        SemWait(&request->channel->mutex, __func__);
        uint64 delta = request->channel->putPos - request->channel->getPos;
        SemPost(&request->channel->mutex, __func__);

        if (delta == 0)
        {
            SemWait(&request->channel->empty, __func__);
        }
        else
        {
            break;
        }
    }

    uint64 offset = request->channel->getPos; /* current read position */
    if (flag & MSGHEADER)
    {
        request->read(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    }
    if ((flag & MSGBODY) && msg.readFunc)
    {
        (*msg.readFunc) (request, &offset, msg.bdy, msg.hdr.bdySize);
    }

    SemWait(&request->channel->mutex, __func__);
    request->channel->getPos = offset;
    SemPost(&request->channel->mutex, __func__);
    SemPost(&request->channel->full, __func__);
}

void
KVMessageQueue::interrupt()
{
    running = false;

    SemPost(&request->channel->empty, __func__);
}

void
KVMessageQueue::wait()
{
    SemWait(&request->channel->ready, __func__);
}

void
KVMessageQueue::ready()
{
    SemPost(&response->channel->ready, __func__);
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
