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

#include "kv_channel.h"
#include <fcntl.h>

extern "C" {
#include "postgres.h"
}


#define MSGPATHPREFIX "/KV"


/*
 * Implementation for kv circular channel
 */

KVCircularChannel::KVCircularChannel(KVRelationId rid, const char* tag,
                                     bool create) {
    create_ = create;
    running_ = true;
    snprintf(name_, MAXPATHLENGTH, "%s%s%u", MSGPATHPREFIX, tag, rid);

    if (!create_) { /* connect to the channel */
        int fd = ShmOpen(name_, O_RDWR, 0777, __func__);
        data_ = (volatile KVCircularChannelData*) Mmap(NULL, sizeof(*data_),
            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    /* create the channel */
    ShmUnlink(name_, __func__);
    int fd = ShmOpen(name_, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(*data_), __func__);
    data_ = (volatile KVCircularChannelData*) Mmap(NULL, sizeof(*data_),
        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    data_->getPos = data_->putPos = 0;
    SemInit(&data_->posMutex, 1, 1, __func__);
    SemInit(&data_->putMutex, 1, 1, __func__);
    SemInit(&data_->empty, 1, 0, __func__);
    SemInit(&data_->full, 1, 0, __func__);
}

KVCircularChannel::~KVCircularChannel() {
    if (!create_) {
        Munmap((void*) data_, sizeof(*data_), __func__);
        return;
    }

    SemDestroy(&data_->posMutex, __func__);
    SemDestroy(&data_->putMutex, __func__);
    SemDestroy(&data_->empty, __func__);
    SemDestroy(&data_->full, __func__);
    Munmap((void*) data_, sizeof(*data_), __func__);
    ShmUnlink(name_, __func__);
}

uint64 KVCircularChannel::GetKVMessageSize(const KVMessage& msg) {
    uint64 hdrSize = sizeof(msg.hdr);
    return hdrSize + msg.hdr.etySize;
}

void KVCircularChannel::Input(const KVMessage& msg) {
    uint64 size = GetKVMessageSize(msg);
    SemWait(&data_->putMutex, __func__);

    while (true) {
        uint64 empty = 0;

        SemWait(&data_->posMutex, __func__);
        if (data_->getPos > data_->putPos) {
            empty = data_->getPos - data_->putPos;
        } else {
            empty = MSGBUFSIZE - (data_->putPos - data_->getPos);
        }
        SemPost(&data_->posMutex, __func__);

        /*
         * reserve an empty slot to avoid that the putPos offset is equal to
         * the getPos offset when the buf is full, namely, the putPos offset
         * will never catch up with the getPos offset
         */
        if (empty < size + 1) {
            SemWait(&data_->full, __func__);
            /*
             * maybe the empty slots is still not enough even if has read
             * some small tuples, so re-check is necessary
             */
        } else {
            break; /* enough */
        }
    }

    uint64 offset = data_->putPos; /* current write position */
    Push(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    if (msg.writeFunc) {
        (*msg.writeFunc) (this, &offset, msg.ety, msg.hdr.etySize);
    }

    SemWait(&data_->posMutex, __func__);
    data_->putPos = offset;
    SemPost(&data_->posMutex, __func__);
    SemPost(&data_->empty, __func__);
    SemPost(&data_->putMutex, __func__);
}

void KVCircularChannel::Output(KVMessage& msg, int flag) {
    if (flag & MSGDISCARD) {
        SemPost(&data_->full, __func__);
        return;
    }

    while (true) {
        SemWait(&data_->posMutex, __func__);
        uint64 delta = data_->putPos - data_->getPos;
        SemPost(&data_->posMutex, __func__);

        if (delta == 0) { /* empty */
            if (!running_) {
                return;
            }
            SemWait(&data_->empty, __func__);
        } else {          /* at least one tuple */
            break;
        }
    }

    uint64 offset = data_->getPos; /* current read position */
    if (flag & MSGHEADER) {
        Pop(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    }
    if ((flag & MSGENTITY) && msg.readFunc) {
        (*msg.readFunc) (this, &offset, msg.ety, msg.hdr.etySize);
    }

    SemWait(&data_->posMutex, __func__);
    data_->getPos = offset;
    SemPost(&data_->posMutex, __func__);

    if (flag & MSGENTITY) {
        SemPost(&data_->full, __func__);
    }
}

void KVCircularChannel::Push(uint64* offset, char* str, uint64 size) {
    if (size == 0) {
        return;
    }

    volatile char* putPos = data_->buf + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (size > delta) { /* circular write into the channel */
        memcpy((char*) putPos, str, delta);
        putPos = data_->buf;
        *offset = 0;
        str += delta;

        memcpy((char*) putPos, str, size - delta);
        *offset = size - delta;
    } else {
        memcpy((char*) putPos, str, size);
        *offset += size;

        if (*offset == MSGBUFSIZE) {
            *offset = 0;
        }
    }
}

void KVCircularChannel::Pop(uint64* offset, char* str, uint64 size) {
    if (size == 0) {
        return;
    }

    volatile char* getPos = data_->buf + *offset;
    uint64 delta = MSGBUFSIZE - *offset;

    if (delta < size) { /* circular read out from the channel */
        memcpy(str, (char*) getPos, delta);
        getPos = data_->buf;
        *offset = 0;
        str += delta;

        memcpy(str, (char*) getPos, size - delta);
        *offset = size - delta;
    } else {
        memcpy(str, (char*) getPos, size);
        *offset += size;

        if (*offset == MSGBUFSIZE) {
            *offset = 0;
        }
    }
}

void KVCircularChannel::Terminate() {
    running_ = false;
    SemPost(&data_->empty, __func__);
}

/*
 * Implementation for kv simple channel
 */

KVSimpleChannel::KVSimpleChannel(KVRelationId rid, const char* tag, bool create) {
    create_ = create;
    snprintf(name_, MAXPATHLENGTH, "%s%s%u", MSGPATHPREFIX, tag, rid);

    if (!create) { /* connect to the channel */
        int fd = ShmOpen(name_, O_RDWR, 0777, __func__);
        data_ = (volatile KVSimpleChannelData*) Mmap(NULL, sizeof(*data_),
            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    /* create the channel */
    ShmUnlink(name_, __func__);
    int fd = ShmOpen(name_, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(KVSimpleChannelData), __func__);
    data_ = (volatile KVSimpleChannelData*) Mmap(NULL, sizeof(*data_),
        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    data_->getPos = 0;
    SemInit(&data_->mutex, 1, 1, __func__);
    SemInit(&data_->ready, 1, 0, __func__);
}

KVSimpleChannel::~KVSimpleChannel() {
    if (!create_) {
        Munmap((void*) data_, sizeof(*data_), __func__);
        return;
    }

    SemDestroy(&data_->mutex, __func__);
    SemDestroy(&data_->ready, __func__);
    Munmap((void*) data_, sizeof(*data_), __func__);
    ShmUnlink(name_, __func__);
}

void KVSimpleChannel::Input(const KVMessage& msg) {
    uint64 offset = 0; /* from start position */

    Push(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
    if (msg.writeFunc) {
        (*msg.writeFunc) (this, &offset, msg.ety, msg.hdr.etySize);
    }

    SemPost(&data_->ready, __func__);
}

void KVSimpleChannel::Output(KVMessage& msg, int flag) {
    uint64 offset = 0; /* from start position */

    if (flag & MSGHEADER) {
        SemWait(&data_->ready, __func__);
        Pop(&offset, (char*) &(msg.hdr), sizeof(msg.hdr));
        data_->getPos = offset;
    }

    if ((flag & MSGENTITY) && msg.readFunc) {
        offset = data_->getPos;
        (*msg.readFunc) (this, &offset, msg.ety, msg.hdr.etySize);
    }
}

void KVSimpleChannel::Push(uint64* offset, char* str, uint64 size) {
    if (size == 0) {
        return;
    }

    volatile char* putPos = data_->buf + *offset;
    memcpy((char*) putPos, str, size);
    *offset += size;
}

void KVSimpleChannel::Pop(uint64* offset, char* str, uint64 size) {
    if (size == 0) {
        return;
    }

    volatile char* getPos = data_->buf + *offset;
    memcpy(str, (char*) getPos, size);
    *offset += size;
}

bool KVSimpleChannel::Lease() {
    return SemTryWait(&data_->mutex, __func__) == 0;
}

void KVSimpleChannel::Unlease() {
    SemPost(&data_->mutex, __func__);
}


/*
 * Implementation for kv ctrl channel
 */

KVCtrlChannel::KVCtrlChannel(KVRelationId rid, const char* tag, bool create) {
    create_ = create;
    snprintf(name_, MAXPATHLENGTH, "%s%s%u", MSGPATHPREFIX, tag, rid);

    if (!create) { /* connect to the channel */
        int fd = ShmOpen(name_, O_RDWR, 0777, __func__);
        data_ = (volatile KVCtrlData*) Mmap(NULL, sizeof(*data_),
            PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
        Fclose(fd, __func__);
        return;
    }

    /* create the channel */
    ShmUnlink(name_, __func__);
    int fd = ShmOpen(name_, O_CREAT | O_RDWR, 0777, __func__);
    Ftruncate(fd, sizeof(*data_), __func__);
    data_ = (volatile KVCtrlData*) Mmap(NULL, sizeof(*data_),
        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0, __func__);
    Fclose(fd, __func__);

    SemInit(&data_->workerReady, 1, 0, __func__);
    SemInit(&data_->workerDesty, 1, 0, __func__);
}

KVCtrlChannel::~KVCtrlChannel() {
    if (!create_) {
        Munmap((void*) data_, sizeof(*data_), __func__);
        return;
    }

    SemDestroy(&data_->workerReady, __func__);
    SemDestroy(&data_->workerDesty, __func__);
    Munmap((void*) data_, sizeof(*data_), __func__);
    ShmUnlink(name_, __func__);
}

void KVCtrlChannel::Wait(KVCtrlType type) {
    switch (type) {
        case WorkerReady:
            SemWait(&data_->workerReady, __func__);
            break;
        case WorkerDesty:
            SemWait(&data_->workerDesty, __func__);
            break;
    }
}

void KVCtrlChannel::Notify(KVCtrlType type) {
    switch (type) {
        case WorkerReady:
            SemPost(&data_->workerReady, __func__);
            break;
        case WorkerDesty:
            SemPost(&data_->workerDesty, __func__);
            break;
    }
}
