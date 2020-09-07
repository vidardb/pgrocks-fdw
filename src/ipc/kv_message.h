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

#ifndef KV_MESSAGE_H_
#define KV_MESSAGE_H_

#include "../kv_api.h"


/*
 * A kv message contains both header and entity (optional), and it also
 * provides two entity operation hook functions which we can customize
 * the message entity read (receive) and write (send) method. Otherwise,
 * one can also use the default implemented <CommonWriteEntity> and
 * <CommonReadEntity> to satisfy your common scenario.
 */

typedef enum {
    KVStatusDummy = 0, /* placeholder */
    KVStatusSuccess,
    KVStatusFailure,
    KVStatusException,
} KVMessageStatus;

struct KVMessageHeader {
    KVOperation     op      = KVOpDummy;
    KVDatabaseId    dbId    = InvalidOid;
    KVRelationId    relId   = InvalidOid;
    KVMessageStatus status  = KVStatusDummy;
    uint32          resChan = 0; /* response channel id */
    uint64          etySize = 0; /* message entity size */
};

typedef void (*WriteEntityFunc) (void* channel, uint64* offset, void* entity,
                                 uint64 size);
typedef void (*ReadEntityFunc)  (void* channel, uint64* offset, void* entity,
                                 uint64 size);

struct KVMessage {
    KVMessageHeader  hdr;              /* message header */
    void*            ety       = NULL; /* message entity */

    ReadEntityFunc   readFunc  = NULL; /* read function */
    WriteEntityFunc  writeFunc = NULL; /* write function */
};

extern KVMessage SuccessMessage(uint32 channel);
extern KVMessage FailureMessage(uint32 channel);
extern KVMessage SimpleMessage(KVOperation op, KVRelationId rid,
                               KVDatabaseId dbId);

extern void CommonWriteEntity(void* channel, uint64* offset, void* entity,
                              uint64 size);
extern void CommonReadEntity(void* channel, uint64* offset, void* entity,
                             uint64 size);

#endif  /* KV_MESSAGE_H_ */