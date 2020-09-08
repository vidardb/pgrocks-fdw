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

#include "kv_api.h"


typedef enum {
    KVOpDummy = 0, /* placeholder */
    KVOpOpen,
    KVOpClose,
    KVOpCount,
    KVOpPut,
    KVOpGet,
    KVOpDel,
    KVOpLoad,
    KVOpReadBatch,
    KVOpDelCursor,
    #ifdef VIDARDB
    KVOpRangeQuery,
    KVOpClearRangeQuery,
    #endif
    KVOpLaunch,
    KVOpTerminate,
} KVOperation;

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

/*
 * Custom message entity read and write function definition
 */

typedef void (*WriteEntityFunc) (void* channel, uint64* offset, void* entity,
                                 uint64 size);
typedef void (*ReadEntityFunc)  (void* channel, uint64* offset, void* entity,
                                 uint64 size);

/*
 * A kv message contains both header and entity (optional), and it also
 * provides two entity operation hook functions which we can customize
 * the message entity read (receive) and write (send) method. Otherwise,
 * one can also use the default implemented <CommonWriteEntity> and
 * <CommonReadEntity> to satisfy your common scenario.
 */

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

/*
 * Common message entity read and write function
 */

extern void CommonWriteEntity(void* channel, uint64* offset, void* entity,
                              uint64 size);
extern void CommonReadEntity(void* channel, uint64* offset, void* entity,
                             uint64 size);

#endif  /* KV_MESSAGE_H_ */
