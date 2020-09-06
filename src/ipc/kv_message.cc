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

#include "kv_message.h"
#include "kv_channel.h"


KVMessage SuccessMessage(uint32 channel) {
    KVMessage msg;
    msg.hdr.status = KVStatusSuccess;
    msg.hdr.resChan = channel;
    return msg;
}

KVMessage FailureMessage(uint32 channel) {
    KVMessage msg;
    msg.hdr.status = KVStatusFailure;
    msg.hdr.resChan = channel;
    return msg;
}

KVMessage SimpleMessage(KVOperation op, KVRelationId rid, KVDatabaseId dbId) {
    KVMessage msg;
    msg.hdr.op = op;
    msg.hdr.relId = rid;
    msg.hdr.dbId = dbId;
    return msg;
}

void CommonWriteEntity(void* channel, uint64* offset, void* entity, uint64 size) {
    ((KVChannel*) channel)->Write(offset, (char*) entity, size);
}

void CommonReadEntity(void* channel, uint64* offset, void* entity, uint64 size) {
    ((KVChannel*) channel)->Read(offset, (char*) entity, size);
}
