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

#include "kv_mq.h"

extern "C" {
#include "postgres.h"
}


#define MSGREQCHANNELNAME "Request"
#define MSGRESCHANNELNAME "Response"
#define MSGCRLCHANNELNAME "Ctrl"


KVMessageQueue::KVMessageQueue(KVRelationId rid, const char* name,
                               bool isServer) {
    isServer_ = isServer;

    char tmp[MAXPATHLENGTH];
    snprintf(tmp, MAXPATHLENGTH, "%s%s", name, MSGCRLCHANNELNAME);
    ctrl_ = new KVCtrlChannel(rid, tmp, isServer);

    snprintf(tmp, MAXPATHLENGTH, "%s%s", name, MSGREQCHANNELNAME);
    request_ = new KVCircularChannel(rid, tmp, isServer);

    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
        snprintf(tmp, MAXPATHLENGTH, "%s%s%d", name, MSGRESCHANNELNAME, i);
        response_[i] = new KVSimpleChannel(rid, tmp, isServer);
    }
}

KVMessageQueue::~KVMessageQueue() {
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
        delete response_[i];
    }
    delete request_;
    delete ctrl_;
}

void KVMessageQueue::Send(const KVMessage& msg) {
    KVChannel* channel = nullptr;

    if (isServer_) {
        if (msg.hdr.rpsId == 0) {
            ereport(WARNING, errmsg("invalid response channel"));
            return;
        }

        channel = response_[msg.hdr.rpsId - 1];
    } else {
        channel = request_;
    }

    channel->Input(msg);
}

void KVMessageQueue::Recv(KVMessage& msg, int flag) {
    KVChannel* channel = nullptr;

    if (isServer_) {
        channel = request_;
    } else {
        if (msg.hdr.rpsId == 0) {
            ereport(WARNING, errmsg("invalid response channel"));
            return;
        }

        channel = response_[msg.hdr.rpsId - 1];
    }

    channel->Output(msg, flag);
}

uint32 KVMessageQueue::LeaseResponseChannel() {
    while (true) {
        for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
            if (response_[i]->Lease()) {
                return i + 1;
            }
        }
    }
}

void KVMessageQueue::UnleaseResponseChannel(uint32 index) {
    response_[index-1]->Unlease();
}

void KVMessageQueue::SendWithResponse(KVMessage& sendmsg, KVMessage& recvmsg) {
    uint32 channel = LeaseResponseChannel();

    sendmsg.hdr.rpsId = recvmsg.hdr.rpsId = channel;

    Send(sendmsg);
    Recv(recvmsg);

    UnleaseResponseChannel(channel);
}

void KVMessageQueue::Wait(KVCtrlType type) {
    ctrl_->Wait(type);
}

void KVMessageQueue::Notify(KVCtrlType type) {
    ctrl_->Notify(type);
}

void KVMessageQueue::Stop() {
    request_->Stop();

    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
        response_[i]->Stop();
    }
}
