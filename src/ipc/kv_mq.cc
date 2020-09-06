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


static char const *REQCHANNEL = "Request";
static char const *RESCHANNEL = "Response";


KVMessageQueue::KVMessageQueue(KVRelationId rid, const char* name, bool isServer) {
    isServer_ = isServer;
    char temp[MAXPATHLENGTH];

    ctrl_ = new KVCtrlChannel(rid, name, isServer);
    StringFormat(temp, MAXPATHLENGTH, "%s%s", name, REQCHANNEL);
    request_ = new KVCircularChannel(rid, temp, isServer);
    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
        StringFormat(temp, MAXPATHLENGTH, "%s%s%d", name, RESCHANNEL, i);
        response_[i] = new KVSimpleChannel(rid, temp, isServer);
    }
}

KVMessageQueue::~KVMessageQueue() {
    delete request_;

    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
        delete response_[i];
    }

    delete ctrl_;
}

void KVMessageQueue::Send(const KVMessage& msg) {
    KVChannel* channel = NULL;

    if (isServer_) {
        if (msg.hdr.resChan == 0) {
            ErrorReport(WARNING, ERRCODE_WARNING, "invalid response channel");
            return;
        }

        channel = response_[msg.hdr.resChan - 1];
    } else {
        channel = request_;
    }

    channel->Send(msg);
}

void KVMessageQueue::SendWithResponse(KVMessage& sendmsg, KVMessage& recvmsg) {
    uint32 chan = LeaseResponseQueue();

    sendmsg.hdr.resChan = chan;
    recvmsg.hdr.resChan = chan;

    Send(sendmsg);
    Recv(recvmsg);

    UnleaseResponseQueue(chan);
}

void KVMessageQueue::Recv(KVMessage& msg, int flag) {
    KVChannel* channel = NULL;

    if (isServer_) {
        channel = request_;
    } else {
        if (msg.hdr.resChan == 0) {
            ErrorReport(WARNING, ERRCODE_WARNING, "invalid response channel");
            return;
        }

        channel = response_[msg.hdr.resChan - 1];
    }

    channel->Recv(msg, flag);
}

void KVMessageQueue::Terminate() {
    request_->Terminate();

    for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
        response_[i]->Terminate();
    }
}

uint32 KVMessageQueue::LeaseResponseQueue() {
    while (true) {
        for (uint32 i = 0; i < MSGRESQUEUELENGTH; i++) {
            if (response_[i]->Lease()) {
                return i + 1;
            }
        }
    }
}

void KVMessageQueue::UnleaseResponseQueue(uint32 index) {
    response_[index-1]->Unlease();
}

void KVMessageQueue::Wait(KVCtrlType type) {
    ctrl_->Wait(type);
}

void KVMessageQueue::Notify(KVCtrlType type) {
    ctrl_->Notify(type);
}
