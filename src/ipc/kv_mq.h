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

#ifndef SRC_KV_MQ_H_
#define SRC_KV_MQ_H_


#include "kv_channel.h"


#define MSGRESQUEUELENGTH 2


/*
 * A message queue which exchanges messages between different processes as a
 * media. Currently it contains four channels: a circular channel as a
 * message receiver, two simple channels as message senders, and a control
 * channel as a coordinator.
 */

class KVMessageQueue {
  public:
    KVMessageQueue(KVRelationId rid, const char* name, bool isServer);
    ~KVMessageQueue();

    void   Send(const KVMessage& msg);
    void   Recv(KVMessage& msg) { Recv(msg, MSGHEADER | MSGENTITY); };
    void   Recv(KVMessage& msg, int flag);
    void   SendWithResponse(KVMessage& sendmsg, KVMessage& recvmsg);
    void   Wait(KVCtrlType type);
    void   Notify(KVCtrlType type);
    void   UnleaseResponseQueue(uint32 index);
    uint32 LeaseResponseQueue();
    void   Terminate();

  private:
    KVCtrlChannel* ctrl_;
    KVCircularChannel* request_;
    KVSimpleChannel* response_[MSGRESQUEUELENGTH];
    volatile bool isServer_;
};


#endif /* SRC_KV_MQ_H_ */
