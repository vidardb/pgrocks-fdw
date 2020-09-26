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

#ifndef KV_MQ_H_
#define KV_MQ_H_


#include "kv_channel.h"


#define MSGRESQUEUELENGTH 2


/*
 * A kv message queue which exchanges messages between different processes as a
 * media. Currently it contains four channels: a circular channel (from client
 * to server), two simple channels (from server to client), and a control
 * channel as a coordinator.
 * Both client and server will use this message queue, so meaning of send and
 * recv will depend on who calls it.
 */
class KVMessageQueue {
  public:
    KVMessageQueue(KVRelationId rid, const char* name, bool isServer);
    ~KVMessageQueue();

    void   Send(const KVMessage& msg);
    void   Recv(KVMessage& msg) { Recv(msg, MSGHEADER | MSGENTITY); };
    void   Recv(KVMessage& msg, int flag);
    uint32 LeaseResponseChannel();
    void   UnleaseResponseChannel(uint32 index);
    void   SendWithResponse(KVMessage& sendmsg, KVMessage& recvmsg);
    void   Wait(KVCtrlType type);
    void   Notify(KVCtrlType type);
    void   Stop();  /* stop to recv kv msg */

  private:
    KVCtrlChannel* ctrl_;
    KVCircularChannel* request_;
    KVSimpleChannel* response_[MSGRESQUEUELENGTH];
    volatile bool isServer_;
};

#endif  /* KV_MQ_H_ */
