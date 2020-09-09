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

#ifndef KV_CHANNEL_H_
#define KV_CHANNEL_H_


#include "kv_message.h"
#include "kv_posix.h"


#define MAXPATHLENGTH 64
#define MSGHEADER     01
#define MSGENTITY     02
#define MSGDISCARD    04
#define MSGBUFSIZE    65536


/*
 * A kv channel abstract class which defines some kv message process
 * functions, and all the kv channel subclass implementation should
 * inherit the definition.
 */

class KVChannel {
  public:
    virtual ~KVChannel() {}
    virtual void Send(const KVMessage& msg) = 0;
    virtual void Recv(KVMessage& msg) { Recv(msg, MSGHEADER | MSGENTITY); }
    virtual void Recv(KVMessage& msg, int flag) = 0;
    virtual void Read(uint64* offset, char* str, uint64 size) = 0;
    virtual void Write(uint64* offset, char* str, uint64 size) = 0;
    virtual void Terminate() = 0;
};

/*
 * A kv circular channel which utilizes the ring buffer algorithm to implement
 * the kv message's concurrent read and write mechanism.
 *
 * It is primarily used as the request channel to receive kv messages in the
 * following <KVMessageQueue> definition.
 */

typedef struct KVCircularChannelData {
    uint64 putPos;           /* the position producer can put data */
    uint64 getPos;           /* the position consumer can get data */
    sem_t  mutex;            /* mutual exclusion for position */
    sem_t  empty;            /* tell whether the data buf is empty */
    sem_t  full;             /* tell whether the data buf is full */
    char   data[MSGBUFSIZE]; /* assume ~64K for a tuple is enough */
} KVCircularChannelData;

class KVCircularChannel : public KVChannel {
  public:
    KVCircularChannel(KVRelationId rid, const char* tag, bool create);
    ~KVCircularChannel();

    void Send(const KVMessage& msg);
    void Recv(KVMessage& msg, int flag);
    void Read(uint64* offset, char* str, uint64 size);
    void Write(uint64* offset, char* str, uint64 size);
    void Terminate();

  private:
    char name_[MAXPATHLENGTH];
    bool create_; /* instruct whether to create */
    volatile bool running_;
    volatile KVCircularChannelData* channel_;
};

/*
 * A kv simple channel which implements the kv message's mutual exclusive
 * read and write mechanism.
 *
 * It is primarily used as the response channel to send kv messages in the
 * following <KVMessageQueue> definition.
 */

typedef struct KVSimpleChannelData {
    uint64 getPos;           /* the position consumer can get data */
    sem_t  mutex;            /* mutual exclusion for response */
    sem_t  ready;            /* tell whether response is ready */
    char   data[MSGBUFSIZE]; /* assume ~64K for a tuple is enough */
} KVSimpleChannelData;

class KVSimpleChannel : public KVChannel {
  public:
    KVSimpleChannel(KVRelationId rid, const char* tag, bool create);
    ~KVSimpleChannel();

    void Send(const KVMessage& msg);
    void Recv(KVMessage& msg, int flag);
    void Read(uint64* offset, char* str, uint64 size);
    void Write(uint64* offset, char* str, uint64 size);
    void Terminate() {};
    bool Lease();
    void Unlease();

  private:
    char name_[MAXPATHLENGTH];
    bool create_;
    volatile KVSimpleChannelData* channel_;
};

/*
 * A kv control channel which defines some semaphores to coordinate kv manager
 * and kv worker processes.
 *
 * It is primarily used as the control channel in the following <KVMessageQueue>
 * definition.
 */

typedef enum {
    WorkerReady = 0,
    WorkerDesty,
} KVCtrlType;

typedef struct KVCtrlData {
    sem_t workerReady; /* tell whether kv worker is ready */
    sem_t workerDesty; /* tell whether kv worker is destroyed */
} KVCtrlData;

class KVCtrlChannel {
  public:
    KVCtrlChannel(KVRelationId rid, const char* tag, bool create);
    ~KVCtrlChannel();

    void Wait(KVCtrlType type);
    void Notify(KVCtrlType type);

  private:
    char name_[MAXPATHLENGTH];
    bool create_;
    volatile KVCtrlData* channel_;
};

#endif  /* KV_CHANNEL_H_ */
