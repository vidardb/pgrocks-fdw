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

#ifndef KV_DB_H_
#define KV_DB_H_

#include <unordered_map>
#include <semaphore.h>
#include "kv_api.h"

#define MSGBUFSIZE        65536
#define MSGPATHPREFIX     "/KV"
#define MSGHEADER         01
#define MSGENTITY         02
#define MSGDISCARD        04
#define MSGRESQUEUELENGTH 2
#define MAXPATHLENGTH     64
#define READBATCHSIZE     4096*20
#define READBATCHPATH     "/KVReadBatch"
#ifdef VIDARDB
#define RANGEQUERYPATH    "/KVRangeQuery"
#endif

/*
 * A kv message contains both header and entity (optional), and it also
 * provides two entity operation hook functions which we can customize
 * the message entity read (receive) and write (send) method. Otherwise,
 * you can also use the default implemented <CommonWriteEntity> and 
 * <CommonReadEntity> to satisfy your common scenario.
 */

typedef void (*WriteEntityFunc) (void* channel, uint64* offset, void* entity,
                                 uint64 size);
typedef void (*ReadEntityFunc)  (void* channel, uint64* offset, void* entity,
                                 uint64 size);

typedef enum
{
    KVStatusDummy = 0, /* placeholder */
    KVStatusSuccess,
    KVStatusFailure,
    KVStatusException,
} KVMessageStatus;

struct KVMessageHeader
{
    KVOperation     op      = KVOpDummy;
    KVDatabaseId    dbId    = InvalidOid;
    KVRelationId    relId   = InvalidOid;
    KVMessageStatus status  = KVStatusDummy;
    uint32          resChan = 0; /* response channel id */
    uint64          etySize = 0; /* message entity size */
};

struct KVMessage
{
    KVMessageHeader  hdr;              /* message header */
    void*            ety       = NULL; /* message entity */

    ReadEntityFunc   readFunc  = NULL; /* read function */
    WriteEntityFunc  writeFunc = NULL; /* write function */
};

/*
 * A kv channel abstract class which defines some kv message process
 * functions, and all the kv channel subclass implementation should
 * inherit the definition.
 */

class KVChannel
{
  public:
    virtual ~KVChannel() {};
    virtual void Send(const KVMessage& msg) = 0;
    virtual void Recv(KVMessage& msg) { Recv(msg, MSGHEADER | MSGENTITY); };
    virtual void Recv(KVMessage& msg, int flag) = 0;
    virtual void Read(uint64 *offset, char *str, uint64 size) = 0;
    virtual void Write(uint64 *offset, char *str, uint64 size) = 0;
    virtual void Terminate() = 0;
};

/*
 * A kv circular channel which utilizes the ring buffer algorithm to implement
 * the kv message's concurrent read and write mechanism.
 *
 * It is primarily used as the request channel to receive kv messages in the 
 * following <KVMessageQueue> definition.
 */

struct KVCircularChannelData
{
    uint64 putPos;           /* the position producer can put data */
    uint64 getPos;           /* the position consumer can get data */
    sem_t  mutex;            /* mutual exclusion for position */
    sem_t  empty;            /* tell wether the data buf is empty */
    sem_t  full;             /* tell wether the data buf is full */
    char   data[MSGBUFSIZE]; /* assume ~64K for a tuple is enough */
};

class KVCircularChannel : public KVChannel
{
  public:
    KVCircularChannel(KVRelationId rid, const char* tag, bool create);
    ~KVCircularChannel();

    void Send(const KVMessage& msg);
    void Recv(KVMessage& msg, int flag);
    void Read(uint64 *offset, char *str, uint64 size);
    void Write(uint64 *offset, char *str, uint64 size);
    void Terminate();

  private:
    char name_[MAXPATHLENGTH];
    bool create_;
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

struct KVSimpleChannelData
{
    uint64 getPos;           /* the position consumer can get data */
    sem_t  mutex;            /* mutual exclusion for response */
    sem_t  ready;            /* tell wether response is ready */
    char   data[MSGBUFSIZE]; /* assume ~64K for a tuple is enough */
};

class KVSimpleChannel : public KVChannel
{
  public:
    KVSimpleChannel(KVRelationId rid, const char* tag, bool create);
    ~KVSimpleChannel();

    void Send(const KVMessage& msg);
    void Recv(KVMessage& msg, int flag);
    void Read(uint64 *offset, char *str, uint64 size);
    void Write(uint64 *offset, char *str, uint64 size);
    void Terminate() {};
    bool Lease();
    void Unlease();

  private:
    char name_[MAXPATHLENGTH];
    bool create_;
    volatile KVSimpleChannelData* channel_;
};

/*
 * A kv control channel which defines some semphores to coordinate kv manager
 * and kv worker processes.
 * 
 * It is primarily used as the control channel in the following <KVMessageQueue>
 * definition.
 */

typedef enum
{
    WorkerReady = 0,
    WorkerDesty,
} KVCtrlType;

struct KVCtrlData
{
    sem_t workerReady; /* tell wether kv worker is ready */
    sem_t workerDesty; /* tell wether kv worker is destroyed */
};

class KVCtrlChannel
{
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

/*
 * A kv message queue which exchanges messages between different processes as a
 * intermediary. Currently it contains four channels: a circular channel as a
 * message receiver, two simple channels as message sender, and a control channel
 * as a coordinator.
 */

class KVMessageQueue
{
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

/*
 * A kv worker which is responsible for receiving kv messages from its
 * corresponding message queue and calling storage engine api to handle
 * the kv tuples.
 */

struct KVCursorKey
{
    pid_t      pid;     /* backend process pid */
    KVCursorId cursor;

    bool operator==(const KVCursorKey& key) const
    {
        return pid == key.pid && cursor == key.cursor;
    }
};

#ifdef VIDARDB
struct KVRangeQueryEntry
{
    void* readOpts = NULL;
    void* range    = NULL;
};
#endif

struct KVCursorKeyHashFunc
{
    size_t operator()(const KVCursorKey& key) const
    {
        return (std::hash<pid_t>()(key.pid)) ^
               (std::hash<KVCursorId>()(key.cursor));
    }
};

class KVWorker
{
  public:
    KVWorker(KVWorkerId workerId, KVDatabaseId dbId);
    ~KVWorker();

    void Start();
    void Run();
    void Stop();
    void Open(KVWorkerId workerId, KVMessage& msg);
    void Put(KVWorkerId workerId, KVMessage& msg);
    void Delete(KVWorkerId workerId, KVMessage& msg);
    void Load(KVWorkerId workerId, KVMessage& msg);
    void Get(KVWorkerId workerId, KVMessage& msg);
    void Close(KVWorkerId workerId, KVMessage& msg);
    void Count(KVWorkerId workerId, KVMessage& msg);
    void Terminate(KVWorkerId workerId, KVMessage& msg);
    void ReadBatch(KVWorkerId workerId, KVMessage& msg);
    void CloseCursor(KVWorkerId workerId, KVMessage& msg);
    #ifdef VIDARDB
    void RangeQuery(KVWorkerId workerId, KVMessage& msg);
    void ClearRangeQuery(KVWorkerId workerId, KVMessage& msg);
    #endif

  private:
    std::unordered_map<KVCursorKey, void*, KVCursorKeyHashFunc> cursors_;
    #ifdef VIDARDB
    std::unordered_map<KVCursorKey, KVRangeQueryEntry, KVCursorKeyHashFunc> ranges_;
    #endif
    KVMessageQueue* channel_;
    volatile bool running_;
    void* conn_;
    uint64 ref_;
};

/*
 * A kv worker client which as a stub to interact with its corresponding kv
 * worker process through message queue.
 */

class KVWorkerClient
{
  public:
    KVWorkerClient(KVWorkerId workerId);
    ~KVWorkerClient();

    void   Open(KVWorkerId workerId, OpenArgs* args);
    bool   Put(KVWorkerId workerId, PutArgs* args);
    bool   Delete(KVWorkerId workerId, DeleteArgs* args);
    void   Load(KVWorkerId workerId, PutArgs* args);
    bool   Get(KVWorkerId workerId, GetArgs* args);
    void   Close(KVWorkerId workerId);
    void   Terminate(KVWorkerId workerId);
    bool   ReadBatch(KVWorkerId workerId, ReadBatchArgs* args);
    void   CloseCursor(KVWorkerId workerId, CloseCursorArgs* args);
    #ifdef VIDARDB
    bool   RangeQuery(KVWorkerId workerId, RangeQueryArgs* args);
    void   ClearRangeQuery(KVWorkerId workerId, RangeQueryArgs* args);
    #endif
    uint64 Count(KVWorkerId workerId);

  private:
    KVMessageQueue* channel_;
};

struct KVWorkerHandle
{
    KVWorkerId      workerId;
    KVDatabaseId    dbId;
    KVWorkerClient* client;
    void*           handle;

    KVWorkerHandle(KVWorkerId workerId, KVDatabaseId dbId,
        KVWorkerClient* client, void* handle) :
        workerId(workerId), dbId(dbId), client(client), handle(handle) {};
    ~KVWorkerHandle();
};

/*
 * A kv manager which is responsible for managing all the kv workers' lifecycle.
 */

class KVManager
{
  public:
    KVManager();
    ~KVManager();

    void Start();
    void Run();
    void Stop();
    void Launch(KVWorkerId workerId, const KVMessage& msg);
    void Terminate(KVWorkerId workerId, const KVMessage& msg);

  private:
    std::unordered_map<KVWorkerId, KVWorkerHandle*> workers_;
    KVMessageQueue* channel_;
    volatile bool running_;
};

/*
 * A kv manager client which as a stub to interact with kv manager process
 * through its corresponding message queue.
 */

class KVManagerClient
{
  public:
    KVManagerClient();
    ~KVManagerClient();

    bool Launch(KVWorkerId workerId);
    bool Terminate(KVWorkerId workerId, KVDatabaseId dbId);
    void Notify(KVCtrlType type);

  private:
    KVMessageQueue* channel_;
};

/*
 * API for kv message
 */

extern KVMessage SuccessMessage(uint32 channel);
extern KVMessage FailureMessage(uint32 channel);
extern KVMessage SimpleMessage(KVOperation op, KVRelationId rid,
                               KVDatabaseId dbId);

extern void CommonWriteEntity(void* channel, uint64* offset, void* entity,
                              uint64 size);
extern void CommonReadEntity(void* channel, uint64* offset, void* entity,
                             uint64 size);

#endif
