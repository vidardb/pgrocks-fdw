/* Copyright 2020 VidarDB Inc.
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
 * KV Message Defines
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
    uint32          resChan = 0;
    uint64          bdySize = 0;
};

struct KVMessage
{
    KVMessageHeader  hdr;
    void*            bdy       = NULL;

    ReadEntityFunc   readFunc  = NULL; /* read function */
    WriteEntityFunc  writeFunc = NULL; /* write function */
};

/*
 * KV Channel Defines
 */

class KVChannel
{
  public:
    virtual ~KVChannel() {};
    virtual void Send(KVMessage const& msg) = 0;
    virtual void Recv(KVMessage& msg) { Recv(msg, MSGHEADER | MSGENTITY); };
    virtual void Recv(KVMessage& msg, int flag) = 0;
    virtual void Read(uint64 *offset, char *str, uint64 size) = 0;
    virtual void Write(uint64 *offset, char *str, uint64 size) = 0;
    virtual void Terminate() = 0;
};

/*
 * KV Circular Queue Defines
 */

struct KVCircularQueueData
{
    uint64 putPos;           /* the position producer can put data */
    uint64 getPos;           /* the position consumer can get data */
    sem_t  mutex;            /* mutual exclusion for position */
    sem_t  empty;            /* tell wether the data buf is empty */
    sem_t  full;             /* tell wether the data buf is full */
    char   data[MSGBUFSIZE]; /* assume ~64K for a tuple is enough */
};

class KVCircularQueueChannel : public KVChannel
{
  public:
    KVCircularQueueChannel(KVRelationId rid, const char* tag, bool create);
    ~KVCircularQueueChannel();

    void Send(KVMessage const& msg);
    void Recv(KVMessage& msg, int flag);
    void Read(uint64 *offset, char *str, uint64 size);
    void Write(uint64 *offset, char *str, uint64 size);
    void Terminate();

  private:
    char name_[MAXPATHLENGTH];
    bool create_;
    volatile bool running_;
    volatile KVCircularQueueData* channel_;
};

/*
 * KV Simple Queue Defines
 */

struct KVSimpleQueueData
{
    uint64 getPos;           /* the position consumer can get data */
    sem_t  mutex;            /* mutual exclusion for response */
    sem_t  ready;            /* tell wether response is ready */
    char   data[MSGBUFSIZE]; /* assume ~64K for a tuple is enough */
};

class KVSimpleQueueChannel : public KVChannel
{
  public:
    KVSimpleQueueChannel(KVRelationId rid, const char* tag, bool create);
    ~KVSimpleQueueChannel();

    void Send(KVMessage const& msg);
    void Recv(KVMessage& msg, int flag);
    void Read(uint64 *offset, char *str, uint64 size);
    void Write(uint64 *offset, char *str, uint64 size);
    void Terminate() {};
    bool Lease();
    void Unlease();

  private:
    char name_[MAXPATHLENGTH];
    bool create_;
    volatile KVSimpleQueueData* channel_;
};

/*
 * KV Ctrl Queue Defines
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
 * KV Message Queue Defines
 */

class KVMessageQueue
{
  public:
    KVMessageQueue(KVRelationId rid, const char* name, bool isServer);
    ~KVMessageQueue();

    void   Send(KVMessage const& msg);
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
    KVCircularQueueChannel* request_;
    KVSimpleQueueChannel* response_[MSGRESQUEUELENGTH];
    volatile bool isServer_;
};

/*
 * KV Worker handles requests from kv client
 */

struct KVCursorKey
{
    pid_t      pid;    /* backend process pid */
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
    void Open(KVWorkerId const& workerId, KVMessage& msg);
    void Put(KVWorkerId const& workerId, KVMessage& msg);
    void Delete(KVWorkerId const& workerId, KVMessage& msg);
    void Load(KVWorkerId const& workerId, KVMessage& msg);
    void Get(KVWorkerId const& workerId, KVMessage& msg);
    void Close(KVWorkerId const& workerId, KVMessage& msg);
    void Count(KVWorkerId const& workerId, KVMessage& msg);
    void Terminate(KVWorkerId const& workerId, KVMessage& msg);
    void ReadBatch(KVWorkerId const& workerId, KVMessage& msg);
    void CloseCursor(KVWorkerId const& workerId, KVMessage& msg);
    #ifdef VIDARDB
    void RangeQuery(KVWorkerId const& workerId, KVMessage& msg);
    void ClearRangeQuery(KVWorkerId const& workerId, KVMessage& msg);
    #endif

  private:
    std::unordered_map<KVCursorKey, void*, KVCursorKeyHashFunc> cursors_;
    #ifdef VIDARDB
    std::unordered_map<KVCursorKey, KVRangeQueryEntry, KVCursorKeyHashFunc> ranges_;
    #endif
    KVMessageQueue* channel_;
    KVWorkerId workerId_;
    volatile bool running_;
    void* conn_;
    uint64 ref_;
};

class KVWorkerClient
{
  public:
    KVWorkerClient(KVWorkerId workerId);
    ~KVWorkerClient();

    bool   Open(KVWorkerId const& workerId, OpenArgs* args);
    bool   Put(KVWorkerId const& workerId, PutArgs* args);
    bool   Delete(KVWorkerId const& workerId, DeleteArgs* args);
    void   Load(KVWorkerId const& workerId, PutArgs* args);
    bool   Get(KVWorkerId const& workerId, GetArgs* args);
    bool   Close(KVWorkerId const& workerId);
    void   Terminate(KVWorkerId const& workerId);
    bool   ReadBatch(KVWorkerId const& workerId, ReadBatchArgs* args);
    bool   CloseCursor(KVWorkerId const& workerId, DelCursorArgs* args);
    #ifdef VIDARDB
    bool   RangeQuery(KVWorkerId const& workerId, RangeQueryArgs* args);
    void   ClearRangeQuery(KVWorkerId const& workerId, RangeQueryArgs* args);
    #endif
    uint64 Count(KVWorkerId const& workerId);

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
 * KV Manager manages the lifecycle of kv workers
 */

class KVManager
{
  public:
    KVManager();
    ~KVManager();

    void Start();
    void Run();
    void Stop();
    void Launch(KVWorkerId const& workerId, KVMessage const& msg);
    void Terminate(KVWorkerId const& workerId, KVMessage const& msg);

  private:
    std::unordered_map<KVWorkerId, KVWorkerHandle*> workers_;
    KVMessageQueue* channel_;
    volatile bool running_;
};

class KVManagerClient
{
  public:
    KVManagerClient();
    ~KVManagerClient();

    bool Launch(KVWorkerId const& workerId);
    bool Terminate(KVWorkerId const& workerId, KVDatabaseId const& dbId);
    void Notify(KVCtrlType type);

  private:
    KVMessageQueue* channel_;
};

/*
 * API for kv message
 */

extern KVMessage SimpleSuccessMessage(uint32 channel);
extern KVMessage SimpleFailureMessage(uint32 channel);
extern KVMessage SimpleMessage(KVOperation op, KVRelationId rid,
                               KVDatabaseId dbId);

extern void CommonWriteEntity(void* channel, uint64* offset, void* entity,
                              uint64 size);
extern void CommonReadEntity(void* channel, uint64* offset, void* entity,
                             uint64 size);

#endif
