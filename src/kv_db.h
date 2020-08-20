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

#include <map>
#include <semaphore.h>

#include "kv_api.h"

using namespace std;

#define MSGBUFSIZE        65536
#define MSGPATHPREFIX     "/KV"
#define MSGHEADER         01
#define MSGENTITY         02
#define MSGRESQUEUELENGTH 2
#define MAXPATHLENGTH     64

/*
 * Circular Queue exchanges messages between different processes
 */

struct CircularQueueData
{
    uint64 putPos;           /* the position producer can put data */
    uint64 getPos;           /* the position consumer can get data */
    sem_t  mutex;            /* mutual exclusion for position */
    sem_t  empty;            /* tell wether the data buf is empty */
    sem_t  full;             /* tell wether the data buf is full */
    char   data[MSGBUFSIZE]; /* assume ~64K for a tuple is enough */
};

struct CircularQueueChannel
{
    char                        name[MAXPATHLENGTH];
    volatile bool               create;
    volatile CircularQueueData* channel;

    void read(uint64 *offset, char *str, uint64 size);
    void write(uint64 *offset, char *str, uint64 size);

    CircularQueueChannel(KVRelationId rid, const char* tag, bool create);
    ~CircularQueueChannel();
};

typedef enum
{
    WorkerReady = 0,
} CtrlType;

struct CtrlData
{
    sem_t workerReady;       /* tell wether kv worker is ready */
    sem_t responseMutex[MSGRESQUEUELENGTH]; /* mutual exclusion for response */
};

struct CtrlChannel
{
    char               name[MAXPATHLENGTH];
    volatile bool      create;
    volatile CtrlData* channel;

    void    wait(volatile sem_t* sem);
    void    notify(volatile sem_t* sem);
    uint32  leaseResponseQueue();
    void    unleaseResponseQueue(uint32 index);

    CtrlChannel(KVRelationId rid, const char* tag, bool create);
    ~CtrlChannel();
};

/*
 * KV Message contains both header and entity
 */

typedef void (*WriteMessageFunc) (CircularQueueChannel* channel, uint64* offset,
                                  void* entity, uint64 size);
typedef void (*ReadMessageFunc)  (CircularQueueChannel* channel, uint64* offset,
                                  void* entity, uint64 size);

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

    /* write & read entity function */
    ReadMessageFunc  readFunc  = NULL;
    WriteMessageFunc writeFunc = NULL;
};

/*
 * KV Message Queue exchanges messages between different processes
 */

struct KVMessageQueue
{
    CtrlChannel*          ctrl;
    CircularQueueChannel* request;
    CircularQueueChannel* response[MSGRESQUEUELENGTH];
    volatile bool         isServer;
    volatile bool         running;

    void   send(KVMessage const& msg);
    void   sendWithResponse(KVMessage& sendmsg, KVMessage& recvmsg);
    void   recv(KVMessage& msg);
    void   recv(KVMessage& msg, int flag);
    void   wait(CtrlType type);
    void   notify(CtrlType type);
    void   terminate();
    uint32 leaseResponseQueue();
    void   unleaseResponseQueue(uint32 index);

    KVMessageQueue(KVRelationId rid, const char* name, bool isServer);
    ~KVMessageQueue();
};

/*
 * KV Connection wraps storage API
 */

struct KVConnection
{
    void*  db;
    uint64 ref;

    #ifdef VIDARDB
    void*  open(char* path, ComparatorOpt* opt, bool useColumn, int attrCount);
    #else
    void*  open(char* path, ComparatorOpt* opt);
    #endif
    void   close(void* db);
    uint64 count(void* db);
};

/*
 * KV Worker handles requests from kv client
 */

struct KVWorker
{
    KVMessageQueue* channel;
    KVConnection*   connection;
    KVWorkerId      workerId;
    KVDatabaseId    dbId;
    volatile bool   running;

    void start();
    void run();
    void stop();
    void open(KVWorkerId const& workerId, KVMessage& msg);
    void terminate(KVWorkerId const& workerId);

    KVWorker(KVWorkerId workerId, KVDatabaseId dbId);
    ~KVWorker();
};

struct KVWorkerClient
{
    KVMessageQueue* channel;

    bool open(KVWorkerId const& workerId, OpenArgs* args);
    void terminate(KVWorkerId const& workerId);

    KVWorkerClient(KVWorkerId workerId);
    ~KVWorkerClient();
};

struct KVWorkerHandle
{
    KVWorkerId      workerId;
    KVWorkerClient* client;
    void*           handle;

    KVWorkerHandle(KVWorkerId workerId, KVWorkerClient* client, void* handle) :
        workerId(workerId), client(client), handle(handle) {};
    ~KVWorkerHandle();
};

/*
 * KV Manager manages the lifecycle of kv workers
 */

struct KVManager
{
    map<KVWorkerId, KVWorkerHandle*> workers;
    KVMessageQueue*                  channel;
    volatile bool                    running;

    void start();
    void run();
    void stop();
    void launch(KVWorkerId const& workerId, KVMessage const& msg);
    void terminate(KVWorkerId const& workerId, KVMessage const& msg);

    KVManager();
    ~KVManager();
};

struct KVManagerClient
{
    KVMessageQueue* channel;

    bool launch(KVWorkerId const& workerId);
    bool terminate(KVWorkerId const& workerId);

    KVManagerClient();
    ~KVManagerClient();
};

/*
 * API for kv message
 */

extern KVMessage SimpleSuccessMessage();
extern KVMessage SimpleFailureMessage();
extern KVMessage SimpleSuccessMessageWithChannel(uint32 channel);
extern KVMessage SimpleFailureMessageWithChannel(uint32 channel);
extern KVMessage SimpleMessage(KVOperation op, KVRelationId rid,
    KVDatabaseId dbId);
extern KVMessage SimpleMessageWithEntity(KVOperation op, KVRelationId rid,
    KVDatabaseId dbId, void* entity, uint64 size);
extern KVMessage SimpleMessageWithEntity(KVOperation op, KVRelationId rid,
    KVDatabaseId dbId, void* entity, uint64 size, ReadMessageFunc readFunc,
    WriteMessageFunc writeFunc);

extern void CommonWriteMessage(CircularQueueChannel* channel, uint64* offset,
                               void* entity, uint64 size);
extern void CommonReadMessage(CircularQueueChannel* channel, uint64* offset,
                              void* entity, uint64 size);

#endif
