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

#ifndef SRC_KV_WORKER_H_
#define SRC_KV_WORKER_H_


#include <unordered_map>
#include "../ipc/kv_mq.h"


#define READBATCHSIZE     4096*20


struct KVCursorKey {
    pid_t      pid;     /* backend process pid */
    KVCursorId cursor;

    bool operator==(const KVCursorKey& key) const {
        return pid == key.pid && cursor == key.cursor;
    }
};

#ifdef VIDARDB
struct KVRangeQueryEntry {
    void* readOpts = NULL;
    void* range    = NULL;
};
#endif

struct KVCursorKeyHashFunc {
    size_t operator()(const KVCursorKey& key) const {
        return (std::hash<pid_t>()(key.pid)) ^
               (std::hash<KVCursorId>()(key.cursor));
    }
};

/*
 * A kv worker which is responsible for receiving kv messages from its
 * corresponding message queue and calling storage engine api to handle
 * the kv tuples.
 */

class KVWorker {
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

class KVWorkerClient {
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

struct KVWorkerHandle {
    KVWorkerId      workerId;
    KVDatabaseId    dbId;
    KVWorkerClient* client;
    void*           handle;

    KVWorkerHandle(KVWorkerId workerId, KVDatabaseId dbId,
                   KVWorkerClient* client, void* handle) :
        workerId(workerId), dbId(dbId), client(client), handle(handle) {};
    ~KVWorkerHandle() { delete client; }
};


#endif /* SRC_KV_WORKER_H_ */
