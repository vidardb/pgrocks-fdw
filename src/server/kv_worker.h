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

#ifndef KV_WORKER_H_
#define KV_WORKER_H_


#include <unordered_map>
using namespace std;

#include "ipc/kv_mq.h"


extern void* LaunchKVWorker(KVWorkerId workerId, KVDatabaseId dbId);


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
    void Close(KVWorkerId workerId, KVMessage& msg);
    void Count(KVWorkerId workerId, KVMessage& msg);
    void Put(KVWorkerId workerId, KVMessage& msg);
    void Get(KVWorkerId workerId, KVMessage& msg);
    void Delete(KVWorkerId workerId, KVMessage& msg);
    void Load(KVWorkerId workerId, KVMessage& msg);
    void ReadBatch(KVWorkerId workerId, KVMessage& msg);
    void CloseCursor(KVWorkerId workerId, KVMessage& msg);
    #ifdef VIDARDB
    void RangeQuery(KVWorkerId workerId, KVMessage& msg);
    void ClearRangeQuery(KVWorkerId workerId, KVMessage& msg);
    #endif
    void Terminate(KVWorkerId workerId, KVMessage& msg);

  private:
    static void ReadOpenArgs(KVChannel* channel, uint64* offset, void* entity,
                             uint64 size);
    static void WriteReadBatchState(KVChannel* channel, uint64* offset,
                                    void* entity, uint64 size);

    struct ReadBatchState {
        bool   next;
        uint64 size;
    };

    struct KVCursorKey {
        pid_t      pid;     /* backend process pid */
        KVCursorId cursor;

        bool operator==(const KVCursorKey& key) const {
            return pid == key.pid && cursor == key.cursor;
        }
    };

    struct KVCursorKeyHashFunc {
        size_t operator()(const KVCursorKey& key) const {
            return (hash<pid_t>()(key.pid)) ^ (hash<KVCursorId>()(key.cursor));
        }
    };

    unordered_map<KVCursorKey, void*, KVCursorKeyHashFunc> cursors_;

    #ifdef VIDARDB
    struct KVRangeQueryEntry {
        void* readOpts = nullptr;
        void* range    = nullptr;
    };
    unordered_map<KVCursorKey, KVRangeQueryEntry, KVCursorKeyHashFunc> ranges_;
    #endif

    KVMessageQueue* queue_;
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
    void   Close(KVWorkerId workerId);
    uint64 Count(KVWorkerId workerId);
    bool   Put(KVWorkerId workerId, PutArgs* args);
    bool   Get(KVWorkerId workerId, GetArgs* args);
    bool   Delete(KVWorkerId workerId, DeleteArgs* args);
    void   Load(KVWorkerId workerId, PutArgs* args);
    bool   ReadBatch(KVWorkerId workerId, ReadBatchArgs* args);
    void   CloseCursor(KVWorkerId workerId, CloseCursorArgs* args);
    #ifdef VIDARDB
    bool   RangeQuery(KVWorkerId workerId, RangeQueryArgs* args);
    void   ClearRangeQuery(KVWorkerId workerId, RangeQueryArgs* args);
    #endif
    void   Terminate(KVWorkerId workerId);

  private:
    static void WriteOpenArgs(KVChannel* channel, uint64* offset, void* entity,
                              uint64 size);
    static void WritePutArgs(KVChannel* channel, uint64* offset, void* entity,
                             uint64 size);
    static void WriteReadBatchArgs(KVChannel* channel, uint64* offset,
                                   void* entity, uint64 size);
    static void WriteDelCursorArgs(KVChannel* channel, uint64* offset,
                                   void* entity, uint64 size);
    #ifdef VIDARDB
    static void WriteRangeQueryArgs(KVChannel* channel, uint64* offset,
                                    void* entity, uint64 size);
    #endif

    KVMessageQueue* queue_;
};

struct BackgroundWorkerHandle;

struct KVWorkerHandle {
    KVWorkerId              workerId;
    KVDatabaseId            dbId;
    KVWorkerClient*         client;
    BackgroundWorkerHandle* handle;

    KVWorkerHandle(KVWorkerId workerId, KVDatabaseId dbId,
                   KVWorkerClient* client, BackgroundWorkerHandle* handle) :
        workerId(workerId), dbId(dbId), client(client), handle(handle) {};
    ~KVWorkerHandle() { delete client; }
};

#endif  /* KV_WORKER_H_ */
