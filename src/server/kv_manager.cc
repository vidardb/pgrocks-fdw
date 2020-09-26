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

#include "kv_manager.h"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
}


/*
 * In kv manager process scope
 */

static const char* MANAGER = "Manager";
static KVManager* manager = nullptr;

/*
 * Implementation for kv manager
 */

KVManager::KVManager() {
    running_ = false;
    queue_ = new KVMessageQueue(InvalidOid, MANAGER, true);
}

KVManager::~KVManager() {
    for (auto& it : workers_) {
        delete it.second; /* new in Launch() */
    }
    delete queue_;
}

void KVManager::Start() {
    running_ = true;
}

bool KVManager::CheckKVWorkerAlive(BackgroundWorkerHandle* handle) {
    pid_t pid;
    BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);
    return BGWH_STARTED == status;
}

void KVManager::Launch(KVWorkerId workerId, const KVMessage& msg) {
    auto it = workers_.find(workerId);
    if (it != workers_.end()) {
        if (CheckKVWorkerAlive(it->second->handle)) {
            queue_->Send(SuccessMessage(msg.hdr.rpsId));
            return;
        } else {
            delete it->second;
            workers_.erase(it);
        }
    }

    BackgroundWorkerHandle* handle =
        (BackgroundWorkerHandle*) LaunchKVWorker(workerId, msg.hdr.dbId);
    if (!handle) {
        queue_->Send(FailureMessage(msg.hdr.rpsId));
        return;
    }

    /* wait kv worker be ready */
    queue_->Wait(WorkerReady);

    KVDatabaseId dbId = msg.hdr.dbId;
    KVWorkerClient* client = new KVWorkerClient(workerId);
    KVWorkerHandle* worker = new KVWorkerHandle(workerId, dbId, client, handle);
    workers_.insert({workerId, worker});
    queue_->Send(SuccessMessage(msg.hdr.rpsId));
}

void KVManager::TerminateKVWorker(BackgroundWorkerHandle* handle) {
    TerminateBackgroundWorker(handle);
    WaitForBackgroundWorkerShutdown(handle);
    pfree(handle);
}

void KVManager::Terminate(KVWorkerId workerId, const KVMessage& msg) {
    if (workerId == KVAllRelationId) { /* for dropping database */
        for (auto it = workers_.begin(); it != workers_.end();) {
            if (it->second->dbId != msg.hdr.dbId) {
                it++;
                continue;
            }

            KVWorkerHandle* handle = it->second;
            if (CheckKVWorkerAlive(handle->handle)) {
                handle->client->Terminate(handle->workerId);
                /* wait destroyed event */
                queue_->Wait(WorkerDesty);
            }

            TerminateKVWorker(handle->handle);
            it = workers_.erase(it);
            delete handle;
        }

        queue_->Send(SuccessMessage(msg.hdr.rpsId));
        return;
    }

    auto it = workers_.find(workerId);
    if (it == workers_.end()) {
        queue_->Send(SuccessMessage(msg.hdr.rpsId));
        return;
    }

    KVWorkerHandle* handle = it->second;
    if (CheckKVWorkerAlive(handle->handle)) {
        handle->client->Terminate(handle->workerId);
        /* wait destroyed event */
        queue_->Wait(WorkerDesty);
    }

    TerminateKVWorker(handle->handle);
    workers_.erase(it);
    delete handle;

    queue_->Send(SuccessMessage(msg.hdr.rpsId));
}

void KVManager::Run() {
    while (running_) {
        KVMessage msg;
        queue_->Recv(msg);

        switch (msg.hdr.op) {
            case KVOpDummy:
                break;
            case KVOpLaunch:
                Launch(msg.hdr.relId, msg);
                break;
            case KVOpTerminate:
                Terminate(msg.hdr.relId, msg);
                break;
            default:
                ereport(WARNING, (errmsg("invalid operation: %d", msg.hdr.op)));
        }
    }
}

void KVManager::Stop() {
    for (auto& it : workers_) {
        if (CheckKVWorkerAlive(it.second->handle)) {
            it.second->client->Terminate(it.first);
            /* wait destroyed event */
            queue_->Wait(WorkerDesty);
        }
        TerminateKVWorker(it.second->handle);
    }

    running_ = false;
    queue_->Stop();
}


/*
 * Implementation for kv manager client
 */

KVManagerClient::KVManagerClient() {
    queue_ = new KVMessageQueue(InvalidOid, MANAGER, false);
}

KVManagerClient::~KVManagerClient() {
    delete queue_;
}

bool KVManagerClient::Launch(KVWorkerId workerId) {
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpLaunch, workerId, MyDatabaseId);
    queue_->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

bool KVManagerClient::Terminate(KVWorkerId workerId, KVDatabaseId dbId) {
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpTerminate, workerId, dbId);
    queue_->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

void KVManagerClient::Notify(KVCtrlType type) {
    queue_->Notify(type);
}

/*
 * Start kv manager and begin to accept and handle requets.
 * Also it will clean the resources when run has finished.
 */
static void KVManagerDo() {
    manager = new KVManager();
    manager->Start();
    manager->Run();
    delete manager;
}

/*
 * Signal handler for SIGTERM
 *
 * Set a flag to let the main loop to terminate, and set our latch to
 * wake it up.
 */
static void KVManagerSigHandler(SIGNAL_ARGS) {
    int save_errno = errno;
    manager->Stop();
    errno = save_errno;
}

/*
 * Entrypoint for kv manager
 */
extern "C" void KVManagerMain(Datum arg) {
    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGTERM, KVManagerSigHandler);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Start, run and clean of kv manager */
    KVManagerDo();
}

/*
 * Launch kv manager process
 */
void LaunchKVManager() {
    printf("\n~~~~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    BackgroundWorker bgw;
    memset(&bgw, 0, sizeof(bgw));
    snprintf(bgw.bgw_name, BGW_MAXLEN, "KV Manager");
    snprintf(bgw.bgw_type, BGW_MAXLEN, "KV Manager");
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
    bgw.bgw_restart_time = 1;
    sprintf(bgw.bgw_library_name, "kv_fdw");
    sprintf(bgw.bgw_function_name, "KVManagerMain");
    bgw.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&bgw);
}
