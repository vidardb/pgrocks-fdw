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
#include "storage/latch.h"
}


/*
 * In kv manager process scope
 */

static const char* MANAGER = "Manager";
static KVManager* manager = NULL;

/*
 * Implementation for kv manager
 */

KVManager::KVManager() : running_(false) {
    channel_ = new KVMessageQueue(InvalidOid, MANAGER, true);
    workers_.clear();
}

KVManager::~KVManager() {
    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
    for (it = workers_.begin(); it != workers_.end(); it++) {
        delete it->second;
    }
    delete channel_;
}

void KVManager::Start() {
    running_ = true;
}

void KVManager::Run() {
    while (running_) {
        KVMessage msg;
        channel_->Recv(msg);

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

static void TerminateKVWorker(void* worker) {
    BackgroundWorkerHandle* handle = (BackgroundWorkerHandle*) worker;
    TerminateBackgroundWorker(handle);
    WaitForBackgroundWorkerShutdown(handle);
    pfree(handle);
}

void KVManager::Stop() {
    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
    for (it = workers_.begin(); it != workers_.end(); it++) {
        it->second->client->Terminate(it->first);
        /* wait destroyed event */
        channel_->Wait(WorkerDesty);
        TerminateKVWorker(it->second->handle);
    }

    running_ = false;
    channel_->Terminate();
}

static bool CheckKVWorkerAlive(void* worker) {
    pid_t pid;
    BackgroundWorkerHandle* handle = (BackgroundWorkerHandle*) worker;
    BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);
    return BGWH_STARTED == status;
}

void KVManager::Launch(KVWorkerId workerId, const KVMessage& msg) {
    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it =
        workers_.find(workerId);
    if (it != workers_.end()) {
        if (CheckKVWorkerAlive(it->second->handle)) {
            channel_->Send(SuccessMessage(msg.hdr.rpsId));
            return;
        } else {
            delete it->second;
            workers_.erase(it);
        }
    }

    void* handle = LaunchKVWorker(workerId, msg.hdr.dbId);
    if (!handle) {
        channel_->Send(FailureMessage(msg.hdr.rpsId));
        return;
    }

    /* wait kv worker be ready */
    channel_->Wait(WorkerReady);

    KVDatabaseId dbId = msg.hdr.dbId;
    KVWorkerClient* client = new KVWorkerClient(workerId);
    KVWorkerHandle* worker = new KVWorkerHandle(workerId, dbId, client, handle);
    workers_.insert({workerId, worker});
    channel_->Send(SuccessMessage(msg.hdr.rpsId));
}

void KVManager::Terminate(KVWorkerId workerId, const KVMessage& msg) {
    if (workerId == KVAllRelationId) {
        std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it;
        for (it = workers_.begin(); it != workers_.end();) {
            if (it->second->dbId != msg.hdr.dbId) {
                it++;
                continue;
            }

            KVWorkerHandle* handle = it->second;
            if (CheckKVWorkerAlive(handle->handle)) {
                handle->client->Terminate(handle->workerId);
                /* wait destroyed event */
                channel_->Wait(WorkerDesty);
            }

            TerminateKVWorker(handle->handle);
            it = workers_.erase(it);
            delete handle;
        }

        channel_->Send(SuccessMessage(msg.hdr.rpsId));
        return;
    }

    std::unordered_map<KVWorkerId, KVWorkerHandle*>::iterator it =
        workers_.find(workerId);
    if (it == workers_.end()) {
        channel_->Send(SuccessMessage(msg.hdr.rpsId));
        return;
    }

    KVWorkerHandle* handle = it->second;
    if (CheckKVWorkerAlive(handle->handle)) {
        handle->client->Terminate(handle->workerId);
        /* wait destroyed event */
        channel_->Wait(WorkerDesty);
    }

    TerminateKVWorker(handle->handle);
    workers_.erase(it);
    delete handle;

    channel_->Send(SuccessMessage(msg.hdr.rpsId));
}


/*
 * Implementation for kv manager client
 */

KVManagerClient::KVManagerClient() {
    channel_ = new KVMessageQueue(InvalidOid, MANAGER, false);
}

KVManagerClient::~KVManagerClient() {
    delete channel_;
}

bool KVManagerClient::Launch(KVWorkerId workerId) {
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpLaunch, workerId, MyDatabaseId);
    channel_->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

bool KVManagerClient::Terminate(KVWorkerId workerId, KVDatabaseId dbId) {
    KVMessage recvmsg;
    KVMessage sendmsg = SimpleMessage(KVOpTerminate, workerId, dbId);
    channel_->SendWithResponse(sendmsg, recvmsg);
    return recvmsg.hdr.status == KVStatusSuccess;
}

void KVManagerClient::Notify(KVCtrlType type) {
    channel_->Notify(type);
}

/*
 * Start kv manager
 */
static void StartKVManager(void) {
    manager = new KVManager();
    manager->Start();
    manager->Run();
    delete manager;
}

/*
 * Terminate kv manager
 */
static void TerminateKVManager(void) {
    manager->Stop();
}

/*
 * Signal handler for SIGTERM
 *
 * Set a flag to let the main loop to terminate, and set our latch to
 * wake it up.
 */
static void KVManagerSigHandler(SIGNAL_ARGS) {
    int save_errno = errno;

    TerminateKVManager();
    SetLatch(MyLatch);

    errno = save_errno;
}

/*
 * Entrypoint for kv manager
 */
extern "C" void KVManagerMain(Datum arg) {
    /* Establish signal handlers before unblocking signals. */
    /* pqsignal(SIGTERM, KVManagerSigHandler); */

    /*
     * We on purpose do not use pqsignal due to its setting at flags = restart.
     * With the setting, the process cannot exit on sem_wait.
     */
    struct sigaction act;
    act.sa_handler = KVManagerSigHandler;
    act.sa_flags = 0;
    sigaction(SIGTERM, &act, NULL);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Start kv manager */
    StartKVManager();
}

/*
 * Launch kv manager process
 */
void LaunchKVManager(void) {
    printf("\n~~~~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "KV Manager");
    snprintf(worker.bgw_type, BGW_MAXLEN, "KV Manager");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    sprintf(worker.bgw_library_name, "kv_fdw");
    sprintf(worker.bgw_function_name, "KVManagerMain");
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}
