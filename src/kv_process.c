
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "kv_shm.h"


/* key must be the first attribute */
typedef struct WorkerProcHandleEntry {
    WorkerProcKey key;
    pid_t pid;
    BackgroundWorkerHandle *handle;
} WorkerProcHandleEntry;


/* non-shared hash can be enlarged */
static long HASHSIZE = 1;

/* referenced by manager process */
static HTAB *workerHash = NULL;

/* flags set by signal handlers */
static volatile sig_atomic_t gotSIGTERM = false;


void KVManageWork(Datum);
void KVDoWork(Datum);
pid_t LaunchBackgroundWorker(WorkerProcKey *workerKey, ManagerShm *manager);
void ShutOffBackgroundWorker(WorkerProcKey *workerKey, ManagerShm *manager);


/*
 * Compare function for WorkerProcKey
 */
int
CompareWorkerProcKey(const void *key1, const void *key2, Size keysize) {
    const WorkerProcKey *k1 = (const WorkerProcKey *) key1;
    const WorkerProcKey *k2 = (const WorkerProcKey *) key2;

    if (k1 == NULL || k2 == NULL) {
        return -1;
    }

    if (k1->relationId == k2->relationId && k1->databaseId == k2->databaseId) {
        return 0;
    }

    return -1;
}

/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake it up.
 */
static void
KVSIGTERM(SIGNAL_ARGS) {
    int save_errno = errno;

    gotSIGTERM = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/*
 * Initialize shared memory and release it when exits
 */
void
KVManageWork(Datum arg) {
    /* Establish signal handlers before unblocking signals. */
    /* pqsignal(SIGTERM, KVSIGTERM); */
    /*
     * We on purpose do not use pqsignal due to its setting at flags = restart.
     * With the setting, the process cannot exit on sem_wait.
     */
    struct sigaction act;
    act.sa_handler = KVSIGTERM;
    act.sa_flags = 0;
    sigaction(SIGTERM, &act, NULL);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    ManagerShm *manager = InitManagerShm();

    HASHCTL hash_ctl;
    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(WorkerProcKey);
    hash_ctl.entrysize = sizeof(WorkerProcHandleEntry);
    hash_ctl.match = CompareWorkerProcKey;
    workerHash = hash_create("workerHash", HASHSIZE, &hash_ctl,
                             HASH_ELEM | HASH_COMPARE);

    while (!gotSIGTERM) {
        /*
         * Don't create worker process until needed!
         * Semaphore here also catches SIGTERN signal.
         */
        if (SemWait(&manager->manager, __func__) == -1) {
            break;
        }

        WorkerProcKey workerKey;
        workerKey.databaseId = manager->databaseId;
        workerKey.relationId = manager->relationId;
        FuncName func = manager->func;

        switch (func) {
            case OPEN:
                LaunchBackgroundWorker(&workerKey, manager);
                break;
            case CLOSE:
                ShutOffBackgroundWorker(&workerKey, manager);
                break;
            default:
                ereport(ERROR, (errmsg("%s failed in switch", __func__)));
        }

        SemPost(&manager->backend, __func__);
    };

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, workerHash);
    WorkerProcHandleEntry *entry = NULL;
    while ((entry = hash_seq_search(&status)) != NULL) {
        ShutOffBackgroundWorker(&entry->key, manager);
    }

    CloseManagerShm(manager);
    proc_exit(0);
}

void
KVDoWork(Datum arg) {
    WorkerProcKey workerKey;
    workerKey.databaseId = DatumGetObjectId(arg);
    workerKey.relationId = *((Oid *) (MyBgworkerEntry->bgw_extra));
    KVWorkerMainOld(&workerKey);
    proc_exit(0);
}

/*
 * Entrypoint, dynamically register worker process here, at most one process
 * for each foreign table created by kv engine.
 */
pid_t
LaunchBackgroundWorker(WorkerProcKey *workerKey, ManagerShm *manager) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

    /* check whether the requested relation has the process */
    bool found = false;
    WorkerProcHandleEntry *entry = hash_search(workerHash, workerKey,
                                               HASH_ENTER, &found);
    if (found) {  /* worker has already been created */
        manager->success = true;
        return entry->pid;
    }

    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "kv_fdw");
    sprintf(worker.bgw_function_name, "KVDoWork");
    snprintf(worker.bgw_name, BGW_MAXLEN, "KV worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "KV worker");
    worker.bgw_main_arg = ObjectIdGetDatum(workerKey->databaseId);
    memcpy(worker.bgw_extra, &(workerKey->relationId), sizeof(workerKey->relationId));
    /* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
    worker.bgw_notify_pid = MyProcPid;
    BackgroundWorkerHandle *handle;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        hash_search(workerHash, workerKey, HASH_REMOVE, NULL);
        manager->success = false;
        return 0;
    }

    pid_t pid;
    BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status == BGWH_STOPPED) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not start background process"),
                 errhint("More details may be available in the server log.")));
    }
    if (status == BGWH_POSTMASTER_DIED) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("cannot start background processes without postmaster"),
                 errhint("Kill all remaining database processes and restart "
                         "the database.")));
    }
    Assert(status == BGWH_STARTED);

    entry->key = *workerKey;
    entry->pid = pid;
    entry->handle = handle;
    manager->success = true;

    /*
     * Make sure worker process has inited to avoid race between backend and worker.
     */
    SemWait(&manager->ready, __func__);

    return pid;
}

/*
 * Terminate the specified dynamically registered worker process.
 */
void
ShutOffBackgroundWorker(WorkerProcKey *workerKey, ManagerShm *manager) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

    if (!OidIsValid(workerKey->relationId)) {
        /* terminate all workers related to the database */
        HASH_SEQ_STATUS status;
        hash_seq_init(&status, workerHash);

        List *removedWorkerList = NIL;
        WorkerProcHandleEntry *entry = NULL;
        while ((entry = hash_seq_search(&status)) != NULL) {
            if (entry->key.databaseId != workerKey->databaseId) {
                continue;
            }

            TerminateWorker(&entry->key);
            TerminateBackgroundWorker(entry->handle);
            WaitForBackgroundWorkerShutdown(entry->handle);
            removedWorkerList = lappend(removedWorkerList, entry);
        }

        ListCell *cell = NULL;
        foreach(cell, removedWorkerList) {
            WorkerProcHandleEntry *entry = lfirst(cell);
            pfree(entry->handle);
            hash_search(workerHash, &entry->key, HASH_REMOVE, NULL);
        }
    } else {
        /* check whether the requested relation has the process */
        bool found = false;
        WorkerProcHandleEntry *entry = hash_search(workerHash, workerKey,
                                                   HASH_REMOVE, &found);
        if (!found) {
            return;
        }

        TerminateWorker(workerKey);
        TerminateBackgroundWorker(entry->handle);
        WaitForBackgroundWorkerShutdown(entry->handle);
        pfree(entry->handle);
    }
}
