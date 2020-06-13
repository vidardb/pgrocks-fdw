
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


void KVManageWork(Datum);
void LaunchBackgroundManager(void);
void KVDoWork(Datum);
pid_t LaunchBackgroundWorker(Oid databaseId);


/* non-shared hash can be enlarged */
static long HASHSIZE = 1;

/* flags set by signal handlers */
static volatile sig_atomic_t gotSIGTERM = false;


/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake it up.
 */
static void KVSIGTERM(SIGNAL_ARGS) {
    int save_errno = errno;

    gotSIGTERM = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/*
 * Initialize shared memory and release it when exits
 */
void KVManageWork(Datum arg) {
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

    ManagerSharedMem *manager = InitManagerSharedMem();

    HASHCTL hashCtl;
    memset(&hashCtl, 0, sizeof(hashCtl));
    hashCtl.entrysize = hashCtl.keysize = sizeof(MyDatabaseId);
    HTAB *wokerHash = hash_create("wokerHash",
                                  HASHSIZE,
                                  &hashCtl,
                                  HASH_ELEM | HASH_BLOBS);

    while (!gotSIGTERM) {
        /*
         * Don't create worker process until needed!
         * Semaphore here also catches SIGTERN signal.
         */
        if (SemWait(&manager->manager, __func__) == -1) {
            break;
        }

        /* check whether the requested database has the process */
        Oid databaseId = manager->databaseId;
        bool found = false;
        Oid *entry = hash_search(wokerHash, &databaseId, HASH_ENTER, &found);
        if (!found) {
            *entry = databaseId;
            LaunchBackgroundWorker(databaseId);

            /*
             * Make sure worker process has inited to avoid race between
             * backend and worker.
             */
            SemWait(&manager->ready, __func__);
        }

        SemPost(&manager->backend, __func__);
    };

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, wokerHash);
    Oid *entry = NULL;
    while ((entry = hash_seq_search(&status)) != NULL) {
        TerminateWorker(*entry);
    }

    CloseManagerSharedMem(manager);
    proc_exit(0);
}

/*
 * Entrypoint, register manager process here, called in _PG_init
 */
void LaunchBackgroundManager(void) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "KV manager");
    snprintf(worker.bgw_type, BGW_MAXLEN, "KV manager");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    sprintf(worker.bgw_library_name, "kv_fdw");
    sprintf(worker.bgw_function_name, "KVManageWork");
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

void KVDoWork(Datum arg) {
    Oid databaseId = DatumGetObjectId(arg);
    KVWorkerMain(databaseId);
    proc_exit(0);
}

/*
 * Entrypoint, dynamically register worker process here, at most one process for
 * each database containing table created by kv engine.
 */
pid_t LaunchBackgroundWorker(Oid databaseId) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "kv_fdw");
    sprintf(worker.bgw_function_name, "KVDoWork");
    snprintf(worker.bgw_name, BGW_MAXLEN, "KV worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "KV worker");
    worker.bgw_main_arg = ObjectIdGetDatum(databaseId);
    /* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
    worker.bgw_notify_pid = MyProcPid;
    BackgroundWorkerHandle *handle;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
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

    return pid;
}
