
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
#include "access/xact.h"
#include "pgstat.h"
#include "tcop/utility.h"
#include "kv_shm.h"


void KVManageWork(Datum);
void LaunchBackgroundManager(void);
void KVDoWork(Datum);
pid_t LaunchBackgroundWorker(void);


/* flags set by signal handlers */
static volatile sig_atomic_t gotSIGTERM = false;


/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake it up.
 */
static void KVSIGTERM(SIGNAL_ARGS) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);
    int save_errno = errno;

    gotSIGTERM = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/*
 * Initialize shared memory and release it when exits
 */
void KVManageWork(Datum arg) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

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

    SharedMem *ptr = InitSharedMem();

    ptr->workerProcessCreated = false;

    while (!gotSIGTERM) {
        /*
         * Don't create worker process until needed!
         * Semaphore here also catches SIGTERN signal.
         */
        if (SemWait(&ptr->agent[0], __func__) == -1) {
            break;
        }
        kvWorkerPid = LaunchBackgroundWorker();
        ptr->workerProcessCreated = true;

        SemPost(&ptr->agent[1], __func__);
    };

    cleanup_handler(&ptr);
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
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);
    KVWorkerMain();
    proc_exit(0);
}

/*
 * Entrypoint, dynamically register worker process here, at most one process for
 * each database containing table created by kv engine.
 */
pid_t LaunchBackgroundWorker(void) {
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
