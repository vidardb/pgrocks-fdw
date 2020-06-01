
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
//void KVDoWork(Datum);
//Datum LaunchBackgroundWorker(void);


/* flags set by signal handlers */
//static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t gotSigterm = false;

/* GUC variables */
//static int  KVWorkerNaptime = 10;


/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake it up.
 */
static void KVManagerSigterm(SIGNAL_ARGS) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);
    int save_errno = errno;

    gotSigterm = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 * Set a flag to tell the main loop to reread the config file, and set our latch
 * to wake it up.
 */
//static void KVWorkerSighup(SIGNAL_ARGS) {
//    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);
//    int save_errno = errno;
//
//    got_sighup = true;
//    SetLatch(MyLatch);
//
//    errno = save_errno;
//}

/*
 * Initialize shared memory and release it when exits
 */
void KVManageWork(Datum main_arg) {
    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);

    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGTERM, KVManagerSigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    SharedMem *ptr = InitSharedMem();

    /* Main loop: do this until the SIGTERM handler tells us to terminate */
    while (!gotSigterm) {
        /*
         * Background workers mustn't call usleep() or any direct equivalent:
         * instead, they may wait on their process latch, which sleeps as
         * necessary, but is awakened if postmaster dies. That way the
         * background process goes away immediately in an emergency.
         */
        int rc = WaitLatch(MyLatch,
                           WL_LATCH_SET | WL_POSTMASTER_DEATH,
                           -1L,
                           PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH) {
            cleanup_handler(&ptr);
            proc_exit(1);
        }

        CHECK_FOR_INTERRUPTS();

        printf("\n~~~~~~~~~~~~in the loop~~~~~~~~~~~~~~~\n");
    }

    cleanup_handler(&ptr);
    proc_exit(1);
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
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    sprintf(worker.bgw_library_name, "kv_fdw");
    sprintf(worker.bgw_function_name, "KVManageWork");
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

//void KVDoWork(Datum main_arg) {
//    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);
//
//    /* Establish signal handlers before unblocking signals. */
//    pqsignal(SIGHUP, KVWorkerSighup);
//    pqsignal(SIGTERM, KVWorkerSigterm);
//
//    /* We're now ready to receive signals */
//    BackgroundWorkerUnblockSignals();
//
//    /* Connect to our database */
//    BackgroundWorkerInitializeConnection(NULL, NULL, 0);
//
//    /* Main loop: do this until the SIGTERM handler tells us to terminate */
//    while (!got_sigterm) {
//        /*
//         * Background workers mustn't call usleep() or any direct equivalent:
//         * instead, they may wait on their process latch, which sleeps as
//         * necessary, but is awakened if postmaster dies. That way the
//         * background process goes away immediately in an emergency.
//         */
//        int rc = WaitLatch(MyLatch,
//                           WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
//                           KVWorkerNaptime * 1000L,
//                           PG_WAIT_EXTENSION);
//        ResetLatch(MyLatch);
//
//        /* emergency bailout if postmaster has died */
//        if (rc & WL_POSTMASTER_DEATH) {
//            proc_exit(1);
//        }
//
//        CHECK_FOR_INTERRUPTS();
//
//        /* In case of a SIGHUP, just reload the configuration. */
//        if (got_sighup) {
//            got_sighup = false;
//            ProcessConfigFile(PGC_SIGHUP);
//        }
//
//        /* Start */
//        StartTransactionCommand();
//        pgstat_report_activity(STATE_RUNNING, NULL);
//
////        printf("!!!!!!!!!!!!!!!!!!!!!!begin!!!!!!!!!!!!!!!!!!!!!!\n");
////        TypeCacheEntry *typentry = lookup_type_cache(1731639, TYPECACHE_CMP_PROC_FINFO);
////        printf("!!!!!!!!!!!!!!!!!!!!!!end!!!!!!!!!!!!!!!!!!!!!!!!\n");
////        printf("\n TypeCacheEntry: %d, %d\n", 1731639, typentry->cmp_proc);
////        printf("\n!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
//
//        /* Finish */
//        CommitTransactionCommand();
//        pgstat_report_stat(false);
//        pgstat_report_activity(STATE_IDLE, NULL);
//    }
//
//    proc_exit(1);
//}
//
///*
// * Dynamically register worker process here.
// */
//Datum LaunchBackgroundWorker(void) {
//    printf("\n~~~~~~~~~~~~%s~~~~~~~~~~~~~~~\n", __func__);
//
//    BackgroundWorker worker;
//    memset(&worker, 0, sizeof(worker));
//    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
//    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
//    worker.bgw_restart_time = BGW_NEVER_RESTART;
//    sprintf(worker.bgw_library_name, "kv_fdw");
//    sprintf(worker.bgw_function_name, "KVDoWork");
//    snprintf(worker.bgw_name, BGW_MAXLEN, "KV worker");
//    snprintf(worker.bgw_type, BGW_MAXLEN, "KV worker");
//    /* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
//    worker.bgw_notify_pid = MyProcPid;
//    BackgroundWorkerHandle *handle;
//    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
//        return (Datum) NULL;
//    }
//
//    pid_t pid;
//    BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &pid);
//
//    if (status == BGWH_STOPPED) {
//        ereport(ERROR,
//                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
//                 errmsg("could not start background process"),
//                 errhint("More details may be available in the server log.")));
//    }
//    if (status == BGWH_POSTMASTER_DIED) {
//        ereport(ERROR,
//                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
//                 errmsg("cannot start background processes without postmaster"),
//                 errhint("Kill all remaining database processes and restart the database.")));
//    }
//    Assert(status == BGWH_STARTED);
//
//    printf("\n~~~~~~~~~~~~End of %s~~~~~~~~~~~~~~~\n", __func__);
//    PG_RETURN_INT32(pid);
//}
