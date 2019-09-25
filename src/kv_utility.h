
#ifndef _UTILITY_H_
#define _UTILITY_H_

#include <sys/stat.h>
#include <unistd.h>
#include "postgres.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "commands/event_trigger.h"
#include "tcop/utility.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "commands/defrem.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "storage/ipc.h"
#include "kv.h"

#define KV_FDW_NAME "kv_fdw"

#define PREVIOUS_UTILITY (PreviousProcessUtilityHook != NULL \
                          ? PreviousProcessUtilityHook : standard_ProcessUtility)

#define CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, \
                              destReceiver, completionTag) \
    PREVIOUS_UTILITY(plannedStmt, queryString, context, paramListInfo, \
                     queryEnvironment, destReceiver, completionTag)

/* Holds the option values to be used when reading or writing files.
 * To resolve these values, we first check foreign table's options,
 * and if not present, we then fall back to the default values.
 */
typedef struct {
    char *filename;
} FdwOptions;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(kv_ddl_event_end_trigger);

/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);

/* local functions forward declarations */
static void KVProcessUtility(PlannedStmt *plannedStmt,
                             const char *queryString,
                             ProcessUtilityContext context,
                             ParamListInfo paramListInfo,
                             QueryEnvironment *queryEnvironment,
                             DestReceiver *destReceiver,
                             char *completionTag);
static void KVShmemStartup(void);

/* saved hook value in case of unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static shmem_startup_hook_type PreviousShmemStartupHook = NULL;


/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void _PG_init(void) {
    PreviousProcessUtilityHook = ProcessUtility_hook;
    ProcessUtility_hook = KVProcessUtility;

    PreviousShmemStartupHook = shmem_startup_hook;
    shmem_startup_hook = KVShmemStartup;
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void _PG_fini(void) {
    ProcessUtility_hook = PreviousProcessUtilityHook;

    shmem_startup_hook = PreviousShmemStartupHook;
}

/* Checks if a directory exists for the given directory name. */
static bool KVDirectoryExists(StringInfo directoryName) {
    bool directoryExists = true;
    struct stat directoryStat;
    if (stat(directoryName->data, &directoryStat) == 0) {
        /* file already exists; check that it is a directory */
        if (!S_ISDIR(directoryStat.st_mode)) {
            ereport(ERROR,
                    (errmsg("\"%s\" is not a directory", directoryName->data),
                     errhint("You need to remove or rename the file \"%s\".",
                             directoryName->data)));
        }
    } else {
        if (errno == ENOENT) {
            directoryExists = false;
        } else {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not stat directory \"%s\": %m",
                            directoryName->data)));
        }
    }

    return directoryExists;
}

/*
 * Creates the directory (and parent directories, if needed)
 * used to store automatically managed kv_fdw files. The path to
 * the directory is $PGDATA/kv_fdw/{databaseOid}.
 */
static void KVCreateDatabaseDirectory(Oid databaseOid) {
    StringInfo directoryPath = makeStringInfo();
    appendStringInfo(directoryPath, "%s/%s", DataDir, KV_FDW_NAME);
    if (!KVDirectoryExists(directoryPath)) {
        if (mkdir(directoryPath->data, S_IRWXU) != 0) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not create directory \"%s\": %m",
                                   directoryPath->data)));
        }
    }

    StringInfo databaseDirectoryPath = makeStringInfo();
    appendStringInfo(databaseDirectoryPath,
                     "%s/%s/%u",
                     DataDir,
                     KV_FDW_NAME,
                     databaseOid);
    if (!KVDirectoryExists(databaseDirectoryPath)) {
        if (mkdir(databaseDirectoryPath->data, S_IRWXU) != 0) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not create directory \"%s\": %m",
                                   databaseDirectoryPath->data)));
        }
    }
}

/*
 * Checks if the given foreign server belongs to kv_fdw. If it
 * does, the function returns true. Otherwise, it returns false.
 */
static bool KVServer(ForeignServer *server) {
    char *fdwName = GetForeignDataWrapper(server->fdwid)->fdwname;
    return strncmp(fdwName, KV_FDW_NAME, NAMEDATALEN) == 0;
}

/*
 * Checks if the given table name belongs to a foreign KV table.
 * If it does, the function returns true. Otherwise, it returns false.
 */
static bool KVTable(Oid relationId) {
    if (relationId == InvalidOid) {
        return false;
    }

    char relationKind = get_rel_relkind(relationId);
    if (relationKind == RELKIND_FOREIGN_TABLE) {
        ForeignTable *foreignTable = GetForeignTable(relationId);
        ForeignServer *server = GetForeignServer(foreignTable->serverid);

        if (KVServer(server)) {
            return true;
        }
    }

    return false;
}

/*
 * kv_ddl_event_end_trigger is the event trigger function which is called on
 * ddl_command_end event. This function creates required directories after the
 * CREATE SERVER statement and after the CREATE FOREIGN TABLE statement.
 */
Datum kv_ddl_event_end_trigger(PG_FUNCTION_ARGS) {
    /* error if event trigger manager did not call this function */
    if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) {
        ereport(ERROR, (errmsg("trigger not fired by event trigger manager")));
    }

    EventTriggerData *triggerData = (EventTriggerData *) fcinfo->context;
    Node *parseTree = triggerData->parsetree;

    if (nodeTag(parseTree) == T_CreateForeignServerStmt) {
        CreateForeignServerStmt *serverStmt = (CreateForeignServerStmt *) parseTree;
        if (strncmp(serverStmt->fdwname, KV_FDW_NAME, NAMEDATALEN) == 0) {
            KVCreateDatabaseDirectory(MyDatabaseId);
        }
    } else if (nodeTag(parseTree) == T_CreateForeignTableStmt) {
        CreateForeignTableStmt *tableStmt = (CreateForeignTableStmt *) parseTree;
        ForeignServer *server = GetForeignServerByName(tableStmt->servername, false);
        if (KVServer(server)) {
            Oid relationId = RangeVarGetRelid(tableStmt->base.relation,
                                              AccessShareLock,
                                              false);

            Relation relation = heap_open(relationId, AccessExclusiveLock);
            /*
             * Make sure database directory exists before creating a table.
             * This is necessary when a foreign server is created inside
             * a template database and a new database is created out of it.
             * We have no chance to hook into server creation to create data
             * directory for it during database creation time.
             */
            KVCreateDatabaseDirectory(MyDatabaseId);

            StringInfo kvPath = makeStringInfo();
            appendStringInfo(kvPath,
                             "%s/%s/%u/%u",
                             DataDir,
                             KV_FDW_NAME,
                             MyDatabaseId,
                             relationId);

            /* Initialize the database */
            void *kvDB = Open(kvPath->data);
            Close(kvDB);

            heap_close(relation, AccessExclusiveLock);
        }
    }

    PG_RETURN_NULL();
}

/*
 * Removes directory previously created for this database.
 * However it does not remove 'kv_fdw' directory even if there
 * are no other databases left.
 */
static void KVRemoveDatabaseDirectory(Oid databaseOid) {
    StringInfo databaseDirectoryPath = makeStringInfo();
    appendStringInfo(databaseDirectoryPath,
                     "%s/%s/%u",
                     DataDir,
                     KV_FDW_NAME,
                     databaseOid);

    if (KVDirectoryExists(databaseDirectoryPath)) {
        rmtree(databaseDirectoryPath->data, true);
    }
}

/*
 * Constructs the default file path to use for a kv_fdw table.
 * The path is of the form $PGDATA/cstore_fdw/{databaseOid}/{relfilenode}.
 */
static char *KVDefaultFilePath(Oid foreignTableId) {
    Relation relation = relation_open(foreignTableId, AccessShareLock);
    RelFileNode relationFileNode = relation->rd_node;

    StringInfo filePath = makeStringInfo();
    appendStringInfo(filePath,
                     "%s/%s/%u/%u",
                     DataDir,
                     KV_FDW_NAME,
                     relationFileNode.dbNode,
                     relationFileNode.relNode);

    relation_close(relation, AccessShareLock);

    return filePath->data;
}

/*
 * Extracts and returns the list of kv file (directory) names
 * from DROP table statement
 */
static List *KVDroppedFilenameList(DropStmt *dropStmt) {
    List *droppedFileList = NIL;
    if (dropStmt->removeType == OBJECT_FOREIGN_TABLE) {

        ListCell *dropObjectCell = NULL;
        foreach(dropObjectCell, dropStmt->objects) {

            List *tableNameList = (List *) lfirst(dropObjectCell);
            RangeVar *rangeVar = makeRangeVarFromNameList(tableNameList);
            Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);

            if (KVTable(relationId)) {
                char *defaultFilename = KVDefaultFilePath(relationId);
                droppedFileList = lappend(droppedFileList, defaultFilename);
            }
        }
    }
    return droppedFileList;
}

/*
 * Hook for handling utility commands. This function
 * customizes the behavior of "DROP FOREIGN TABLE " commands.
 * For all other utility statements, the function calls
 * the previous utility hook or the standard utility command via macro
 * CALL_PREVIOUS_UTILITY.
 */
static void KVProcessUtility(PlannedStmt *plannedStmt,
                             const char *queryString,
                             ProcessUtilityContext context,
                             ParamListInfo paramListInfo,
                             QueryEnvironment *queryEnvironment,
                             DestReceiver *destReceiver,
                             char *completionTag) {
    Node *parseTree = plannedStmt->utilityStmt;
    if (nodeTag(parseTree) == T_DropStmt) {

        DropStmt *dropStmt = (DropStmt *) parseTree;
        if (dropStmt->removeType == OBJECT_EXTENSION) {
            /* drop extension */
            bool removeDirectory = false;
            ListCell *objectCell = NULL;
            foreach(objectCell, dropStmt->objects) {

                Node *object = (Node *) lfirst(objectCell);
                Assert(IsA(object, String));
                char *objectName = strVal(object);
                if (strncmp(KV_FDW_NAME, objectName, NAMEDATALEN) == 0) {
                    removeDirectory = true;
                }
            }

            CALL_PREVIOUS_UTILITY(parseTree,
                                  queryString,
                                  context,
                                  paramListInfo,
                                  destReceiver,
                                  completionTag);

            if (removeDirectory) {
                KVRemoveDatabaseDirectory(MyDatabaseId);
            }
        } else {
            /* drop table & drop server */
            List *droppedTables = KVDroppedFilenameList((DropStmt *) parseTree);

            /* delete metadata */
            CALL_PREVIOUS_UTILITY(parseTree,
                                  queryString,
                                  context,
                                  paramListInfo,
                                  destReceiver,
                                  completionTag);

            /* delete real data */
            ListCell *fileCell = NULL;
            foreach(fileCell, droppedTables) {
                char *path = lfirst(fileCell);
                StringInfo tablePath = makeStringInfo();
                appendStringInfo(tablePath, "%s", path);
                if (KVDirectoryExists(tablePath)) {
                    rmtree(path, true);
                }
            }
        }
    } else {
        /* handle other utility statements */
        CALL_PREVIOUS_UTILITY(parseTree,
                              queryString,
                              context,
                              paramListInfo,
                              destReceiver,
                              completionTag);
    }
}

/*
 * Walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from mongo_fdw.
 */
static char *KVGetOptionValue(Oid foreignTableId, const char *optionName) {
    ForeignTable *foreignTable = GetForeignTable(foreignTableId);
    ForeignServer *foreignServer = GetForeignServer(foreignTable->serverid);

    List *optionList = NIL;
    optionList = list_concat(optionList, foreignTable->options);
    optionList = list_concat(optionList, foreignServer->options);

    ListCell *optionCell = NULL;
    foreach(optionCell, optionList) {
        DefElem *optionDef = (DefElem *) lfirst(optionCell);
        char *optionDefName = optionDef->defname;

        if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0) {
            return defGetString(optionDef);
        }
    }

    return NULL;
}

/*
 * Returns the option values to be used when reading and writing
 * the files. To resolve these values, the function checks options for the
 * foreign table, and if not present, falls back to default values. This function
 * errors out if given option values are considered invalid.
 */
static FdwOptions *KVGetOptions(Oid foreignTableId) {
    char *filename = KVGetOptionValue(foreignTableId, "filename");

    /* set default filename if it is not provided */
    if (filename == NULL) {
        filename = KVDefaultFilePath(foreignTableId);
    }

    FdwOptions *options = palloc0(sizeof(FdwOptions));
    options->filename = filename;

    return options;
}

/*
 * Release memory.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void KVShmemShutdown(int code, Datum arg) {
    printf("\n============KVShmemShutdown=============\n");
}

/*
 * Allocate or attach to shared memory while the module is enabled.
 */
static void KVShmemStartup(void) {
    printf("\n============KVShmemStartup=============\n");
    if (PreviousShmemStartupHook) {
        PreviousShmemStartupHook();
    }

    /*
     * If we're in the postmaster (or a standalone backend...), set up a shmem
     * exit hook to release memory.
     */
    if (!IsUnderPostmaster) {
        on_shmem_exit(KVShmemShutdown, (Datum) 0);
    }
}

#endif /* _UTILITY_H_ */
