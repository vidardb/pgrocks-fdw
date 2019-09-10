
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
#include "catalog/pg_class.h"
#include "utils/lsyscache.h"
#include "commands/defrem.h"
#include "kv.h"

#define KV_FDW_NAME "kv_fdw"

#define PREVIOUS_UTILITY (PreviousProcessUtilityHook != NULL \
                          ? PreviousProcessUtilityHook : standard_ProcessUtility)

#define CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, \
                              destReceiver, completionTag) \
    PREVIOUS_UTILITY(plannedStatement, queryString, context, paramListInfo, \
                     queryEnvironment, destReceiver, completionTag)

PG_FUNCTION_INFO_V1(kv_ddl_event_end_trigger);

/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);

/* local functions forward declarations */
static void KVProcessUtility(PlannedStmt *plannedStatement, const char *queryString,
                                 ProcessUtilityContext context,
                                 ParamListInfo paramListInfo,
                                 QueryEnvironment *queryEnvironment,
                                 DestReceiver *destReceiver, char *completionTag);

/* saved hook value in case of unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void _PG_init(void) {
    PreviousProcessUtilityHook = ProcessUtility_hook;
    ProcessUtility_hook = KVProcessUtility;
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void _PG_fini(void) {
    ProcessUtility_hook = PreviousProcessUtilityHook;
}

/* Checks if a directory exists for the given directory name. */
static bool DirectoryExists(StringInfo directoryName) {
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

/* Creates a new directory with the given directory name. */
static void CreateDirectory(StringInfo directoryName) {
    if (mkdir(directoryName->data, S_IRWXU) != 0) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not create directory \"%s\": %m",
                               directoryName->data)));
    }
}

/*
 * Creates the directory (and parent directories, if needed)
 * used to store automatically managed kv_fdw files. The path to
 * the directory is $PGDATA/kv_fdw/{databaseOid}.
 */
static void CreateDatabaseDirectory(Oid databaseOid) {
    StringInfo directoryPath = makeStringInfo();
    appendStringInfo(directoryPath, "%s/%s", DataDir, KV_FDW_NAME);
    if (!DirectoryExists(directoryPath)) {
        CreateDirectory(directoryPath);
    }

    StringInfo databaseDirectoryPath = makeStringInfo();
    appendStringInfo(databaseDirectoryPath,
                     "%s/%s/%u",
                     DataDir,
                     KV_FDW_NAME,
                     databaseOid);
    if (!DirectoryExists(databaseDirectoryPath)) {
        CreateDirectory(databaseDirectoryPath);
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
            CreateDatabaseDirectory(MyDatabaseId);
        }
    } else if (nodeTag(parseTree) == T_CreateForeignTableStmt) {
        CreateForeignTableStmt *createStmt = (CreateForeignTableStmt *) parseTree;
        ForeignServer *server = GetForeignServerByName(createStmt->servername, false);
        if (KVServer(server)) {
            /*
             * Make sure database directory exists before creating a table.
             * This is necessary when a foreign server is created inside
             * a template database and a new database is created out of it.
             * We have no chance to hook into server creation to create data
             * directory for it during database creation time.
             */
            CreateDatabaseDirectory(MyDatabaseId);

            Oid relationId = RangeVarGetRelid(createStmt->base.relation,
                                              AccessShareLock,
                                              false);
            StringInfo kvPath = makeStringInfo();
            appendStringInfo(kvPath,
                             "%s/%s/%u/%u",
                             DataDir,
                             KV_FDW_NAME,
                             MyDatabaseId,
                             relationId);

            void *kvDB = Open(kvPath->data);
            Close(kvDB);
        }
    }

    PG_RETURN_NULL();
}

/*
 * Removes directory previously created for this database.
 * However it does not remove 'kv_fdw' directory even if there
 * are no other databases left.
 */
static void RemoveDatabaseDirectory(Oid databaseOid) {
    StringInfo databaseDirectoryPath = makeStringInfo();
    appendStringInfo(databaseDirectoryPath,
                     "%s/%s/%u",
                     DataDir,
                     KV_FDW_NAME,
                     databaseOid);

    if (DirectoryExists(databaseDirectoryPath)) {
        rmtree(databaseDirectoryPath->data, true);
    }
}

/*
 * Deletes the files for a kv table whose data filename is given.
 */
static void DeleteTableFiles(char *filename) {
    StringInfo tableFooterFilename = makeStringInfo();
    appendStringInfo(tableFooterFilename, "%s", filename);

    /* delete the footer file */
    int footerFileRemoved = unlink(tableFooterFilename->data);
    if (footerFileRemoved != 0) {
        ereport(WARNING, (errcode_for_file_access(),
                          errmsg("could not delete file \"%s\": %m",
                                 tableFooterFilename->data)));
    }

    /* delete the data file */
    int dataFileRemoved = unlink(filename);
    if (dataFileRemoved != 0) {
        ereport(WARNING, (errcode_for_file_access(),
                          errmsg("could not delete file \"%s\": %m",
                                 filename)));
    }
}

/*
 * Walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from mongo_fdw.
 */
static char *GetOptionValue(Oid foreignTableId, const char *optionName) {
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
 * Extracts and returns the list of kv file (directory) names
 * from DROP table statement
 */
static List *DroppedFilenameList(DropStmt *dropStatement) {
    List *droppedCStoreFileList = NIL;

    if (dropStatement->removeType == OBJECT_FOREIGN_TABLE) {
        ListCell *dropObjectCell = NULL;
        foreach(dropObjectCell, dropStatement->objects) {
            List *tableNameList = (List *) lfirst(dropObjectCell);
            RangeVar *rangeVar = makeRangeVarFromNameList(tableNameList);

            Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);
            if (KVTable(relationId)) {
//                CStoreFdwOptions *cstoreFdwOptions = CStoreGetOptions(relationId);
//                char *defaultfilename = CStoreDefaultFilePath(relationId);
//
//                /*
//                 * Skip files that are placed in default location, they are handled
//                 * by sql drop trigger. Both paths are generated by code, use
//                 * of strcmp is safe here.
//                 */
//                if (strcmp(defaultfilename, cstoreFdwOptions->filename) == 0) {
//                    continue;
//                }
//
//                droppedCStoreFileList = lappend(droppedCStoreFileList,
//                                                cstoreFdwOptions->filename);
            }
        }
    }

    return droppedCStoreFileList;
}

/*
 * Hook for handling utility commands. This function
 * customizes the behavior of "DROP FOREIGN TABLE " commands.
 * For all other utility statements, the function calls
 * the previous utility hook or the standard utility command via macro
 * CALL_PREVIOUS_UTILITY.
 */
static void KVProcessUtility(PlannedStmt *plannedStatement,
                           const char *queryString,
                           ProcessUtilityContext context,
                           ParamListInfo paramListInfo,
                           QueryEnvironment *queryEnvironment,
                           DestReceiver *destReceiver,
                           char *completionTag) {
    Node *parseTree = plannedStatement->utilityStmt;
    printf("\n~~~~~~~~~~~~~~KVProcessUtility~~~~~~~~~~~~~~\n");
    if (nodeTag(parseTree) == T_DropStmt) {
        printf("\n~~~~~~~~~~~~~~drop statement~~~~~~~~~~~~~~\n");
        DropStmt *dropStmt = (DropStmt *) parseTree;
        if (dropStmt->removeType == OBJECT_EXTENSION) {
            printf("\n~~~~~~~~~~~~~~drop extension ~~~~~~~~~~~~~~\n");
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
                RemoveDatabaseDirectory(MyDatabaseId);
            }
        } else {
            printf("\n~~~~~~~~~~~~~~drop others ~~~~~~~~~~~~~~\n");
            CALL_PREVIOUS_UTILITY(parseTree,
                                  queryString,
                                  context,
                                  paramListInfo,
                                  destReceiver,
                                  completionTag);

            ListCell *fileListCell = NULL;
            List *droppedTables = DroppedFilenameList((DropStmt *) parseTree);
            foreach(fileListCell, droppedTables) {
                char *fileName = lfirst(fileListCell);
                DeleteTableFiles(fileName);
            }
        }
    } else {
        printf("\n~~~~~~~~~~~~~~other utilies ~~~~~~~~~~~~~~\n");
        /* handle other utility statements */
        CALL_PREVIOUS_UTILITY(parseTree,
                              queryString,
                              context,
                              paramListInfo,
                              destReceiver,
                              completionTag);
    }
}

#endif /* _UTILITY_H_ */
