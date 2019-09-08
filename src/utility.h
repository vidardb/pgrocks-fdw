
#ifndef _UTILITY_H_
#define _UTILITY_H_

#include <sys/stat.h>
#include "postgres.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "commands/event_trigger.h"

#define KV_FDW_NAME "kv_fdw"


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
 * Checks if the given foreign server belongs to kv_fdw. If it
 * does, the function returns true. Otherwise, it returns false.
 */
static bool KVServer(ForeignServer *server) {
    char *fdwName = GetForeignDataWrapper(server->fdwid)->fdwname;
    return strncmp(fdwName, KV_FDW_NAME, NAMEDATALEN) == 0;
}

/*
 * ddl_event_end_trigger is the event trigger function which is called on
 * ddl_command_end event. This function creates required directories after the
 * CREATE SERVER statement and after the CREATE FOREIGN TABLE statement.
 */
Datum ddl_event_end_trigger(PG_FUNCTION_ARGS) {
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
//            Oid relationId = RangeVarGetRelid(createStmt->base.relation,
//                                              AccessShareLock,
//                                              false);
//            Relation relation = heap_open(relationId, AccessExclusiveLock);

            /*
             * Make sure database directory exists before creating a table.
             * This is necessary when a foreign server is created inside
             * a template database and a new database is created out of it.
             * We have no chance to hook into server creation to create data
             * directory for it during database creation time.
             */
            CreateDatabaseDirectory(MyDatabaseId);

//            InitializeCStoreTableFile(relationId, relation);
//            heap_close(relation, AccessExclusiveLock);
        }
    }

    PG_RETURN_NULL();
}

#endif /* _UTILITY_H_ */
