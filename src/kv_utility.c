
#include "kv_fdw.h"
#include "kv_storage.h"
#include "kv_posix.h"

#include <sys/stat.h>

#include "foreign/foreign.h"
#include "miscadmin.h"
#include "commands/event_trigger.h"
#include "tcop/utility.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "commands/defrem.h"
#include "utils/rel.h"
#include "storage/ipc.h"
#include "commands/copy.h"
#include "utils/memutils.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"


#define PREVIOUS_UTILITY (PreviousProcessUtilityHook != NULL ? \
                          PreviousProcessUtilityHook : standard_ProcessUtility)

#define CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, \
                              destReceiver, completionTag) \
        PREVIOUS_UTILITY(plannedStmt, queryString, context, paramListInfo, \
                         queryEnvironment, destReceiver, completionTag)

/* */
static SharedMem *ptr = NULL;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(kv_ddl_event_end_trigger);


/* saved hook value in case of unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static shmem_startup_hook_type PreviousShmemStartupHook = NULL;


/* local functions forward declarations */
static void KVProcessUtility(PlannedStmt *plannedStmt,
                             const char *queryString,
                             ProcessUtilityContext context,
                             ParamListInfo paramListInfo,
                             QueryEnvironment *queryEnvironment,
                             DestReceiver *destReceiver,
                             char *completionTag);
static void KVShmemStartup(void);


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
    appendStringInfo(directoryPath, "%s/%s", DataDir, KVFDWNAME);
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
                     KVFDWNAME,
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
    return strncmp(fdwName, KVFDWNAME, NAMEDATALEN) == 0;
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
        if (strncmp(serverStmt->fdwname, KVFDWNAME, NAMEDATALEN) == 0) {
            KVCreateDatabaseDirectory(MyDatabaseId);
        }
    } else if (nodeTag(parseTree) == T_CreateForeignTableStmt) {
        CreateForeignTableStmt *tableStmt = (CreateForeignTableStmt *) parseTree;
        ForeignServer *server = GetForeignServerByName(tableStmt->servername,
                                                       false);
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
                             KVFDWNAME,
                             MyDatabaseId,
                             relationId);

            /* Initialize the database */
            #ifdef VidarDB
            bool useColumn = false;
            char* storageOption = GetOptionValue(relationId, OPTION_STORAGE_FORMAT);
            if (storageOption != NULL) {
                if (0 == strncmp(storageOption, COLUMNSTORE, sizeof(COLUMNSTORE))) {
                    useColumn = true;
                }
            }
            TupleDesc tupleDescriptor = RelationGetDescr(relation);
            int natts = tupleDescriptor->natts;
            void *kvDB = Open(kvPath->data, useColumn, natts);
            #else
            void *kvDB = Open(kvPath->data);
            #endif
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
                     KVFDWNAME,
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
                     KVFDWNAME,
                     relationFileNode.dbNode,
                     relationFileNode.relNode);

    relation_close(relation, AccessShareLock);

    return filePath->data;
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
KVFdwOptions *KVGetOptions(Oid foreignTableId) {
    char *filename = KVGetOptionValue(foreignTableId, "filename");

    /* set default filename if it is not provided */
    if (filename == NULL) {
        filename = KVDefaultFilePath(foreignTableId);
    }

    KVFdwOptions *options = palloc0(sizeof(KVFdwOptions));
    options->filename = filename;

    return options;
}

/*
 * Extracts and returns the list of kv file (directory) names from DROP table
 * statement.
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
 * Checks whether the COPY statement is a "COPY kv_table FROM ..." or
 * "COPY kv_table TO ...." statement. If it is then the function returns
 * true. The function returns false otherwise.
 */
static bool KVCopyTableStatement(CopyStmt* copyStmt) {
    if (!copyStmt->relation) {
        return false;
    }

    Oid relationId = RangeVarGetRelid(copyStmt->relation,
                                      AccessShareLock,
                                      true);
    return KVTable(relationId);
}

/*
 * Checks if superuser privilege is required by copy operation and reports
 * error if user does not have superuser rights.
 */
static void KVCheckSuperuserPrivilegesForCopy(const CopyStmt* copyStmt) {
    /*
     * We disallow copy from file or program except to superusers. These checks
     * are based on the checks in DoCopy() function of copy.c.
     */
    if (copyStmt->filename != NULL && !superuser()) {
        if (copyStmt->is_program) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("must be superuser to COPY to or from a program"),
                     errhint("Anyone can COPY to stdout or from stdin. "
                             "psql's \\copy command also works for anyone.")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("must be superuser to COPY to or from a file"),
                     errhint("Anyone can COPY to stdout or from stdin. "
                             "psql's \\copy command also works for anyone.")));
        }
    }
}

Datum ShortVarlena(Datum datum, int typeLength, char storage) {
    /* Make sure item to be inserted is not toasted */
    if (typeLength == -1) {
        datum = PointerGetDatum(PG_DETOAST_DATUM_PACKED(datum));
    }

    if (typeLength == -1 && storage != 'p' && VARATT_CAN_MAKE_SHORT(datum)) {
        /* convert to short varlena -- no alignment */
        Pointer val = DatumGetPointer(datum);
        uint32 shortSize = VARATT_CONVERTED_SHORT_SIZE(val);
        Pointer temp = palloc0(shortSize);
        SET_VARSIZE_SHORT(temp, shortSize);
        memcpy(temp + 1, VARDATA(val), shortSize - 1);
        datum = PointerGetDatum(temp);
    }

    PG_RETURN_DATUM(datum);
}

#ifdef VidarDB
void SerializeAttribute(TupleDesc tupleDescriptor,
                        Index index,
                        Datum datum,
                        StringInfo buffer,
                        bool useColumn) {
    Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, index);
    bool byValue = attributeForm->attbyval;
    int typeLength = attributeForm->attlen;
    char storage = attributeForm->attstorage;

    datum = ShortVarlena(datum, typeLength, storage);
    uint32 offset = buffer->len;
    uint32 datumLength = att_addlength_datum(offset, typeLength, datum);

    if (useColumn && index > 0) {
        enlargeStringInfo(buffer, datumLength + 1);
    }else {
        enlargeStringInfo(buffer, datumLength);
    }
    char *current = buffer->data + buffer->len;

    if (useColumn && index > 0) {
        memset(current, 0, datumLength + 1 - offset);
    } else {
        memset(current, 0, datumLength - offset);
    }

    if (typeLength > 0) {
        if (byValue) {
            store_att_byval(current, datum, typeLength);
        } else {
            memcpy(current, DatumGetPointer(datum), typeLength);
        }
    } else {
        memcpy(current, DatumGetPointer(datum), datumLength - offset);
    }

    if (useColumn && index > 0) {
        char delimiter = '|';
        memcpy(current+datumLength-offset+1, &delimiter, 1);
        buffer->len = (datumLength + 1);
    } else {
        buffer->len = datumLength;
    }
}
#else
void SerializeAttribute(TupleDesc tupleDescriptor,
                        Index index,
                        Datum datum,
                        StringInfo buffer) {
    Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, index);
    bool byValue = attributeForm->attbyval;
    int typeLength = attributeForm->attlen;
    char storage = attributeForm->attstorage;

    /* copy utility gets varlena with 4B header, same with constant */
    datum = ShortVarlena(datum, typeLength, storage);

    uint32 offset = buffer->len;
    uint32 datumLength = att_addlength_datum(offset, typeLength, datum);

    enlargeStringInfo(buffer, datumLength);

    char *current = buffer->data + buffer->len;
    memset(current, 0, datumLength - offset);

    if (typeLength > 0) {
        if (byValue) {
            store_att_byval(current, datum, typeLength);
        } else {
            memcpy(current, DatumGetPointer(datum), typeLength);
        }
    } else {
        memcpy(current, DatumGetPointer(datum), datumLength - offset);
    }

    buffer->len = datumLength;
}
#endif

/*
 * Handles a "COPY kv_table FROM" statement. This function uses the COPY
 * command's functions to read and parse rows from the data source specified
 * in the COPY statement. The function then writes each row to the file
 * specified in the foreign table options. Finally, the function returns the
 * number of copied rows.
 */
static uint64 KVCopyIntoTable(const CopyStmt *copyStmt,
                              const char *queryString) {
    /* TODO copy specified columns */
    if (copyStmt->attlist != NIL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("copy column list is not supported"),
                        errhint("use 'copy (select <columns> from <table>) to "
                                "...' instead")));
    }

    /* Only superuser can copy from or to local file */
    KVCheckSuperuserPrivilegesForCopy(copyStmt);

    /*
     * Open and lock the relation. We acquire ShareUpdateExclusiveLock to allow
     * concurrent reads, but block concurrent writes.
     */
    Relation relation = heap_openrv(copyStmt->relation,
                                    ShareUpdateExclusiveLock);

    /* init state to read from COPY data source */
    ParseState *pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;
    CopyState copyState = BeginCopyFrom(pstate,
                                        relation,
                                        copyStmt->filename,
                                        copyStmt->is_program,
                                        NULL,
                                        NIL/*copyStatement->attlist*/,
                                        copyStmt->options);
    free_parsestate(pstate);

    /*
     * We create a new memory context called tuple context, and read and write
     * each row's values within this memory context. After each read and write,
     * we reset the memory context. That way, we immediately release memory
     * allocated for each row, and don't bloat memory usage with large input
     * files.
     */
    MemoryContext tupleContext = AllocSetContextCreate(CurrentMemoryContext,
                                                       "KV COPY Row Context",
                                                       ALLOCSET_DEFAULT_SIZES);

    Oid relationId = RelationGetRelid(relation);
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    uint32 count = tupleDescriptor->natts;

    #ifdef VidarDB
    char* storageOption = GetOptionValue(relationId, OPTION_STORAGE_FORMAT);
    bool useColumn = false;
    if (storageOption != NULL) {
        if (0 == strncmp(storageOption, COLUMNSTORE, sizeof(COLUMNSTORE))) {
            useColumn = true;
        }
    }
    ptr = OpenRequest(relationId, ptr, useColumn, count);
    #else
    ptr = OpenRequest(relationId, ptr);
    #endif

    Datum *values = palloc0(count * sizeof(Datum));
    bool *nulls = palloc0(count * sizeof(bool));

    /* first column must exist */
    uint32 bufLen = (count - 1 + 7) / 8;
    StringInfo buffer = makeStringInfo();
    enlargeStringInfo(buffer, bufLen);
    buffer->len = bufLen;

    uint64 rowCount = 0;
    bool found = true;
    while (found) {
        /* read the next row in tupleContext */
        MemoryContext oldContext = MemoryContextSwitchTo(tupleContext);
        found = NextCopyFrom(copyState, NULL, values, nulls, NULL);
        /* write the row to the kv file */
        if (found) {
            memset(buffer->data, 0, bufLen);
            StringInfo key = makeStringInfo();
            StringInfo val = makeStringInfo();
            val->len += bufLen;

            for (uint32 index = 0; index < count; index++) {

                if (nulls[index]) {
                    if (index == 0) {
                        ereport(ERROR, (errmsg("first column cannot be null!")));
                    }
                    uint32 byteIndex = (index - 1) / 8;
                    uint32 bitIndex = (index - 1) % 8;
                    uint8 bitmask = (1 << bitIndex);
                    buffer->data[byteIndex] |= bitmask;
                    continue;
                }
                Datum datum = values[index];
                #ifdef VidarDB
                if (index == count - 1) {
                    useColumn = false;
                }
                SerializeAttribute(tupleDescriptor,
                                   index,
                                   datum,
                                   index==0? key: val,
                                   useColumn);
                #else
                SerializeAttribute(tupleDescriptor,
                                   index,
                                   datum,
                                   index==0? key: val);
                #endif
            }
            memcpy(val->data, buffer->data, bufLen);

            PutRequest(relationId, ptr, key->data, key->len, val->data, val->len);
            rowCount++;
        }

        MemoryContextSwitchTo(oldContext);
        MemoryContextReset(tupleContext);
        CHECK_FOR_INTERRUPTS();
    }

    /* end read/write sessions and close the relation */
    EndCopyFrom(copyState);
    CloseRequest(relationId, ptr);
    heap_close(relation, ShareUpdateExclusiveLock);

    return rowCount;
}

/*
 * Handles a "COPY kv_table TO ..." statement. Statement is converted to
 * "COPY (SELECT * FROM kv_table) TO ..." and forwarded to Postgres native
 * COPY handler. Function returns number of files copied to external stream.
 */
static uint64 KVCopyOutTable(CopyStmt* copyStmt, const char* queryString) {

    if (copyStmt->attlist != NIL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("copy column list is not supported"),
                        errhint("use 'copy (select <columns> from <table>) to "
                                "...' instead")));
    }

    RangeVar *relation = copyStmt->relation;
    char *qualifiedName = quote_qualified_identifier(relation->schemaname,
                                                     relation->relname);
    StringInfo newQuerySubstring = makeStringInfo();
    appendStringInfo(newQuerySubstring, "select * from %s", qualifiedName);
    List *queryList = raw_parser(newQuerySubstring->data);

    /* take the first parse tree */
    Node *rawQuery = linitial(queryList);

    /*
     * Set the relation field to NULL so that COPY command works on
     * query field instead.
     */
    copyStmt->relation = NULL;

    /*
     * raw_parser returns list of RawStmt* in PG 10+ we need to
     * extract actual query from it.
     */
    RawStmt *rawStmt = (RawStmt *) rawQuery;

    ParseState *pstate = make_parsestate(NULL);
    pstate->p_sourcetext = newQuerySubstring->data;
    copyStmt->query = rawStmt->stmt;

    uint64 count = 0;
    DoCopy(pstate, copyStmt, -1, -1, &count);
    free_parsestate(pstate);

    return count;
}

/*
 * Checks alter column type compatible. The function errors out if current
 * column type can not be safely converted to requested column type.
 * This check is more restrictive than PostgreSQL's because we can not
 * change existing data. However, it is not strict enough to prevent cast like
 * float <--> integer, which does not deserialize successfully in our case.
 */
static void KVCheckAlterTable(AlterTableStmt *alterStmt) {
    ObjectType objectType = alterStmt->relkind;
    /* we are only interested in foreign table changes */
    if (objectType != OBJECT_TABLE && objectType != OBJECT_FOREIGN_TABLE) {
        return;
    }

    RangeVar *rangeVar = alterStmt->relation;
    Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);
    if (!KVTable(relationId)) {
        return;
    }

    ListCell *cmdCell = NULL;
    List *cmdList = alterStmt->cmds;
    foreach (cmdCell, cmdList) {

        AlterTableCmd *alterCmd = (AlterTableCmd *) lfirst(cmdCell);
        if (alterCmd->subtype == AT_AlterColumnType) {

            char *columnName = alterCmd->name;
            AttrNumber attributeNumber = get_attnum(relationId, columnName);
            if (attributeNumber <= 0) {
                /* let standard utility handle this */
                continue;
            }

            Oid currentTypeId = get_atttype(relationId, attributeNumber);

            /*
             * We are only interested in implicit coersion type compatibility.
             * Erroring out here to prevent further processing.
             */
            ColumnDef *columnDef = (ColumnDef *) alterCmd->def;
            Oid targetTypeId = typenameTypeId(NULL, columnDef->typeName);
            if (!can_coerce_type(1,
                                 &currentTypeId,
                                 &targetTypeId,
                                 COERCION_IMPLICIT)) {
                char *typeName = TypeNameToString(columnDef->typeName);
                ereport(ERROR, (errmsg("Column %s cannot be cast automatically "
                                       "to type %s", columnName, typeName)));
            }
        }

        if (alterCmd->subtype == AT_AddColumn) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("No support for adding column currently")));
        }
    }
}

/*
 * Hook for handling utility commands. This function customizes the behavior of
 * "COPY kv_table" and "DROP FOREIGN TABLE " commands. For all other utility
 * statements, the function calls the previous utility hook or the standard
 * utility command via macro CALL_PREVIOUS_UTILITY.
 */
static void KVProcessUtility(PlannedStmt *plannedStmt,
                             const char *queryString,
                             ProcessUtilityContext context,
                             ParamListInfo paramListInfo,
                             QueryEnvironment *queryEnvironment,
                             DestReceiver *destReceiver,
                             char *completionTag) {
    Node *parseTree = plannedStmt->utilityStmt;
    if (nodeTag(parseTree) == T_CopyStmt) {

        CopyStmt *copyStmt = (CopyStmt *) parseTree;
        if (KVCopyTableStatement(copyStmt)) {

            uint64 rowCount = 0;
            if (copyStmt->is_from) {
                rowCount = KVCopyIntoTable(copyStmt, queryString);
            } else {
                rowCount = KVCopyOutTable(copyStmt, queryString);
            }

            if (completionTag != NULL) {
                snprintf(completionTag,
                         COMPLETION_TAG_BUFSIZE,
                         "COPY " UINT64_FORMAT,
                          rowCount);
            }
        } else {
            CALL_PREVIOUS_UTILITY(parseTree,
                                  queryString,
                                  context,
                                  paramListInfo,
                                  destReceiver,
                                  completionTag);
        }
    } else if (nodeTag(parseTree) == T_DropStmt) {

        DropStmt *dropStmt = (DropStmt *) parseTree;
        if (dropStmt->removeType == OBJECT_EXTENSION) {
            /* drop extension */
            bool removeDirectory = false;
            ListCell *objectCell = NULL;
            foreach(objectCell, dropStmt->objects) {

                Node *object = (Node *) lfirst(objectCell);
                Assert(IsA(object, String));
                char *objectName = strVal(object);
                if (strncmp(KVFDWNAME, objectName, NAMEDATALEN) == 0) {
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
    } else if (nodeTag(parseTree) == T_AlterTableStmt) {
        AlterTableStmt *alterStmt = (AlterTableStmt *) parseTree;
        KVCheckAlterTable(alterStmt);
        CALL_PREVIOUS_UTILITY(parseTree,
                              queryString,
                              context,
                              paramListInfo,
                              destReceiver,
                              completionTag);
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
 * Release memory.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void KVShmemShutdown(int code, Datum arg) {
    printf("\n============%s=============\n", __func__);
    PthreadCancel(kvStorageThread, __func__);

    void *retVal;
    PthreadJoin(kvStorageThread, &retVal, __func__);
    if (retVal == PTHREAD_CANCELED) {
        printf("\nkvStorageThread cancelled!\n");
    } else {
        printf("\nkvStorageThread is not cancelled!\n");
    }
}

/*
 * Allocate or attach to shared memory while the module is enabled.
 */
static void KVShmemStartup(void) {
    printf("\n============%s=============\n", __func__);
    if (PreviousShmemStartupHook) {
        PreviousShmemStartupHook();
    }

    /*
     * If we're in the postmaster (or a standalone backend...), set up a shmem
     * exit hook to release memory. I wonder what else we can in?
     */
    if (!IsUnderPostmaster) {
        before_shmem_exit(KVShmemShutdown, (Datum) 0);
    }

    PthreadCreate(&kvStorageThread, NULL, KVStorageThreadFun, NULL, __func__);
}
