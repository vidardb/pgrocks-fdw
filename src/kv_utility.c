/* Copyright 2019-present VidarDB Inc. All rights reserved.
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


#include <sys/stat.h>

#include "kv_fdw.h"
#include "server/kv_storage.h"

#include "foreign/foreign.h"
#include "miscadmin.h"
#include "commands/event_trigger.h"
#include "tcop/utility.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "commands/defrem.h"
#include "utils/rel.h"
#include "commands/copy.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "executor/executor.h"
#include "utils/typcache.h"
#include "commands/dbcommands.h"
#include "access/table.h"


/* Defines */
#define KVFDWNAME             "kv_fdw"
#define HEADERBUFFSIZE        10
#define OPTION_FILENAME       "filename"
#ifdef VIDARDB
#define COLUMNSTORE           "column"
#define BATCHCAPACITY         8*1024*1024
#define OPTION_STORAGE_FORMAT "storage"
#define OPTION_BATCH_CAPACITY "batch"
#endif


/* Forward Declaration */
void _PG_init(void);
void _PG_fini(void);


/* saved dropped object info */
typedef struct DroppedObject {
    Oid   objectId;
    char* path;
} DroppedObject;


#define PREVIOUS_UTILITY (PreviousProcessUtilityHook != NULL ? \
                          PreviousProcessUtilityHook : standard_ProcessUtility)

#define CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, \
                              destReceiver, qc) \
        PREVIOUS_UTILITY(plannedStmt, queryString, context, paramListInfo, \
                         queryEnvironment, destReceiver, qc)


/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(kv_ddl_event_end_trigger);

/* saved hook value in case of unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

/* local functions forward declarations */
static void KVProcessUtility(PlannedStmt* plannedStmt, const char* queryString,
                             ProcessUtilityContext context,
                             ParamListInfo paramListInfo,
                             QueryEnvironment* queryEnvironment,
                             DestReceiver* destReceiver, QueryCompletion* qc);

/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void _PG_init(void) {
    PreviousProcessUtilityHook = ProcessUtility_hook;
    ProcessUtility_hook = KVProcessUtility;

    LaunchKVManager();
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void _PG_fini(void) {
    ProcessUtility_hook = PreviousProcessUtilityHook;
}

/* Checks if a directory exists for the given directory name. */
static bool KVDirectoryExists(StringInfo directoryName) {
    bool directoryExists = true;
    struct stat directoryStat;
    if (stat(directoryName->data, &directoryStat) == 0) {
        /* file already exists; check that it is a directory */
        if (!S_ISDIR(directoryStat.st_mode)) {
            ereport(ERROR,
                    errmsg("\"%s\" is not a directory", directoryName->data),
                    errhint("You need to remove or rename the file \"%s\".",
                            directoryName->data));
        }
    } else {
        if (errno == ENOENT) {
            directoryExists = false;
        } else {
            ereport(ERROR,
                    errcode_for_file_access(),
                    errmsg("could not stat directory \"%s\": %m",
                           directoryName->data));
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
            ereport(ERROR, errcode_for_file_access(),
                           errmsg("could not create directory \"%s\": %m",
                                  directoryPath->data));
        }
    }

    StringInfo databaseDirectoryPath = makeStringInfo();
    appendStringInfo(databaseDirectoryPath, "%s/%s/%u", DataDir, KVFDWNAME,
                     databaseOid);
    if (!KVDirectoryExists(databaseDirectoryPath)) {
        if (mkdir(databaseDirectoryPath->data, S_IRWXU) != 0) {
            ereport(ERROR, errcode_for_file_access(),
                           errmsg("could not create directory \"%s\": %m",
                                  databaseDirectoryPath->data));
        }
    }
}

/*
 * Checks if the given foreign server belongs to kv_fdw. If it
 * does, the function returns true. Otherwise, it returns false.
 */
static bool KVServer(ForeignServer* server) {
    char* fdwName = GetForeignDataWrapper(server->fdwid)->fdwname;
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
        ForeignTable* foreignTable = GetForeignTable(relationId);
        ForeignServer* server = GetForeignServer(foreignTable->serverid);

        if (KVServer(server)) {
            return true;
        }
    }

    return false;
}

/*
 * Constructs the default file path to use for a kv_fdw table.
 * The path is of the form $PGDATA/kv_fdw/{databaseOid}/{relfilenode}.
 */
static char* KVDefaultFilePath(Oid foreignTableId) {
    StringInfo filePath = makeStringInfo();
    appendStringInfo(filePath, "%s/%s/%u/%u", DataDir, KVFDWNAME, MyDatabaseId,
                     foreignTableId);

    return filePath->data;
}

/*
 * Walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from mongo_fdw.
 */
static char* KVGetOptionValue(Oid foreignTableId, const char* optionName) {
    ForeignTable* foreignTable = GetForeignTable(foreignTableId);
    ForeignServer* foreignServer = GetForeignServer(foreignTable->serverid);

    List* optionList = NIL;
    optionList = list_concat(optionList, foreignTable->options);
    optionList = list_concat(optionList, foreignServer->options);

    ListCell* optionCell = NULL;
    foreach(optionCell, optionList) {
        DefElem* optionDef = (DefElem*) lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0) {
            return defGetString(optionDef);
        }
    }

    return NULL;
}

/*
 * Returns the option values to be used when reading and writing
 * the files. To resolve these values, the function checks options for the
 * foreign table, and if not present, falls back to default values.
 * This function errors out if given option values are considered invalid.
 */
KVFdwOptions* KVGetOptions(Oid foreignTableId) {
    KVFdwOptions* options = palloc0(sizeof(KVFdwOptions));

    char* filename = KVGetOptionValue(foreignTableId, OPTION_FILENAME);
    /* set default filename if it is not provided */
    options->filename = filename ? filename : KVDefaultFilePath(foreignTableId);

    #ifdef VIDARDB
    char* storage = KVGetOptionValue(foreignTableId, OPTION_STORAGE_FORMAT);
    options->useColumn = storage ?
        (0 == strncmp(storage, COLUMNSTORE, sizeof(COLUMNSTORE))) : false;

    char* capacity = KVGetOptionValue(foreignTableId, OPTION_BATCH_CAPACITY);
    options->batchCapacity = capacity ?
        pg_atoi(capacity, sizeof(int32), 0) : BATCHCAPACITY;
    #endif

    return options;
}

/*
 * kv_ddl_event_end_trigger is the event trigger function which is called on
 * ddl_command_end event. This function creates required directories after the
 * CREATE SERVER statement and after the CREATE FOREIGN TABLE statement.
 */
Datum kv_ddl_event_end_trigger(PG_FUNCTION_ARGS) {
    /* error if event trigger manager did not call this function */
    if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) {
        ereport(ERROR, errmsg("trigger not fired by event trigger manager"));
    }

    EventTriggerData* triggerData = (EventTriggerData*) fcinfo->context;
    Node* parseTree = triggerData->parsetree;

    if (nodeTag(parseTree) == T_CreateForeignServerStmt) {
        CreateForeignServerStmt* serverStmt = (CreateForeignServerStmt*) parseTree;
        if (strncmp(serverStmt->fdwname, KVFDWNAME, NAMEDATALEN) == 0) {
            KVCreateDatabaseDirectory(MyDatabaseId);
        }
    } else if (nodeTag(parseTree) == T_CreateForeignTableStmt) {
        CreateForeignTableStmt* tableStmt = (CreateForeignTableStmt*) parseTree;
        ForeignServer* server = GetForeignServerByName(tableStmt->servername,
                                                       false);
        if (KVServer(server)) {
            Oid relationId = RangeVarGetRelid(tableStmt->base.relation,
                                              AccessShareLock, false);

            Relation relation = table_open(relationId, AccessExclusiveLock);
            /*
             * Make sure database directory exists before creating a table.
             * This is necessary when a foreign server is created inside
             * a template database and a new database is created out of it.
             * We have no chance to hook into server creation to create data
             * directory for it during database creation time.
             */
            KVCreateDatabaseDirectory(MyDatabaseId);

            StringInfo kvPath = makeStringInfo();
            appendStringInfo(kvPath, "%s/%s/%u/%u", DataDir, KVFDWNAME,
                             MyDatabaseId, relationId);

            ComparatorOpts opts;
            SetRelationComparatorOpts(relation, &opts);

            /* Initialize the database */
            #ifdef VIDARDB
            char* option = KVGetOptionValue(relationId, OPTION_STORAGE_FORMAT);
            bool useColumn = (option != NULL) ?
                (0 == strncmp(option, COLUMNSTORE, sizeof(COLUMNSTORE))): false;
            TupleDesc tupleDescriptor = RelationGetDescr(relation);
            void* kvDB = OpenConn(kvPath->data, useColumn,
                                  tupleDescriptor->natts, &opts);
            #else
            void* kvDB = OpenConn(kvPath->data, &opts);
            #endif
            CloseConn(kvDB);

            table_close(relation, AccessExclusiveLock);
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
    appendStringInfo(databaseDirectoryPath, "%s/%s/%u", DataDir, KVFDWNAME,
                     databaseOid);

    if (KVDirectoryExists(databaseDirectoryPath)) {
        rmtree(databaseDirectoryPath->data, true);
    }
}

/*
 * Extracts and returns the list of kv file (directory) names from DROP table
 * statement.
 */
static List* KVDroppedFilenameList(DropStmt* dropStmt) {
    List* droppedFileList = NIL;
    if (dropStmt->removeType == OBJECT_FOREIGN_TABLE) {

        ListCell* dropObjectCell = NULL;
        foreach(dropObjectCell, dropStmt->objects) {

            List* tableNameList = (List*) lfirst(dropObjectCell);
            RangeVar* rangeVar = makeRangeVarFromNameList(tableNameList);
            Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);

            if (KVTable(relationId)) {
                DroppedObject* obj = palloc0(sizeof(DroppedObject));
                obj->objectId = relationId;
                obj->path = KVDefaultFilePath(relationId);
                droppedFileList = lappend(droppedFileList, obj);
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

    Oid relationId = RangeVarGetRelid(copyStmt->relation, AccessShareLock, true);
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
            ereport(ERROR, errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("must be superuser to COPY to or from a program"),
                    errhint("Anyone can COPY to stdout or from stdin. "
                            "psql's \\copy command also works for anyone."));
        } else {
            ereport(ERROR, errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("must be superuser to COPY to or from a file"),
                    errhint("Anyone can COPY to stdout or from stdin. "
                            "psql's \\copy command also works for anyone."));
        }
    }
}

static Datum ShortVarlena(Datum datum, int typeLength, char storage) {
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

/* copied from the storage engine */
static inline char* EncodeVarint64(char* dst, uint64 v) {
    static const unsigned int B = 128;
    unsigned char* ptr = (unsigned char*) dst;
    while (v >= B) {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = (unsigned char) v;
    return (char*) ptr;
}

static uint8 EncodeVarintLength(uint64 len, char* buf) {
    char* ptr = EncodeVarint64(buf, len);
    return (ptr - buf);
}

/* copied from the storage engine */
static inline const char* GetVarint64Ptr(const char* p, const char* limit,
                                         uint64* value) {
    uint64 result = 0;
    for (uint32 shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64 byte = *((const unsigned char*) p);
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return (const char*) p;
        }
    }
    return NULL;
}

static uint8 DecodeVarintLength(char* start, char* limit, uint64* len) {
    const char* ret = GetVarint64Ptr(start, limit, len);
    return ret ? (ret - start) : 0;
}

void SerializeNullAttribute(TupleDesc tupleDescriptor, Index index,
                            StringInfo buffer) {
    enlargeStringInfo(buffer, buffer->len + HEADERBUFFSIZE);
    char* current = buffer->data + buffer->len;
    memset(current, 0, HEADERBUFFSIZE);
    uint8 headerLen = EncodeVarintLength(0, current);
    buffer->len += headerLen;
}

void SerializeAttribute(TupleDesc tupleDescriptor, Index index, Datum datum,
                        StringInfo buffer) {
    Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, index);
    bool byValue = attributeForm->attbyval;
    int typeLength = attributeForm->attlen;
    char storage = attributeForm->attstorage;

    /* copy utility gets varlena with 4B header, same with constant */
    datum = ShortVarlena(datum, typeLength, storage);

    int offset = buffer->len;
    int datumLength = att_addlength_datum(offset, typeLength, datum);

    /* the key does not have a size header */ 
    enlargeStringInfo(buffer, datumLength + (index == 0 ? 0 : HEADERBUFFSIZE));

    char* current = buffer->data + buffer->len;
    memset(current, 0, datumLength - offset + (index == 0 ? 0 : HEADERBUFFSIZE));

    /* set the size header */
    uint8 headerLen = 0;
    if (index > 0) {
        uint64 dataLen = typeLength > 0 ? typeLength : (datumLength - offset);
        headerLen = EncodeVarintLength(dataLen, current);
        current += headerLen;
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

    buffer->len = datumLength + headerLen;
}

/*
 * Return the next start offset.
 * After processing the first attribute, it should return 0 since the key and
 * the value are separated.
 */
int DeserializeAttribute(TupleDesc tupleDescriptor, Index index, int offset,
                         char* key, char* val, char* limit, Datum* values,
                         bool* nulls) {
    char* current = (index == 0) ? key : val;
    current += offset; /* let the current point to the offset pos */

    if (index > 0) {
        uint64 dataLen = 0;
        uint8 headerLen = DecodeVarintLength(current, limit, &dataLen);
        offset += headerLen;
        current += headerLen;
        nulls[index] = (dataLen == 0) ? true : false;
        if (dataLen == 0) {
            return offset;
        }
    }

    Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, index);
    bool byValue = attributeForm->attbyval;
    int typeLength = attributeForm->attlen;

    values[index] = fetch_att(current, byValue, typeLength);
    offset = att_addlength_datum(offset, typeLength, current);

    if (index == 0) {
        offset = 0;
        nulls[index] = false;
    }

    return offset;
}

void SetRelationComparatorOpts(Relation relation, ComparatorOpts* opts) {
    TupleDesc tupleDescriptor = RelationGetDescr(relation);

    /* TODO: we assume the 1st column is primary key */
    FormData_pg_attribute* key = TupleDescAttr(tupleDescriptor, 0);
    opts->attrByVal = key->attbyval;
    opts->attrLength = key->attlen;
    opts->attrCollOid = key->attcollation;

    TypeCacheEntry* typeEntry = lookup_type_cache(key->atttypid,
                                                  TYPECACHE_CMP_PROC_FINFO);
    opts->cmpFuncOid = typeEntry->cmp_proc; /* maybe not exist */
}

/*
 * Handles a "COPY kv_table FROM" statement. This function uses the COPY
 * command's functions to read and parse rows from the data source specified
 * in the COPY statement. The function then writes each row to the file
 * specified in the foreign table options. Finally, the function returns the
 * number of copied rows.
 */
static uint64 KVCopyIntoTable(const CopyStmt* copyStmt, const char* queryString) {
    /* Only superuser can copy from or to local file */
    KVCheckSuperuserPrivilegesForCopy(copyStmt);

    /*
     * Open and lock the relation. We acquire ShareUpdateExclusiveLock to allow
     * concurrent reads, but block concurrent writes.
     */
    Relation relation = table_openrv(copyStmt->relation,
                                     ShareUpdateExclusiveLock);

    /* init state to read from COPY data source */
    ParseState* pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;
    CopyState copyState = BeginCopyFrom(pstate, relation, copyStmt->filename,
                                        copyStmt->is_program, NULL,
                                        copyStmt->attlist, copyStmt->options);
    free_parsestate(pstate);

    Oid relationId = RelationGetRelid(relation);
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    int attrCount = tupleDescriptor->natts;

    OpenArgs args;
    SetRelationComparatorOpts(relation, &args.opts);
    KVFdwOptions* fdwOptions = KVGetOptions(relationId);
    args.path = fdwOptions->filename;

    #ifdef VIDARDB
    char* option = KVGetOptionValue(relationId, OPTION_STORAGE_FORMAT);
    bool useColumn = (option != NULL) ?
        (0 == strncmp(option, COLUMNSTORE, sizeof(COLUMNSTORE))): false;
    args.useColumn = useColumn;
    args.attrCount = attrCount;
    #endif
    KVOpenRequest(relationId, &args);

    Datum* values = palloc0(attrCount * sizeof(Datum));
    bool* nulls = palloc0(attrCount * sizeof(bool));

    EState* estate = CreateExecutorState();
    ExprContext* econtext = GetPerTupleExprContext(estate);

    uint64 rowCount = 0;
    bool found = true;
    while (found) {
        /* read the next row in tupleContext */
        MemoryContext oldContext =
            MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

        /*
         * 'econtext' is used to evaluate default expression for each columns
         * not read from the file. It can be NULL when no default values are
         * used, i.e. when all columns are read from the file.
         */
        found = NextCopyFrom(copyState, econtext, values, nulls);

        /* write the row to the kv file */
        if (found) {
            StringInfo key = makeStringInfo();
            StringInfo val = makeStringInfo();

            for (int index = 0; index < attrCount; index++) {
                Datum datum = values[index];
                if (nulls[index]) {
                    if (index == 0) {
                        ereport(ERROR, errmsg("first column cannot be null!"));
                    }
                    SerializeNullAttribute(tupleDescriptor, index, val);
                } else {
                    SerializeAttribute(tupleDescriptor, index, datum,
                                       index == 0 ? key : val);
                }
            }

            PutArgs args;
            args.keyLen = key->len;
            args.valLen = val->len;
            args.key = key->data;
            args.val = val->data;
            KVLoadRequest(relationId, &args);

            rowCount++;
        }

        MemoryContextSwitchTo(oldContext);
        /*
         * Reset the per-tuple exprcontext. We do this after every tuple, to
         * clean-up after expression evaluations etc.
         */
        ResetPerTupleExprContext(estate);
        CHECK_FOR_INTERRUPTS();
    }

    /* end read/write sessions and close the relation */
    EndCopyFrom(copyState);
    KVCloseRequest(relationId);
    table_close(relation, ShareUpdateExclusiveLock);

    return rowCount;
}

/*
 * Handles a "COPY kv_table TO ..." statement. Statement is converted to
 * "COPY (SELECT * FROM kv_table) TO ..." and forwarded to Postgres native
 * COPY handler. Function returns number of files copied to external stream.
 */
static uint64 KVCopyOutTable(CopyStmt* copyStmt, const char* queryString) {
    if (copyStmt->attlist != NIL) {
        ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("copy column list is not supported"),
                       errhint("use 'copy (select <columns> from <table>) to "
                                "...' instead"));
    }

    RangeVar* relation = copyStmt->relation;
    char* qualifiedName = quote_qualified_identifier(relation->schemaname,
                                                     relation->relname);
    StringInfo newQuerySubstring = makeStringInfo();
    appendStringInfo(newQuerySubstring, "select * from %s", qualifiedName);
    List* queryList = raw_parser(newQuerySubstring->data);

    /* take the first parse tree */
    Node* rawQuery = linitial(queryList);

    /*
     * Set the relation field to NULL so that COPY command works on
     * query field instead.
     */
    copyStmt->relation = NULL;

    /*
     * raw_parser returns list of RawStmt* in PG 10+ we need to
     * extract actual query from it.
     */
    RawStmt* rawStmt = (RawStmt*) rawQuery;

    ParseState* pstate = make_parsestate(NULL);
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
static void KVCheckAlterTable(AlterTableStmt* alterStmt) {
    ObjectType objectType = alterStmt->relkind;
    /* we are only interested in foreign table changes */
    if (objectType != OBJECT_TABLE && objectType != OBJECT_FOREIGN_TABLE) {
        return;
    }

    RangeVar* rangeVar = alterStmt->relation;
    Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);
    if (!KVTable(relationId)) {
        return;
    }

    ListCell* cmdCell = NULL;
    List* cmdList = alterStmt->cmds;
    foreach (cmdCell, cmdList) {

        AlterTableCmd* alterCmd = (AlterTableCmd*) lfirst(cmdCell);
        if (alterCmd->subtype == AT_AlterColumnType) {

            char* columnName = alterCmd->name;
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
            ColumnDef* columnDef = (ColumnDef*) alterCmd->def;
            Oid targetTypeId = typenameTypeId(NULL, columnDef->typeName);
            if (!can_coerce_type(1, &currentTypeId, &targetTypeId,
                                 COERCION_IMPLICIT)) {
                char* typeName = TypeNameToString(columnDef->typeName);
                ereport(ERROR, errmsg("Column %s cannot be cast automatically "
                                      "to type %s", columnName, typeName));
            }
        }

        if (alterCmd->subtype == AT_AddColumn) {
            ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("No support for adding column currently"));
        }
    }
}

/*
 * Hook for handling utility commands. This function customizes the behavior of
 * "COPY kv_table" and "DROP FOREIGN TABLE " commands. For all other utility
 * statements, the function calls the previous utility hook or the standard
 * utility command via macro CALL_PREVIOUS_UTILITY.
 */
static void KVProcessUtility(PlannedStmt* plannedStmt, const char* queryString,
                             ProcessUtilityContext context,
                             ParamListInfo paramListInfo,
                             QueryEnvironment* queryEnvironment,
                             DestReceiver* destReceiver,
                             QueryCompletion* qc) {
    Node* parseTree = plannedStmt->utilityStmt;
    if (nodeTag(parseTree) == T_CopyStmt) {

        CopyStmt* copyStmt = (CopyStmt*) parseTree;
        if (KVCopyTableStatement(copyStmt)) {

            uint64 rowCount = 0;
            if (copyStmt->is_from) {
                rowCount = KVCopyIntoTable(copyStmt, queryString);
            } else {
                rowCount = KVCopyOutTable(copyStmt, queryString);
            }

            if (qc != NULL) {
                qc->commandTag = copyStmt->is_from ? CMDTAG_COPY_FROM :
                                                     CMDTAG_COPY;
                qc->nprocessed = rowCount;
            }
        } else {
            CALL_PREVIOUS_UTILITY(parseTree, queryString, context,
                                  paramListInfo, destReceiver, qc);
        }
    } else if (nodeTag(parseTree) == T_DropStmt) {

        DropStmt* dropStmt = (DropStmt*) parseTree;
        if (dropStmt->removeType == OBJECT_EXTENSION) {
            /* drop extension */
            bool removeDirectory = false;
            ListCell* objectCell = NULL;
            foreach(objectCell, dropStmt->objects) {

                Node* object = (Node*) lfirst(objectCell);
                Assert(IsA(object, String));
                char* objectName = strVal(object);
                if (strncmp(KVFDWNAME, objectName, NAMEDATALEN) == 0) {
                    removeDirectory = true;
                }
            }

            CALL_PREVIOUS_UTILITY(parseTree, queryString, context,
                                  paramListInfo, destReceiver, qc);

            if (removeDirectory) {
                KVRemoveDatabaseDirectory(MyDatabaseId);
                KVTerminateRequest(KVAllRelationId, MyDatabaseId);
            }
        } else {
            /* drop table & drop server */
            List* droppedTables = KVDroppedFilenameList((DropStmt*) parseTree);

            /* delete metadata */
            CALL_PREVIOUS_UTILITY(parseTree, queryString, context,
                                  paramListInfo, destReceiver, qc);

            /* delete real data and worker */
            ListCell* fileCell = NULL;
            foreach(fileCell, droppedTables) {
                DroppedObject* obj = lfirst(fileCell);
                StringInfo tablePath = makeStringInfo();
                appendStringInfo(tablePath, "%s", obj->path);
                if (KVDirectoryExists(tablePath)) {
                    rmtree(obj->path, true);
                }

                KVTerminateRequest(obj->objectId, MyDatabaseId);
            }
        }
    } else if (nodeTag(parseTree) == T_AlterTableStmt) {
        AlterTableStmt* alterStmt = (AlterTableStmt*) parseTree;
        KVCheckAlterTable(alterStmt);
        CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo,
                              destReceiver, qc);
    } else if (nodeTag(parseTree) == T_DropdbStmt) {
        DropdbStmt* dropdbStmt = (DropdbStmt*) parseTree;
        Oid dbId = get_database_oid(dropdbStmt->dbname, true);
        /* simple check, can't drop my own database */
        if (dbId == MyDatabaseId) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                            errmsg("cannot drop the currently open database")));
        }

        /* delete worker */
        if (OidIsValid(dbId)) {
            KVTerminateRequest(KVAllRelationId, dbId);
        }

        /* delete metadata */
        CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo,
                              destReceiver, qc);

        /* delete database from kv_fdw directory */
        if (OidIsValid(dbId)) {
            KVRemoveDatabaseDirectory(dbId);
        }
    } else {
        /* handle other utility statements */
        CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo,
                              destReceiver, qc);
    }
}
