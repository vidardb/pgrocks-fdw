
#include <src/kv_utility.h>
#include "postgres.h"
#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

//taken from redis_fdw
#define PROCID_TEXTEQ 67

PG_FUNCTION_INFO_V1(kv_fdw_handler);

/*
 * The plan state is set up in GetForeignRelSize and stashed away in
 * baserel->fdw_private and fetched in GetForeignPaths.
 */
typedef struct {
    void *db;
} TablePlanState;

/*
 * The scan state is for maintaining state for a scan, either for a
 * SELECT or UPDATE or DELETE.
 *
 * It is set up in BeginForeignScan and stashed in node->fdw_state and
 * subsequently used in IterateForeignScan, EndForeignScan and ReScanForeignScan.
 */
typedef struct {
    void *db;
    void *iter;
    bool keyBasedQual;
    char *keyBasedQualValue;
    bool keyBasedQualSent;
    AttInMetadata *attinmeta;
} TableReadState;

/*
 * The modify state is for maintaining state of modify operations.
 *
 * It is set up in BeginForeignModify and stashed in
 * rinfo->ri_FdwState and subsequently used in ExecForeignInsert,
 * ExecForeignUpdate, ExecForeignDelete and EndForeignModify.
 */
typedef struct {
    void *db;
    Relation rel;
    FmgrInfo *keyInfo;
    FmgrInfo *valueInfo;
    AttrNumber keyJunkNo;
} TableWriteState;


static void GetForeignRelSize(PlannerInfo *root,
                              RelOptInfo *baserel,
                              Oid foreignTableId) {
    printf("\n-----------------GetForeignRelSize----------------------\n");
    /*
     * Obtain relation size estimates for a foreign table. This is called at
     * the beginning of planning for a query that scans a foreign table. root
     * is the planner's global information about the query; baserel is the
     * planner's information about this table; and foreigntableid is the
     * pg_class OID of the foreign table. (foreigntableid could be obtained
     * from the planner data structures, but it's passed explicitly to save
     * effort.)
     *
     * This function should update baserel->rows to be the expected number of
     * rows returned by the table scan, after accounting for the filtering
     * done by the restriction quals. The initial value of baserel->rows is
     * just a constant default estimate, which should be replaced if at all
     * possible. The function may also choose to update baserel->width if it
     * can compute a better estimate of the average result row width.
     */

    elog(DEBUG1, "entering function %s", __func__);

    TablePlanState *planState = palloc0(sizeof(TablePlanState));

    FdwOptions *fdwOptions = KVGetOptions(foreignTableId);
    planState->db = Open(fdwOptions->filename);

    baserel->fdw_private = (void *) planState;

    baserel->rows = Count(planState->db);
}

static void GetForeignPaths(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreignTableId) {
    printf("\n-----------------GetForeignPaths----------------------\n");
    /*
     * Create possible access paths for a scan on a foreign table. This is
     * called during query planning. The parameters are the same as for
     * GetForeignRelSize, which has already been called.
     *
     * This function must generate at least one access path (ForeignPath node)
     * for a scan on the foreign table and must call add_path to add each such
     * path to baserel->pathlist. It's recommended to use
     * create_foreignscan_path to build the ForeignPath nodes. The function
     * can generate multiple access paths, e.g., a path which has valid
     * pathkeys to represent a pre-sorted result. Each access path must
     * contain cost estimates, and can contain any FDW-private information
     * that is needed to identify the specific scan method intended.
     */

    elog(DEBUG1, "entering function %s", __func__);

    Cost startupCost = 0;
    Cost totalCost = startupCost + baserel->rows;

    /* Create a ForeignPath node and add it as only possible path */
    add_path(baserel,
             (Path *) create_foreignscan_path(root,
                                              baserel,
                                              NULL, /* default pathtarget */
                                              baserel->rows,
                                              startupCost,
                                              totalCost,
                                              NIL, /* no pathkeys */
                                              NULL, /* no outer rel either */
                                              NULL, /* no extra plan */
                                              NIL)); /* no fdw_private data */
}

static ForeignScan *GetForeignPlan(PlannerInfo *root,
                                   RelOptInfo *baserel,
                                   Oid foreignTableId,
                                   ForeignPath *bestPath,
                                   List *targetList,
                                   List *scanClauses,
                                   Plan *outerPlan) {
    printf("\n-----------------GetForeignPlan----------------------\n");
    /*
     * Create a ForeignScan plan node from the selected foreign access path.
     * This is called at the end of query planning. The parameters are as for
     * GetForeignRelSize, plus the selected ForeignPath (previously produced
     * by GetForeignPaths), the target list to be emitted by the plan node,
     * and the restriction clauses to be enforced by the plan node.
     *
     * This function must create and return a ForeignScan plan node; it's
     * recommended to use make_foreignscan to build the ForeignScan node.
     *
     */

    elog(DEBUG1, "entering function %s", __func__);

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check. So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */

    scanClauses = extract_actual_clauses(scanClauses, false);

    /*
     * Build the fdw_private list that will be available to the executor.
     */
    TablePlanState *planState = (TablePlanState *) baserel->fdw_private;
    List *fdwPrivate = list_make1(planState->db);

    /* Create the ForeignScan node */
    return make_foreignscan(targetList,
                            scanClauses,
                            baserel->relid,
                            NIL, /* no expressions to evaluate */
                            fdwPrivate,
                            NIL, /* no custom tlist */
                            NIL, /* no remote quals */
                            NULL);
}

static void GetKeyBasedQual(Node *node,
                            TupleDesc tupdesc,
                            char **value,
                            bool *keyBasedQual) {
    char *key = NULL;
    *value = NULL;
    *keyBasedQual = false;

    if (!node) {
        return;
    }

    if (IsA(node, OpExpr)) {
        OpExpr *op = (OpExpr *) node;
        if (list_length(op->args) != 2) {
            return;
        }

        Node *left = list_nth(op->args, 0);
        if (!IsA(left, Var)) {
            return;
        }
        Index varattno = ((Var *) left)->varattno;

        Node *right = list_nth(op->args, 1);
        if (IsA(right, Const)) {
            StringInfoData buf;
            initStringInfo(&buf);

            /* And get the column and value... */
            key = NameStr(tupdesc->attrs[varattno - 1].attname);
            *value = TextDatumGetCString(((Const *) right)->constvalue);
            /*
             * We can push down this qual if: - The operatory is TEXTEQ - The
             * qual is on the key column
             */
            printf("PROCID_TEXTEQ");
            if (op->opfuncid == PROCID_TEXTEQ && strcmp(key, "key") == 0) {
                *keyBasedQual = true;
            }
            return;
        }
    }

    return;
}

static void BeginForeignScan(ForeignScanState *scanState, int executorFlags) {
    printf("\n-----------------BeginForeignScan----------------------\n");
    /*
     * Begin executing a foreign scan. This is called during executor startup.
     * It should perform any initialization needed before the scan can start,
     * but not start executing the actual scan (that should be done upon the
     * first call to IterateForeignScan). The ForeignScanState node has
     * already been created, but its fdw_state field is still NULL.
     * Information about the table to scan is accessible through the
     * ForeignScanState node (in particular, from the underlying ForeignScan
     * plan node, which contains any FDW-private information provided by
     * GetForeignPlan). eflags contains flag bits describing the executor's
     * operating mode for this plan node.
     *
     * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
     * should not perform any externally-visible actions; it should only do
     * the minimum required to make the node state valid for
     * ExplainForeignScan and EndForeignScan.
     *
     */

    elog(DEBUG1, "entering function %s", __func__);

    if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    TableReadState *readState = palloc0(sizeof(TableReadState));

    ForeignScan *foreignScan = (ForeignScan *) scanState->ss.ps.plan;
    readState->db = list_nth((List *) foreignScan->fdw_private, 0);

    readState->iter = NULL;
    readState->keyBasedQual = false;
    readState->keyBasedQualValue = NULL;
    readState->keyBasedQualSent = false;
    readState->attinmeta =
            TupleDescGetAttInMetadata(scanState->ss.ss_currentRelation->rd_att);

    scanState->fdw_state = (void *) readState;

    if (scanState->ss.ps.plan->qual) {
        printf("\nqual\n");
        ListCell *lc;
        foreach (lc, scanState->ss.ps.plan->qual) {
            /* Only the first qual can be pushed down */
            printf("\nOnly the first qual can be pushed down\n");
            Expr *state = lfirst(lc);
            GetKeyBasedQual((Node *) state,
                            scanState->ss.ss_currentRelation->rd_att,
                            &readState->keyBasedQualValue,
                            &readState->keyBasedQual);
            if (readState->keyBasedQual) {
                printf("\nkey_based_qual\n");
                break;
            }
        }
    }

    if (!readState->keyBasedQual) {
        readState->iter = GetIter(readState->db);
    }
}

static TupleTableSlot *IterateForeignScan(ForeignScanState *scanState) {
    printf("\n-----------------IterateForeignScan----------------------\n");
    /*
     * Fetch one row from the foreign source, returning it in a tuple table
     * slot (the node's ScanTupleSlot should be used for this purpose). Return
     * NULL if no more rows are available. The tuple table slot infrastructure
     * allows either a physical or virtual tuple to be returned; in most cases
     * the latter choice is preferable from a performance standpoint. Note
     * that this is called in a short-lived memory context that will be reset
     * between invocations. Create a memory context in BeginForeignScan if you
     * need longer-lived storage, or use the es_query_cxt of the node's
     * EState.
     *
     * The rows returned must match the column signature of the foreign table
     * being scanned. If you choose to optimize away fetching columns that are
     * not needed, you should insert nulls in those column positions.
     *
     * Note that PostgreSQL's executor doesn't care whether the rows returned
     * violate any NOT NULL constraints that were defined on the foreign table
     * columns — but the planner does care, and may optimize queries
     * incorrectly if NULL values are present in a column declared not to
     * contain them. If a NULL value is encountered when the user has declared
     * that none should be present, it may be appropriate to raise an error
     * (just as you would need to do in the case of a data type mismatch).
     */

    elog(DEBUG1, "entering function %s", __func__);


    TupleTableSlot *slot = scanState->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);

    TableReadState *readState = (TableReadState *) scanState->fdw_state;

    bool found = false;
    char *key;
    char *value;

    /* get the next record, if any, and fill in the slot */
    if (readState->keyBasedQual) {
        if (!readState->keyBasedQualSent) {
            readState->keyBasedQualSent = true;
            found = Get(readState->db, readState->keyBasedQualValue, &value);
        }
    } else {
        found = Next(readState->db, readState->iter, &key, &value);
    }

    if (found) {
        char **values = (char **) palloc(sizeof(char *) * 2);
        if (readState->keyBasedQual) {
            values[0] = readState->keyBasedQualValue;
        } else {
            values[0] = key;
        }
        values[1] = value;
        HeapTuple tuple = BuildTupleFromCStrings(readState->attinmeta, values);
        ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    }

    /* then return the slot */
    return slot;
}

static void ReScanForeignScan(ForeignScanState *scanState) {
    printf("\n-----------------ReScanForeignScan----------------------\n");
    /*
     * Restart the scan from the beginning. Note that any parameters the scan
     * depends on may have changed value, so the new scan does not necessarily
     * return exactly the same rows.
     */

    elog(DEBUG1, "entering function %s", __func__);
}

static void EndForeignScan(ForeignScanState *scanState) {
    printf("\n-----------------EndForeignScan----------------------\n");
    /*
     * End the scan and release resources. It is normally not important to
     * release palloc'd memory, but for example open files and connections to
     * remote servers should be cleaned up.
     */

    elog(DEBUG1, "entering function %s", __func__);

    TableReadState *readState = (TableReadState *) scanState->fdw_state;

    if (readState) {
        if (readState->iter) {
            DelIter(readState->iter);
            readState->iter = NULL;
        }

        if (readState->db) {
            Close(readState->db);
            readState->db = NULL;
        }
    }
}

static void AddForeignUpdateTargets(Query *parsetree,
                                    RangeTblEntry *tableEntry,
                                    Relation targetRelation) {
    printf("\n-----------------AddForeignUpdateTargets----------------------\n");
    /*
     * UPDATE and DELETE operations are performed against rows previously
     * fetched by the table-scanning functions. The FDW may need extra
     * information, such as a row ID or the values of primary-key columns, to
     * ensure that it can identify the exact row to update or delete. To
     * support that, this function can add extra hidden, or "junk", target
     * columns to the list of columns that are to be retrieved from the
     * foreign table during an UPDATE or DELETE.
     *
     * To do that, add TargetEntry items to parsetree->targetList, containing
     * expressions for the extra values to be fetched. Each such entry must be
     * marked resjunk = true, and must have a distinct resname that will
     * identify it at execution time. Avoid using names matching ctidN or
     * wholerowN, as the core system can generate junk columns of these names.
     *
     * This function is called in the rewriter, not the planner, so the
     * information available is a bit different from that available to the
     * planning routines. parsetree is the parse tree for the UPDATE or DELETE
     * command, while target_rte and target_relation describe the target
     * foreign table.
     *
     * If the AddForeignUpdateTargets pointer is set to NULL, no extra target
     * expressions are added. (This will make it impossible to implement
     * DELETE operations, though UPDATE may still be feasible if the FDW
     * relies on an unchanging primary key to identify rows.)
     */

    elog(DEBUG1, "entering function %s", __func__);

    Form_pg_attribute attr = &RelationGetDescr(targetRelation)->attrs[0];

    Var *varnode = makeVar(parsetree->resultRelation,
                           attr->attnum,
                           attr->atttypid,
                           attr->atttypmod,
                           attr->attcollation,
                           0);
    /* Wrap it in a resjunk TLE with the right name ... */
    const char *attrname = "key_junk";
    TargetEntry *entry = makeTargetEntry((Expr *) varnode,
                                         list_length(parsetree->targetList) + 1,
                                         pstrdup(attrname),
                                         true);

    /* ... and add it to the query's targetlist */
    parsetree->targetList = lappend(parsetree->targetList, entry);
}

static List *PlanForeignModify(PlannerInfo *plannerInfo,
                               ModifyTable *plan,
                               Index resultRelation,
                               int subplanIndex) {
    printf("\n-----------------PlanForeignModify----------------------\n");
    /*
     * Perform any additional planning actions needed for an insert, update,
     * or delete on a foreign table. This function generates the FDW-private
     * information that will be attached to the ModifyTable plan node that
     * performs the update action. This private information must have the form
     * of a List, and will be delivered to BeginForeignModify during the
     * execution stage.
     *
     * root is the planner's global information about the query. plan is the
     * ModifyTable plan node, which is complete except for the fdwPrivLists
     * field. resultRelation identifies the target foreign table by its
     * rangetable index. subplan_index identifies which target of the
     * ModifyTable plan node this is, counting from zero; use this if you want
     * to index into plan->plans or other substructure of the plan node.
     *
     * If the PlanForeignModify pointer is set to NULL, no additional
     * plan-time actions are taken, and the fdw_private list delivered to
     * BeginForeignModify will be NIL.
     */

    elog(DEBUG1, "entering function %s", __func__);

    return NULL;
}

static void BeginForeignModify(ModifyTableState *modifyTableState,
                               ResultRelInfo *relationInfo,
                               List *fdwPrivate,
                               int subplanIndex,
                               int executorFlags) {
    printf("\n-----------------BeginForeignModify----------------------\n");
    /*
     * Begin executing a foreign table modification operation. This routine is
     * called during executor startup. It should perform any initialization
     * needed prior to the actual table modifications. Subsequently,
     * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
     * called for each tuple to be inserted, updated, or deleted.
     *
     * mtstate is the overall state of the ModifyTable plan node being
     * executed; global data about the plan and execution state is available
     * via this structure. rinfo is the ResultRelInfo struct describing the
     * target foreign table. (The ri_FdwState field of ResultRelInfo is
     * available for the FDW to store any private state it needs for this
     * operation.) fdw_private contains the private data generated by
     * PlanForeignModify, if any. subplan_index identifies which target of the
     * ModifyTable plan node this is. eflags contains flag bits describing the
     * executor's operating mode for this plan node.
     *
     * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
     * should not perform any externally-visible actions; it should only do
     * the minimum required to make the node state valid for
     * ExplainForeignModify and EndForeignModify.
     *
     * If the BeginForeignModify pointer is set to NULL, no action is taken
     * during executor startup.
     */

    elog(DEBUG1, "entering function %s", __func__);

    if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    TableWriteState *writeState = palloc0(sizeof(TableWriteState));

    Oid foreignTableId = RelationGetRelid(relationInfo->ri_RelationDesc);
    FdwOptions *fdwOptions = KVGetOptions(foreignTableId);
    writeState->db = Open(fdwOptions->filename);

    writeState->rel = relationInfo->ri_RelationDesc;
    writeState->keyInfo = palloc0(sizeof(FmgrInfo));
    writeState->valueInfo = palloc0(sizeof(FmgrInfo));

    CmdType operation = modifyTableState->operation;
    if (operation == CMD_UPDATE || operation == CMD_DELETE) {
        /* Find the ctid resjunk column in the subplan's result */
        Plan *subplan = modifyTableState->mt_plans[subplanIndex]->plan;
        writeState->keyJunkNo =
                ExecFindJunkAttributeInTlist(subplan->targetlist, "key_junk");
        if (!AttributeNumberIsValid(writeState->keyJunkNo)) {
            elog(ERROR, "could not find key junk column");
        }
    }

    Form_pg_attribute attr = &RelationGetDescr(writeState->rel)->attrs[0];
    Assert(!attr->attisdropped);
    Oid typefnoid;
    bool isvarlena;
    getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
    fmgr_info(typefnoid, writeState->keyInfo);

    attr = &RelationGetDescr(writeState->rel)->attrs[1];
    Assert(!attr->attisdropped);
    getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
    fmgr_info(typefnoid, writeState->valueInfo);

    relationInfo->ri_FdwState = (void *) writeState;
}

static TupleTableSlot *ExecForeignInsert(EState *executorState,
                                         ResultRelInfo *relationInfo,
                                         TupleTableSlot *tupleSlot,
                                         TupleTableSlot *planSlot) {
    printf("\n-----------------ExecForeignInsert----------------------\n");
    /*
     * Insert one tuple into the foreign table. estate is global execution
     * state for the query. rinfo is the ResultRelInfo struct describing the
     * target foreign table. slot contains the tuple to be inserted; it will
     * match the rowtype definition of the foreign table. planSlot contains
     * the tuple that was generated by the ModifyTable plan node's subplan; it
     * differs from slot in possibly containing additional "junk" columns.
     * (The planSlot is typically of little interest for INSERT cases, but is
     * provided for completeness.)
     *
     * The return value is either a slot containing the data that was actually
     * inserted (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually inserted
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the INSERT query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignInsert pointer is set to NULL, attempts to insert
     * into the foreign table will fail with an error message.
     */

    elog(DEBUG1, "entering function %s", __func__);

    TableWriteState *writeState = (TableWriteState *) relationInfo->ri_FdwState;

    bool isnull;
    Datum value = slot_getattr(planSlot, 1, &isnull);
    if (isnull) {
        elog(ERROR, "can't get key value");
    }
    char *keyValue = OutputFunctionCall(writeState->keyInfo, value);

    value = slot_getattr(planSlot, 2, &isnull);
    if (isnull) {
        elog(ERROR, "can't get value value");
    }
    char *valueValue = OutputFunctionCall(writeState->valueInfo, value);

    if (!Put(writeState->db, keyValue, valueValue)) {
        elog(ERROR, "Error from ExecForeignInsert");
    }
    return tupleSlot;
}

static TupleTableSlot *ExecForeignUpdate(EState *executorState,
                                         ResultRelInfo *relationInfo,
                                         TupleTableSlot *tupleSlot,
                                         TupleTableSlot *planSlot) {
    printf("\n-----------------ExecForeignUpdate----------------------\n");
    /*
     * Update one tuple in the foreign table. estate is global execution state
     * for the query. rinfo is the ResultRelInfo struct describing the target
     * foreign table. slot contains the new data for the tuple; it will match
     * the rowtype definition of the foreign table. planSlot contains the
     * tuple that was generated by the ModifyTable plan node's subplan; it
     * differs from slot in possibly containing additional "junk" columns. In
     * particular, any junk columns that were requested by
     * AddForeignUpdateTargets will be available from this slot.
     *
     * The return value is either a slot containing the row as it was actually
     * updated (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually updated
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the UPDATE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
     * foreign table will fail with an error message.
     *
     */

    elog(DEBUG1, "entering function %s", __func__);

    TableWriteState *writeState = (TableWriteState *) relationInfo->ri_FdwState;

    bool isnull;
    Datum value = ExecGetJunkAttribute(planSlot,
                                       writeState->keyJunkNo,
                                       &isnull);
    if (isnull) {
        elog(ERROR, "can't get junk key value");
    }
    char *keyValue = OutputFunctionCall(writeState->keyInfo, value);

    value = slot_getattr(planSlot, 1, &isnull);
    if (isnull) {
        elog(ERROR, "can't get new key value");
    }
    char *keyValueNew = OutputFunctionCall(writeState->keyInfo, value);
    if (strcmp(keyValue, keyValueNew) != 0) {
        elog(ERROR,
             "You cannot update key values (original key value was %s)",
             keyValue);
        return tupleSlot;
    }

    value = slot_getattr(planSlot, 2, &isnull);
    if (isnull) {
        elog(ERROR, "can't get value value");
    }
    char *valueValue = OutputFunctionCall(writeState->valueInfo, value);

    if (!Put(writeState->db, keyValue, valueValue)) {
        elog(ERROR, "Error from ExecForeignUpdate");
    }

    return tupleSlot;
}

static TupleTableSlot *ExecForeignDelete(EState *executorState,
                                         ResultRelInfo *relationInfo,
                                         TupleTableSlot *tupleSlot,
                                         TupleTableSlot *planSlot) {
    printf("\n-----------------ExecForeignDelete----------------------\n");
    /*
     * Delete one tuple from the foreign table. estate is global execution
     * state for the query. rinfo is the ResultRelInfo struct describing the
     * target foreign table. slot contains nothing useful upon call, but can
     * be used to hold the returned tuple. planSlot contains the tuple that
     * was generated by the ModifyTable plan node's subplan; in particular, it
     * will carry any junk columns that were requested by
     * AddForeignUpdateTargets. The junk column(s) must be used to identify
     * the tuple to be deleted.
     *
     * The return value is either a slot containing the row that was deleted,
     * or NULL if no row was deleted (typically as a result of triggers). The
     * passed-in slot can be used to hold the tuple to be returned.
     *
     * The data in the returned slot is used only if the DELETE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignDelete pointer is set to NULL, attempts to delete
     * from the foreign table will fail with an error message.
     */

    elog(DEBUG1, "entering function %s", __func__);

    TableWriteState *writeState = (TableWriteState *) relationInfo->ri_FdwState;

    bool isnull;
    Datum value = ExecGetJunkAttribute(planSlot, writeState->keyJunkNo, &isnull);
    if (isnull) {
        elog(ERROR, "can't get key value");
    }

    char *keyValue = OutputFunctionCall(writeState->keyInfo, value);

    if (!Delete(writeState->db, keyValue)) {
        elog(ERROR, "Error from ExecForeignDelete");
    }

    return tupleSlot;
}

static void EndForeignModify(EState *executorState, ResultRelInfo *relationInfo) {
    printf("\n-----------------EndForeignModify----------------------\n");
    /*
     * End the table update and release resources. It is normally not
     * important to release palloc'd memory, but for example open files and
     * connections to remote servers should be cleaned up.
     *
     * If the EndForeignModify pointer is set to NULL, no action is taken
     * during executor shutdown.
     */

    elog(DEBUG1, "entering function %s", __func__);

    TableWriteState *writeState = (TableWriteState *) relationInfo->ri_FdwState;

    if (writeState && writeState->db) {
        Close(writeState->db);
        writeState->db = NULL;
    }
}

static void ExplainForeignScan(ForeignScanState *scanState,
                               struct ExplainState * explainState) {
    printf("\n-----------------ExplainForeignScan----------------------\n");
    /*
     * Print additional EXPLAIN output for a foreign table scan. This function
     * can call ExplainPropertyText and related functions to add fields to the
     * EXPLAIN output. The flag fields in es can be used to determine what to
     * print, and the state of the ForeignScanState node can be inspected to
     * provide run-time statistics in the EXPLAIN ANALYZE case.
     *
     * If the ExplainForeignScan pointer is set to NULL, no additional
     * information is printed during EXPLAIN.
     */

    elog(DEBUG1, "entering function %s", __func__);
}

static void ExplainForeignModify(ModifyTableState *modifyTableState,
                                 ResultRelInfo *relationInfo,
                                 List *fdwPrivate,
                                 int subplanIndex,
                                 struct ExplainState *explainState) {
    printf("\n-----------------ExplainForeignModify----------------------\n");
    /*
     * Print additional EXPLAIN output for a foreign table update. This
     * function can call ExplainPropertyText and related functions to add
     * fields to the EXPLAIN output. The flag fields in es can be used to
     * determine what to print, and the state of the ModifyTableState node can
     * be inspected to provide run-time statistics in the EXPLAIN ANALYZE
     * case. The first four arguments are the same as for BeginForeignModify.
     *
     * If the ExplainForeignModify pointer is set to NULL, no additional
     * information is printed during EXPLAIN.
     */

    elog(DEBUG1, "entering function %s", __func__);
}

static bool AnalyzeForeignTable(Relation relation,
                                AcquireSampleRowsFunc *acquireSampleRowsFunc,
                                BlockNumber *totalPageCount) {
    printf("\n-----------------AnalyzeForeignTable----------------------\n");
    /* ----
     * This function is called when ANALYZE is executed on a foreign table. If
     * the FDW can collect statistics for this foreign table, it should return
     * true, and provide a pointer to a function that will collect sample rows
     * from the table in func, plus the estimated size of the table in pages
     * in totalpages. Otherwise, return false.
     *
     * If the FDW does not support collecting statistics for any tables, the
     * AnalyzeForeignTable pointer can be set to NULL.
     *
     * If provided, the sample collection function must have the signature:
     *
     *	  int
     *	  AcquireSampleRowsFunc (Relation relation, int elevel,
     *							 HeapTuple *rows, int targrows,
     *							 double *totalrows,
     *							 double *totaldeadrows);
     *
     * A random sample of up to targrows rows should be collected from the
     * table and stored into the caller-provided rows array. The actual number
     * of rows collected must be returned. In addition, store estimates of the
     * total numbers of live and dead rows in the table into the output
     * parameters totalrows and totaldeadrows. (Set totaldeadrows to zero if
     * the FDW does not have any concept of dead rows.)
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    return false;
}

Datum kv_fdw_handler(PG_FUNCTION_ARGS) {
    printf("\n-----------------fdw_handler----------------------\n");
    FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

    elog(DEBUG1, "entering function %s", __func__);

    /*
     * assign the handlers for the FDW
     *
     * This function might be called a number of times. In particular, it is
     * likely to be called for each INSERT statement. For an explanation, see
     * core postgres file src/optimizer/plan/createplan.c where it calls
     * GetFdwRoutineByRelId(().
     */

    /* Required by notations: S=SELECT I=INSERT U=UPDATE D=DELETE */

    /* these are required */
    fdwRoutine->GetForeignRelSize = GetForeignRelSize; /* S U D */
    fdwRoutine->GetForeignPaths = GetForeignPaths; /* S U D */
    fdwRoutine->GetForeignPlan = GetForeignPlan; /* S U D */
    fdwRoutine->BeginForeignScan = BeginForeignScan; /* S U D */
    fdwRoutine->IterateForeignScan = IterateForeignScan; /* S */
    fdwRoutine->ReScanForeignScan = ReScanForeignScan; /* S */
    fdwRoutine->EndForeignScan = EndForeignScan; /* S U D */

    /* remainder are optional - use NULL if not required */
    /* support for insert / update / delete */
    fdwRoutine->AddForeignUpdateTargets = AddForeignUpdateTargets; /* U D */
    fdwRoutine->PlanForeignModify = PlanForeignModify; /* I U D */
    fdwRoutine->BeginForeignModify = BeginForeignModify; /* I U D */
    fdwRoutine->ExecForeignInsert = ExecForeignInsert; /* I */
    fdwRoutine->ExecForeignUpdate = ExecForeignUpdate; /* U */
    fdwRoutine->ExecForeignDelete = ExecForeignDelete; /* D */
    fdwRoutine->EndForeignModify = EndForeignModify; /* I U D */

    /* support for EXPLAIN */
    fdwRoutine->ExplainForeignScan = ExplainForeignScan; /* EXPLAIN S U D */
    fdwRoutine->ExplainForeignModify = ExplainForeignModify; /* EXPLAIN I U D */

    /* support for ANALYSE */
    fdwRoutine->AnalyzeForeignTable = AnalyzeForeignTable; /* ANALYZE only */

    PG_RETURN_POINTER(fdwRoutine);
}
