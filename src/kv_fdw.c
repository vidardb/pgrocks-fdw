#include "kvapi.h"
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

/*
 * SQL functions
 */
extern Datum kv_fdw_handler(PG_FUNCTION_ARGS);
extern Datum kv_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(kv_fdw_handler);
PG_FUNCTION_INFO_V1(kv_fdw_validator);

/*
 * structures used by the FDW
 *
 * These next structures are not actually used, but something like
 * them will be needed by anything more complicated that does actual work.
 */

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct FdwOption {
    const char *optname;
    Oid optcontext; /* Oid of catalog in which option may appear */
};

/*
 * The plan state is set up in GetForeignRelSize and stashed away in
 * baserel->fdw_private and fetched in GetForeignPaths.
 */
typedef struct {
    char *foo;
    int bar;
} FdwPlanState;

/*
 * The scan state is for maintaining state for a scan, eiher for a
 * SELECT or UPDATE or DELETE.
 *
 * It is set up in BeginForeignScan and stashed in node->fdw_state
 * and subsequently used in IterateForeignScan,
 * EndForeignScan and ReScanForeignScan.
 */
typedef struct {
    void *db;
    void *iter;
    bool key_based_qual;
    char *key_based_qual_value;
    bool key_based_qual_sent;
    AttInMetadata *attinmeta;
} FdwScanState;

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
    FmgrInfo *key_info;
    FmgrInfo *value_info;
    AttrNumber key_junk_no;
} FdwModifyState;

static void *db = NULL;

static void GetForeignRelSize(PlannerInfo *root,
                              RelOptInfo *baserel,
                              Oid foreigntableid) {
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

    baserel->rows = 0;

    FdwPlanState *plan_state = palloc0(sizeof(FdwPlanState));
    baserel->fdw_private = (void *) plan_state;

    /* initialize required state in plan_state */
    if (!db) db = Open();
    baserel->rows = Count(db);
}

static void GetForeignPaths(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid) {
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

    /*
     * FdwPlanState *plan_state = baserel->fdw_private;
     */

    elog(DEBUG1, "entering function %s", __func__);

    Cost startup_cost = 0;
    Cost total_cost = startup_cost + baserel->rows;

    /* Create a ForeignPath node and add it as only possible path */
    add_path(baserel,
             (Path *) create_foreignscan_path(root,
                                              baserel,
                                              NULL, /* default pathtarget */
                                              baserel->rows,
                                              startup_cost,
                                              total_cost,
                                              NIL, /* no pathkeys */
                                              NULL, /* no outer rel either */
                                              NULL, /* no extra plan */
                                              NIL)); /* no fdw_private data */
}

static ForeignScan *GetForeignPlan(PlannerInfo *root,
                                   RelOptInfo *baserel,
                                   Oid foreigntableid,
                                   ForeignPath *best_path,
                                   List *tlist,
                                   List *scan_clauses,
                                   Plan *outer_plan) {
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
     * FdwPlanState *plan_state = baserel->fdw_private;
     */

    Index scan_relid = baserel->relid;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check. So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */

    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            NIL, /* no expressions to evaluate */
                            NIL, /* no private state either */
                            NIL, /* no custom tlist */
                            NIL, /* no remote quals */
                            outer_plan);
}

static void GetKeyBasedQual(Node *node,
                            TupleDesc tupdesc,
                            char **value,
                            bool *key_based_qual) {
    char *key = NULL;
    *value = NULL;
    *key_based_qual = false;

    if (!node) return;

    if (IsA(node, OpExpr)) {
        OpExpr *op = (OpExpr *) node;
        if (list_length(op->args) != 2) return;

        Node *left = list_nth(op->args, 0);
        if (!IsA(left, Var)) return;
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
            if (op->opfuncid == PROCID_TEXTEQ && strcmp(key, "key") == 0) {
                *key_based_qual = true;
            }
            return;
        }
    }

    return;
}

static void BeginForeignScan(ForeignScanState *node, int eflags) {
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

    FdwScanState *scan_state = palloc0(sizeof(FdwScanState));
    scan_state->db = db;
    scan_state->iter = NULL;
    scan_state->key_based_qual = false;
    scan_state->key_based_qual_value = NULL;
    scan_state->key_based_qual_sent = false;
    scan_state->attinmeta =
            TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);

    node->fdw_state = (void *) scan_state;

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

    if (node->ss.ps.plan->qual) {
        ListCell *lc;
        foreach (lc, node->ss.ps.qual) {
            /* Only the first qual can be pushed down */
            ExprState *state = lfirst(lc);
            GetKeyBasedQual((Node *) state->expr,
                            node->ss.ss_currentRelation->rd_att,
                            &scan_state->key_based_qual_value,
                            &scan_state->key_based_qual);
            if (scan_state->key_based_qual) break;
        }
    }

    if (!scan_state->key_based_qual) {
        scan_state->iter = GetIter(scan_state->db);
    }
}

static TupleTableSlot *IterateForeignScan(ForeignScanState *node) {
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

    /* ----
     * FdwScanState *scan_state =
     *	 (FdwScanState *) node->fdw_state;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);


    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);

    FdwScanState *scan_state = (FdwScanState *) node->fdw_state;

    bool found = false;
    char *key;
    char *value;

    /* get the next record, if any, and fill in the slot */
    if (scan_state->key_based_qual) {
        if (!scan_state->key_based_qual_sent) {
            scan_state->key_based_qual_sent = true;
            found = Get(scan_state->db, scan_state->key_based_qual_value, &value);
        }
    } else {
        found = Next(scan_state->db, scan_state->iter, &key, &value);
    }

    if (found) {
        char **values = (char **) palloc(sizeof(char *) * 2);
        if (scan_state->key_based_qual) {
            values[0] = scan_state->key_based_qual_value;
        } else {
            values[0] = key;
        }
        values[1] = value;
        HeapTuple tuple = BuildTupleFromCStrings(scan_state->attinmeta, values);
        ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    }

    /* then return the slot */
    return slot;
}

static void ReScanForeignScan(ForeignScanState *node) {
    printf("\n-----------------ReScanForeignScan----------------------\n");
    /*
     * Restart the scan from the beginning. Note that any parameters the scan
     * depends on may have changed value, so the new scan does not necessarily
     * return exactly the same rows.
     */

    /* ----
     * FdwScanState *scan_state =
     *	 (FdwScanState *) node->fdw_state;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);
}

static void EndForeignScan(ForeignScanState *node) {
    printf("\n-----------------EndForeignScan----------------------\n");
    /*
     * End the scan and release resources. It is normally not important to
     * release palloc'd memory, but for example open files and connections to
     * remote servers should be cleaned up.
     */

    /* ----
     * FdwScanState *scan_state =
     *	 (FdwScanState *) node->fdw_state;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    FdwScanState *scan_state = (FdwScanState *) node->fdw_state;

    if (scan_state) {
        if (scan_state->iter) {
            DelIter(scan_state->iter);
            scan_state->iter = NULL;
        }

        if (scan_state->db) {
            scan_state->db = NULL;
        }
    }
}

static void AddForeignUpdateTargets(Query *parsetree,
                                    RangeTblEntry *target_rte,
                                    Relation target_relation) {
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

    Form_pg_attribute attr = &RelationGetDescr(target_relation)->attrs[0];

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

static List *PlanForeignModify(PlannerInfo *root,
                               ModifyTable *plan,
                               Index resultRelation,
                               int subplan_index) {
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

static int IsForeignRelUpdatable(Relation rel) {
    printf("\n-----------------IsForeignRelUpdatable----------------------\n");
    /*
     * Report which update operations the specified foreign table supports.
     * The return value should be a bit mask of rule event numbers indicating
     * which operations are supported by the foreign table, using the CmdType
     * enumeration; that is, (1 << CMD_UPDATE) = 4 for UPDATE, (1 <<
     * CMD_INSERT) = 8 for INSERT, and (1 << CMD_DELETE) = 16 for DELETE.
     *
     * If the IsForeignRelUpdatable pointer is set to NULL, foreign tables are
     * assumed to be insertable, updatable, or deletable if the FDW provides
     * ExecForeignInsert, ExecForeignUpdate, or ExecForeignDelete
     * respectively. This function is only needed if the FDW supports some
     * tables that are updatable and some that are not. (Even then, it's
     * permissible to throw an error in the execution routine instead of
     * checking in this function. However, this function is used to determine
     * updatability for display in the information_schema views.)
     */

    elog(DEBUG1, "entering function %s", __func__);

    return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

static void BeginForeignModify(ModifyTableState *mtstate,
                               ResultRelInfo *rinfo,
                               List *fdw_private,
                               int subplan_index,
                               int eflags) {
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

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

    FdwModifyState *modify_state = palloc0(sizeof(FdwModifyState));
    if (!db) db = Open();
    modify_state->db = db;
    modify_state->rel = rinfo->ri_RelationDesc;
    modify_state->key_info = palloc0(sizeof(FmgrInfo));
    modify_state->value_info = palloc0(sizeof(FmgrInfo));

    CmdType operation = mtstate->operation;
    if (operation == CMD_UPDATE || operation == CMD_DELETE) {
        /* Find the ctid resjunk column in the subplan's result */
        Plan *subplan = mtstate->mt_plans[subplan_index]->plan;
        modify_state->key_junk_no =
                ExecFindJunkAttributeInTlist(subplan->targetlist, "key_junk");
        if (!AttributeNumberIsValid(modify_state->key_junk_no)) {
            elog(ERROR, "could not find key junk column");
        }
    }

    Form_pg_attribute attr = &RelationGetDescr(modify_state->rel)->attrs[0];
    Assert(!attr->attisdropped);
    Oid typefnoid;
    bool isvarlena;
    getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
    fmgr_info(typefnoid, modify_state->key_info);

    attr = &RelationGetDescr(modify_state->rel)->attrs[1];
    Assert(!attr->attisdropped);
    getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
    fmgr_info(typefnoid, modify_state->value_info);

    rinfo->ri_FdwState = modify_state;
}

static TupleTableSlot *ExecForeignInsert(EState *estate,
                                         ResultRelInfo *rinfo,
                                         TupleTableSlot *slot,
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
     *
     */

    /* ----
     * FdwModifyState *modify_state =
     *	 (FdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    FdwModifyState *modify_state = (FdwModifyState *) rinfo->ri_FdwState;

    bool isnull;
    Datum value = slot_getattr(planSlot, 1, &isnull);
    if (isnull) elog(ERROR, "can't get key value");
    char *key_value = OutputFunctionCall(modify_state->key_info, value);

    value = slot_getattr(planSlot, 2, &isnull);
    if (isnull) elog(ERROR, "can't get value value");
    char *value_value = OutputFunctionCall(modify_state->value_info, value);

    if (!Put(modify_state->db, key_value, value_value)) {
        elog(ERROR, "Error from ExecForeignInsert");
    }
    return slot;
}

static TupleTableSlot *ExecForeignUpdate(EState *estate,
                                         ResultRelInfo *rinfo,
                                         TupleTableSlot *slot,
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

    /* ----
     * FdwModifyState *modify_state =
     *	 (FdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    FdwModifyState *modify_state = (FdwModifyState *) rinfo->ri_FdwState;

    bool isnull;
    Datum value = ExecGetJunkAttribute(planSlot,
                                       modify_state->key_junk_no,
                                       &isnull);
    if (isnull) elog(ERROR, "can't get junk key value");
    char *key_value = OutputFunctionCall(modify_state->key_info, value);

    value = slot_getattr(planSlot, 1, &isnull);
    if (isnull) elog(ERROR, "can't get new key value");
    char *key_value_new = OutputFunctionCall(modify_state->key_info, value);
    if (strcmp(key_value, key_value_new) != 0) {
        elog(ERROR,
             "You cannot update key values (original key value was %s)",
             key_value);
        return slot;
    }

    value = slot_getattr(planSlot, 2, &isnull);
    if (isnull) elog(ERROR, "can't get value value");
    char *value_value = OutputFunctionCall(modify_state->value_info, value);

    if(!Put(modify_state->db, key_value, value_value)) {
        elog(ERROR, "Error from ExecForeignUpdate");
    }

    return slot;
}

static TupleTableSlot *ExecForeignDelete(EState *estate,
                                         ResultRelInfo *rinfo,
                                         TupleTableSlot *slot,
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

    /* ----
     * FdwModifyState *modify_state =
     *	 (FdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    FdwModifyState *modify_state = (FdwModifyState *) rinfo->ri_FdwState;

    bool isnull;
    Datum value = ExecGetJunkAttribute(planSlot, modify_state->key_junk_no, &isnull);
    if (isnull) elog(ERROR, "can't get key value");

    char *key_value = OutputFunctionCall(modify_state->key_info, value);

    if (!Delete(modify_state->db, key_value)) {
        elog(ERROR, "Error from ExecForeignDelete");
    }

    return slot;
}

static void EndForeignModify(EState *estate, ResultRelInfo *rinfo) {
    printf("\n-----------------EndForeignModify----------------------\n");
    /*
     * End the table update and release resources. It is normally not
     * important to release palloc'd memory, but for example open files and
     * connections to remote servers should be cleaned up.
     *
     * If the EndForeignModify pointer is set to NULL, no action is taken
     * during executor shutdown.
     */

    /* ----
     * FdwModifyState *modify_state =
     *	 (FdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    FdwModifyState *modify_state = (FdwModifyState *) rinfo->ri_FdwState;

    if (modify_state && modify_state->db) modify_state->db = NULL;
}

static void ExplainForeignScan(ForeignScanState *node,
                               struct ExplainState * es) {
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

static void ExplainForeignModify(ModifyTableState *mtstate,
                                 ResultRelInfo *rinfo,
                                 List *fdw_private,
                                 int subplan_index,
                                 struct ExplainState * es) {
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

    /* ----
     * FdwModifyState *modify_state =
     *	 (FdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);
}

static bool AnalyzeForeignTable(Relation relation,
                                AcquireSampleRowsFunc *func,
                                BlockNumber *totalpages) {
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

static void GetForeignJoinPaths(PlannerInfo *root,
                                RelOptInfo *joinrel,
                                RelOptInfo *outerrel,
                                RelOptInfo *innerrel,
                                JoinType jointype,
                                JoinPathExtraData *extra) {
    printf("\n-----------------GetForeignJoinPaths----------------------\n");
    /*
     * Create possible access paths for a join of two (or more) foreign tables
     * that all belong to the same foreign server. This optional function is
     * called during query planning. As with GetForeignPaths, this function
     * should generate ForeignPath path(s) for the supplied joinrel, and call
     * add_path to add these paths to the set of paths considered for the
     * join. But unlike GetForeignPaths, it is not necessary that this
     * function succeed in creating at least one path, since paths involving
     * local joining are always possible.
     *
     * Note that this function will be invoked repeatedly for the same join
     * relation, with different combinations of inner and outer relations; it
     * is the responsibility of the FDW to minimize duplicated work.
     *
     * If a ForeignPath path is chosen for the join, it will represent the
     * entire join process; paths generated for the component tables and
     * subsidiary joins will not be used. Subsequent processing of the join
     * path proceeds much as it does for a path scanning a single foreign
     * table. One difference is that the scanrelid of the resulting
     * ForeignScan plan node should be set to zero, since there is no single
     * relation that it represents; instead, the fs_relids field of the
     * ForeignScan node represents the set of relations that were joined. (The
     * latter field is set up automatically by the core planner code, and need
     * not be filled by the FDW.) Another difference is that, because the
     * column list for a remote join cannot be found from the system catalogs,
     * the FDW must fill fdw_scan_tlist with an appropriate list of
     * TargetEntry nodes, representing the set of columns it will supply at
     * runtime in the tuples it returns.
     */

    elog(DEBUG1, "entering function %s", __func__);
}

static RowMarkType GetForeignRowMarkType(RangeTblEntry *rte,
                                         LockClauseStrength strength) {
    printf("\n-----------------GetForeignRowMarkType----------------------\n");
    /*
     * Report which row-marking option to use for a foreign table. rte is the
     * RangeTblEntry node for the table and strength describes the lock
     * strength requested by the relevant FOR UPDATE/SHARE clause, if any. The
     * result must be a member of the RowMarkType enum type.
     *
     * This function is called during query planning for each foreign table
     * that appears in an UPDATE, DELETE, or SELECT FOR UPDATE/SHARE query and
     * is not the target of UPDATE or DELETE.
     *
     * If the GetForeignRowMarkType pointer is set to NULL, the ROW_MARK_COPY
     * option is always used. (This implies that RefetchForeignRow will never
     * be called, so it need not be provided either.)
     */

    elog(DEBUG1, "entering function %s", __func__);

    return ROW_MARK_COPY;
}

static HeapTuple RefetchForeignRow(EState *estate,
                                   ExecRowMark *erm,
                                   Datum rowid,
                                   bool *updated) {
    printf("\n-----------------RefetchForeignRow----------------------\n");
    /*
     * Re-fetch one tuple from the foreign table, after locking it if
     * required. estate is global execution state for the query. erm is the
     * ExecRowMark struct describing the target foreign table and the row lock
     * type (if any) to acquire. rowid identifies the tuple to be fetched.
     * updated is an output parameter.
     *
     * This function should return a palloc'ed copy of the fetched tuple, or
     * NULL if the row lock couldn't be obtained. The row lock type to acquire
     * is defined by erm->markType, which is the value previously returned by
     * GetForeignRowMarkType. (ROW_MARK_REFERENCE means to just re-fetch the
     * tuple without acquiring any lock, and ROW_MARK_COPY will never be seen
     * by this routine.)
     *
     * In addition, *updated should be set to true if what was fetched was an
     * updated version of the tuple rather than the same version previously
     * obtained. (If the FDW cannot be sure about this, always returning true
     * is recommended.)
     *
     * Note that by default, failure to acquire a row lock should result in
     * raising an error; a NULL return is only appropriate if the SKIP LOCKED
     * option is specified by erm->waitPolicy.
     *
     * The rowid is the ctid value previously read for the row to be
     * re-fetched. Although the rowid value is passed as a Datum, it can
     * currently only be a tid. The function API is chosen in hopes that it
     * may be possible to allow other datatypes for row IDs in future.
     *
     * If the RefetchForeignRow pointer is set to NULL, attempts to re-fetch
     * rows will fail with an error message.
     */

    elog(DEBUG1, "entering function %s", __func__);

    return NULL;
}

static List *ImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid) {
    printf("\n-----------------ImportForeignSchema----------------------\n");
    /*
     * Obtain a list of foreign table creation commands. This function is
     * called when executing IMPORT FOREIGN SCHEMA, and is passed the parse
     * tree for that statement, as well as the OID of the foreign server to
     * use. It should return a list of C strings, each of which must contain a
     * CREATE FOREIGN TABLE command. These strings will be parsed and executed
     * by the core server.
     *
     * Within the ImportForeignSchemaStmt struct, remote_schema is the name of
     * the remote schema from which tables are to be imported. list_type
     * identifies how to filter table names: FDW_IMPORT_SCHEMA_ALL means that
     * all tables in the remote schema should be imported (in this case
     * table_list is empty), FDW_IMPORT_SCHEMA_LIMIT_TO means to include only
     * tables listed in table_list, and FDW_IMPORT_SCHEMA_EXCEPT means to
     * exclude the tables listed in table_list. options is a list of options
     * used for the import process. The meanings of the options are up to the
     * FDW. For example, an FDW could use an option to define whether the NOT
     * NULL attributes of columns should be imported. These options need not
     * have anything to do with those supported by the FDW as database object
     * options.
     *
     * The FDW may ignore the local_schema field of the
     * ImportForeignSchemaStmt, because the core server will automatically
     * insert that name into the parsed CREATE FOREIGN TABLE commands.
     *
     * The FDW does not have to concern itself with implementing the filtering
     * specified by list_type and table_list, either, as the core server will
     * automatically skip any returned commands for tables excluded according
     * to those options. However, it's often useful to avoid the work of
     * creating commands for excluded tables in the first place. The function
     * IsImportableForeignTable() may be useful to test whether a given
     * foreign-table name will pass the filter.
     */

    elog(DEBUG1, "entering function %s", __func__);

    return NULL;
}

Datum kv_fdw_handler(PG_FUNCTION_ARGS) {
    printf("\n-----------------fdw_handler----------------------\n");
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

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
    fdwroutine->GetForeignRelSize = GetForeignRelSize; /* S U D */
    fdwroutine->GetForeignPaths = GetForeignPaths; /* S U D */
    fdwroutine->GetForeignPlan = GetForeignPlan; /* S U D */
    fdwroutine->BeginForeignScan = BeginForeignScan; /* S U D */
    fdwroutine->IterateForeignScan = IterateForeignScan; /* S */
    fdwroutine->ReScanForeignScan = ReScanForeignScan; /* S */
    fdwroutine->EndForeignScan = EndForeignScan; /* S U D */

    /* remainder are optional - use NULL if not required */
    /* support for insert / update / delete */
    fdwroutine->AddForeignUpdateTargets = AddForeignUpdateTargets; /* U D */
    fdwroutine->PlanForeignModify = PlanForeignModify; /* I U D */
    fdwroutine->IsForeignRelUpdatable = IsForeignRelUpdatable;
    fdwroutine->BeginForeignModify = BeginForeignModify; /* I U D */
    fdwroutine->ExecForeignInsert = ExecForeignInsert; /* I */
    fdwroutine->ExecForeignUpdate = ExecForeignUpdate; /* U */
    fdwroutine->ExecForeignDelete = ExecForeignDelete; /* D */
    fdwroutine->EndForeignModify = EndForeignModify; /* I U D */

    /* support for EXPLAIN */
    fdwroutine->ExplainForeignScan = ExplainForeignScan; /* EXPLAIN S U D */
    fdwroutine->ExplainForeignModify = ExplainForeignModify; /* EXPLAIN I U D */

    /* support for ANALYSE */
    fdwroutine->AnalyzeForeignTable = AnalyzeForeignTable; /* ANALYZE only */

    /* Support for scanning foreign joins */
    fdwroutine->GetForeignJoinPaths = GetForeignJoinPaths;

    /* Support for locking foreign rows */
    fdwroutine->GetForeignRowMarkType = GetForeignRowMarkType;
    fdwroutine->RefetchForeignRow = RefetchForeignRow;

    /* Support functions for IMPORT FOREIGN SCHEMA */
    fdwroutine->ImportForeignSchema = ImportForeignSchema;

    PG_RETURN_POINTER(fdwroutine);
}

Datum kv_fdw_validator(PG_FUNCTION_ARGS) {
    List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));

    elog(DEBUG1, "entering function %s", __func__);

    /* make sure the options are valid */

    /* no options are supported */

    if (list_length(options_list) > 0)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                 errmsg("invalid options"),
                 errhint("FDW does not support any options")));

    PG_RETURN_VOID();
}
