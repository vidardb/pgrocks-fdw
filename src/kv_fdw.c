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

#include "kv_fdw.h"
#include "kv_api.h"

#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"

#include "access/tuptoaster.h"
#include "access/heaptoast.h"
#include "catalog/pg_operator.h"
#include "utils/syscache.h"
#include "access/table.h"
#ifdef VIDARDB
#include "parser/parsetree.h"
#include "optimizer/optimizer.h"
#endif


PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(kv_fdw_handler);
PG_FUNCTION_INFO_V1(kv_fdw_validator);


#ifdef VIDARDB
typedef struct TablePlanState {
    KVFdwOptions* fdwOptions;
    int attrCount;        /* total attributes in a table */
    List* targetAttrs;    /* attributes in select, where, groupby */
    bool toUpdateDelete;  /* any update or delete? indicate when to delete */
} TablePlanState;
#endif

/*
 * The scan state is for maintaining state for a scan, either for a
 * SELECT or UPDATE or DELETE.
 *
 * It is set up in BeginForeignScan and stashed in node->fdw_state and
 * subsequently used in IterateForeignScan, EndForeignScan and ReScanForeignScan.
 */
typedef struct TableReadState {
    bool isKeyBased;
    uint64 operationId;
    bool done;
    StringInfo key;
    char* buf;     /* shared mem for data returned by RangeQuery or ReadBatch */
    size_t bufLen; /* shared mem length, no next batch if it is 0 */
    char* next;    /* pointer to the next data entry for IterateForeignScan */
    bool hasNext;  /* whether a next batch from RangeQuery or ReadBatch*/

    #ifdef VIDARDB
    bool useColumn;
    List* targetAttrs;    /* attributes in select, where, group */
    #endif

    bool execExplainOnly;
} TableReadState;

/*
 * The modify state is for maintaining state of modify operations.
 *
 * It is set up in BeginForeignModify and stashed in
 * rinfo->ri_FdwState and subsequently used in ExecForeignInsert,
 * ExecForeignUpdate, ExecForeignDelete and EndForeignModify.
 */
typedef struct TableWriteState {
    CmdType operation;
    #ifdef VIDARDB
    bool    useColumn;
    List*   targetAttrs;    /* attributes in select, where, group */
    #endif
} TableWriteState;

/*
 * in backend process scope
 */

static uint64 operationId = 0;  /* a SQL might cause multiple scans */


static void GetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel,
                              Oid foreignTableId) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * Obtain relation size estimates for a foreign table. This is called at
     * the beginning of planning for a query that scans a foreign table. root
     * is the planner's global information about the query; baserel is the
     * planner's information about this table; and foreignTableId is the
     * pg_class OID of the foreign table. (foreignTableId could be obtained
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    /*
     * min & max will call GetForeignRelSize & GetForeignPaths multiple times,
     * we should open & close db multiple times.
     */
    OpenArgs args;

    Relation relation = table_open(foreignTableId, AccessShareLock);
    SetRelationComparatorOpts(relation, &args.opts);
    table_close(relation, AccessShareLock);

    KVFdwOptions* fdwOptions = KVGetOptions(foreignTableId);
    args.path = fdwOptions->filename;

    #ifdef VIDARDB
    TablePlanState* planState = palloc0(sizeof(TablePlanState));
    planState->fdwOptions = fdwOptions;
    planState->attrCount = baserel->max_attr;
    planState->toUpdateDelete = false;

    /*
     * Identify which attributes will need to be retrieved.
     * These include all attrs needed for joins or final output, plus
     * all attrs used in the conds.
     */
    Bitmapset* attrs = NULL;
    pull_varattnos((Node*) baserel->reltarget->exprs, baserel->relid, &attrs);

    /* identify baserestrictinfo, such as where, groupby */
    List* conds = NIL;
    ListCell* lc = NULL;
    foreach (lc, baserel->baserestrictinfo) {
        RestrictInfo* restrictInfo = lfirst_node(RestrictInfo, lc);
        conds = lappend(conds, restrictInfo);
    }
    foreach (lc, conds) {
        RestrictInfo* restrictInfo = lfirst_node(RestrictInfo, lc);
        pull_varattnos((Node*) restrictInfo->clause, baserel->relid, &attrs);
    }

    printf("\n");
    planState->targetAttrs = NIL;
    int col = -1;
    while ((col = bms_next_member(attrs, col)) >= 0) {
        /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
        AttrNumber attr = col + FirstLowInvalidHeapAttributeNumber;
        if (attr <= InvalidAttrNumber) {  /* shouldn't happen */
            ereport(ERROR, errmsg("InvalidAttrNumber in %s", __func__));
        }
        planState->targetAttrs = lappend_int(planState->targetAttrs, attr);
        printf(" %d ", attr);
    }
    printf("\n");

    /* count(*), no attributes show up, so we have to manually add key column */
    if (planState->targetAttrs == NIL) {
        planState->targetAttrs = lappend_int(planState->targetAttrs, 1);
    }

    baserel->fdw_private = planState;

    args.useColumn = planState->fdwOptions->useColumn;
    args.attrCount = planState->attrCount;
    #endif

    KVOpenRequest(foreignTableId, &args);
    /* TODO: better estimation */
    baserel->rows = KVCountRequest(foreignTableId);
    KVCloseRequest(foreignTableId);
}

static void GetForeignPaths(PlannerInfo* root, RelOptInfo* baserel,
                            Oid foreignTableId) {
    printf("\n-----------------%s----------------------\n", __func__);
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    Cost startupCost = 0;
    Cost totalCost = startupCost + baserel->rows;

    /* Create a ForeignPath node and add it as only possible path */
    add_path(baserel,
             (Path *) create_foreignscan_path(root, baserel,
                                              NULL,  /* default pathtarget */
                                              baserel->rows, startupCost,
                                              totalCost,
                                              NIL,   /* no pathkeys */
                                              NULL,  /* no outer rel either */
                                              NULL,  /* no extra plan */
                                              NIL)); /* no fdw_private data */
}

static ForeignScan* GetForeignPlan(PlannerInfo* root, RelOptInfo* baserel,
                                   Oid foreignTableId, ForeignPath* bestPath,
                                   List* targetList, List* scanClauses,
                                   Plan* outerPlan) {
    printf("\n-----------------%s----------------------\n", __func__);
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check. So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */

    scanClauses = extract_actual_clauses(scanClauses, false);

    OpenArgs args;

    Relation relation = table_open(foreignTableId, AccessShareLock);
    SetRelationComparatorOpts(relation, &args.opts);
    table_close(relation, AccessShareLock);

    KVFdwOptions* fdwOptions = KVGetOptions(foreignTableId);
    args.path = fdwOptions->filename;

    /* To accommodate min & max, we open file here */
    #ifdef VIDARDB
    TablePlanState* planState = baserel->fdw_private;
    args.useColumn = planState->fdwOptions->useColumn;
    args.attrCount = planState->attrCount;
    #endif

    KVOpenRequest(foreignTableId, &args);

    /* Create the ForeignScan node */
    return make_foreignscan(targetList, scanClauses, baserel->relid,
                            NIL, /* no expressions to evaluate */
                            #ifdef VIDARDB
                            list_make1(planState),
                            #else
                            NIL,
                            #endif
                            NIL, /* no custom tlist */
                            NIL, /* no remote quals */ NULL);
}

static void GetKeyBasedQual(Node* node, ForeignScanState* scanState,
                            TableReadState* readState) {
    if (!node || !IsA(node, OpExpr)) {
        return;
    }

    OpExpr* op = (OpExpr*) node;
    if (list_length(op->args) != 2) {
        return;
    }

    Node* left = list_nth(op->args, 0);
    if (!IsA(left, Var)) {
        return;
    }

    Node* right = list_nth(op->args, 1);
    if (!IsA(right, Const) && !IsA(right, Param)) {
        return;
    }

    Index varattno = ((Var*) left)->varattno;
    if (varattno != 1) {
        return;
    }

    /* get the name of the operator according to PG_OPERATOR OID */
    HeapTuple opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
    if (!HeapTupleIsValid(opertup)) {
        ereport(ERROR, errmsg("cache lookup failed for operator %u", op->opno));
    }
    Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);
    char* oprname = NameStr(operform->oprname);
    /* TODO: support more operators */
    if (strncmp(oprname, "=", NAMEDATALEN)) {
        ReleaseSysCache(opertup);
        return;
    }
    ReleaseSysCache(opertup);

    Datum datum;

    if (IsA(right, Const)) {
        Const* constNode = (Const*) right;
        datum = constNode->constvalue;
    } else {
        Param* paramNode = (Param*) right;
        ParamListInfo paramListInfo = scanState->ss.ps.state->es_param_list_info;
        datum = paramListInfo->params[paramNode->paramid-1].value;
    }

    /*
     * We can push down this qual if:
     * - The operatory is =
     * - The qual is on the key column
     */
    readState->isKeyBased = true;
    readState->key = makeStringInfo();
    Relation relation = scanState->ss.ss_currentRelation;
    TupleDesc tupleDescriptor = relation->rd_att;

    SerializeAttribute(tupleDescriptor, varattno-1, datum, readState->key);

    return;
}

static void BeginForeignScan(ForeignScanState* scanState, int executorFlags) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * Begin executing a foreign scan. This is called during executor startup.
     * It should perform any initialization needed before the scan can start,
     * but not start executing the actual scan (that should be done upon the
     * first call to IterateForeignScan). The ForeignScanState node has
     * already been created, but its fdw_state field is still NULL.
     * Information about the table to scan is accessible through the
     * ForeignScanState node (in particular, from the underlying ForeignScan
     * plan node, which contains any FDW-private information provided by
     * GetForeignPlan). executorFlags contains flag bits describing the
     * executor's operating mode for this plan node.
     *
     * Note that when (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) is true, this
     * function should not perform any externally-visible actions; it should
     * only do the minimum required to make the node state valid for
     * ExplainForeignScan and EndForeignScan.
     *
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    TableReadState* readState = palloc0(sizeof(TableReadState));
    readState->execExplainOnly = false;
    readState->isKeyBased = false;
    readState->operationId = 0;
    readState->done = false;
    readState->key = NULL;
    readState->buf = NULL;
    readState->bufLen = 0;

    #ifdef VIDARDB
    ForeignScan* foreignScan = (ForeignScan*) scanState->ss.ps.plan;
    List* fdwPrivateList = (List*) foreignScan->fdw_private;
    TablePlanState* planState = (TablePlanState*) linitial(fdwPrivateList);
    readState->useColumn = planState->fdwOptions->useColumn;
    readState->targetAttrs = planState->targetAttrs;

    size_t batchCapacity = planState->fdwOptions->batchCapacity;

    if (planState->toUpdateDelete == false) {
        pfree(planState);
    }
    #endif

    scanState->fdw_state = (void*) readState;

    /* must after readState is recorded, otherwise explain won't close db */
    if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) {
        readState->execExplainOnly = true;
        return;
    }

    ListCell* lc;
    foreach (lc, scanState->ss.ps.plan->qual) {
        Expr* state = lfirst(lc);
        GetKeyBasedQual((Node*) state, scanState, readState);
        if (readState->isKeyBased) {
            printf("\nkey_based_qual\n");
            break;
        }
    }

    Oid relationId = RelationGetRelid(scanState->ss.ss_currentRelation);

    if (!readState->isKeyBased) {
        #ifdef VIDARDB
        if (readState->useColumn) {

            RangeQueryOpts options;
            options.batchCapacity = batchCapacity;
            options.startLen = 0;
            options.limitLen = 0;
            options.attrCount = list_length(readState->targetAttrs);
            options.attrs = palloc0(options.attrCount * sizeof(*options.attrs));

            printf("\n");
            int i = 0;
            ListCell* targetCell = NULL;
            foreach (targetCell, readState->targetAttrs) {
                AttrNumber attr = lfirst_int(targetCell);
                *(options.attrs + i) = attr - 1;
                printf(" %d ", *(options.attrs + i));
                ++i;
            }
            printf("\n");

            RangeQueryArgs args;
            args.opid = ++operationId;
            args.opts = &options;
            args.buf = &readState->buf;
            args.bufLen = &readState->bufLen;
            readState->hasNext = KVRangeQueryRequest(relationId, &args);
            pfree(options.attrs);
        } else {
            ReadBatchArgs args;
            args.buf = &readState->buf;
            args.bufLen = &readState->bufLen;
            args.opid = ++operationId;
            readState->hasNext = KVReadBatchRequest(relationId, &args);
        }
        #else
        ReadBatchArgs args;
        args.buf = &readState->buf;
        args.bufLen = &readState->bufLen;
        args.opid = ++operationId;
        readState->hasNext = KVReadBatchRequest(relationId, &args);
        #endif

        readState->next = readState->buf;
        readState->operationId = operationId;
    }
}

#ifdef VIDARDB
/*
 * It also serves key-based search, but key-based search provides full tuple,
 * where we need to match the tuple when do deserialization.
 * Update and Delete also provide full tuple.
 */
static void DeserializeColumnTuple(char* key, size_t kLen, char* val,
                                   size_t vLen, TupleTableSlot* tupleSlot,
                                   List* targetList, bool fullTuple) {
    Datum* values = tupleSlot->tts_values;
    bool* nulls = tupleSlot->tts_isnull;

    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    int count = tupleDescriptor->natts;

    /* initialize all values for this row to null */
    memset(values, 0, count * sizeof(Datum));
    /* nulls tells the above layer whether corresponding attribute is carried */
    memset(nulls, true, count * sizeof(bool));

    int targetListLen = list_length(targetList);
    int targetAttrsLen = fullTuple ? count : targetListLen;
    AttrNumber* attrs = (AttrNumber*) palloc0(targetAttrsLen * sizeof(*attrs));

    if (fullTuple) {
        for (int index = 0; index < targetAttrsLen; ++index) {
            *(attrs + index) = index + 1;
        }
    } else {
        int i = 0;
        ListCell* targetCell = NULL;
        foreach (targetCell, targetList) {
            AttrNumber attr = lfirst_int(targetCell);
            *(attrs + i) = attr;
            ++i;
        }
    }

//    printf("\n");
//    for (int index = 0; index < targetAttrsLen; ++index) {
//        printf(" %d ", *(attrs + index));
//    }
//    printf("\n");

    for (int index = 0, offset = 0; index < targetAttrsLen; index++) {
        AttrNumber attr = *(attrs + index);
        attr--;
        offset = DeserializeAttribute(tupleDescriptor, attr, offset, key, val,
                                      val + vLen, values, nulls);
    }

    pfree(attrs);
}
#endif

static void DeserializeTuple(char* key, size_t kLen, char* val, size_t vLen,
                             TupleTableSlot* tupleSlot) {
    Datum* values = tupleSlot->tts_values;
    bool* nulls = tupleSlot->tts_isnull;

    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    int count = tupleDescriptor->natts;

    /* initialize all values for this row to null */
    memset(values, 0, count * sizeof(Datum));
    memset(nulls, false, count * sizeof(bool));

    for (int index = 0, offset = 0; index < count; index++) {
        offset = DeserializeAttribute(tupleDescriptor, index, offset, key, val,
                                      val + vLen, values, nulls);
    }
}

static bool GetNextFromBatch(Oid relationId, TableReadState* readState,
                             char** key, size_t* keyLen, char** val,
                             size_t* valLen) {
    bool found = false;
    if (readState->next < readState->buf + readState->bufLen) {
        found = true;
    } else if (readState->hasNext) {
        #ifdef VIDARDB
        if (readState->useColumn) {
            RangeQueryArgs args;
            args.opid = readState->operationId;
            args.opts = NULL;
            args.buf = &readState->buf;
            args.bufLen = &readState->bufLen;
            readState->hasNext = KVRangeQueryRequest(relationId, &args);
        } else {
            ReadBatchArgs args;
            args.buf = &readState->buf;
            args.bufLen = &readState->bufLen;
            args.opid = readState->operationId;
            readState->hasNext = KVReadBatchRequest(relationId, &args);
        }
        #else
        ReadBatchArgs args;
        args.buf = &readState->buf;
        args.bufLen = &readState->bufLen;
        args.opid = readState->operationId;
        readState->hasNext = KVReadBatchRequest(relationId, &args);
        #endif

        readState->next  = readState->buf;
        if (readState->bufLen > 0) {
            found = true;
        }
    }

    if (found) {
        memcpy(keyLen, readState->next, sizeof(*keyLen));
        readState->next += sizeof(*keyLen);
        *key = readState->next;
        readState->next += *keyLen;

        memcpy(valLen, readState->next, sizeof(*valLen));
        readState->next += sizeof(*valLen);
        *val = readState->next;
        readState->next += *valLen;
    }

    return found;
}

static TupleTableSlot* IterateForeignScan(ForeignScanState* scanState) {
//    printf("\n-----------------%s----------------------\n", __func__);
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    TupleTableSlot* tupleSlot = scanState->ss.ss_ScanTupleSlot;
    ExecClearTuple(tupleSlot);

    TableReadState* readState = (TableReadState*) scanState->fdw_state;
    char* k = NULL, *v = NULL;
    size_t kLen = 0, vLen = 0;
    Oid relationId = RelationGetRelid(scanState->ss.ss_currentRelation);
    bool found = false;
    if (readState->isKeyBased) {
        if (!readState->done) {
            k = readState->key->data;
            kLen = readState->key->len;

            GetArgs args;
            args.key = k;
            args.keyLen = kLen;
            args.val = &v;
            args.valLen = &vLen;
            found = KVGetRequest(relationId, &args);

            readState->done = true;
        }
    } else {
        found = GetNextFromBatch(relationId, readState, &k, &kLen, &v, &vLen);
    }

    if (found) {
        #ifdef VIDARDB
        if (readState->useColumn) {
            DeserializeColumnTuple(k, kLen, v, vLen, tupleSlot,
                                   readState->targetAttrs,
                                   readState->isKeyBased);
        } else {
            DeserializeTuple(k, kLen, v, vLen, tupleSlot);
        }
        #else
        DeserializeTuple(k, kLen, v, vLen, tupleSlot);
        #endif

        ExecStoreVirtualTuple(tupleSlot);
    }

    return tupleSlot;
}

static void ReScanForeignScan(ForeignScanState* scanState) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * Restart the scan from the beginning. Note that any parameters the scan
     * depends on may have changed value, so the new scan does not necessarily
     * return exactly the same rows.
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));
}

static void EndForeignScan(ForeignScanState* scanState) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * End the scan and release resources. It is normally not important to
     * release palloc'd memory, but for example open files and connections to
     * remote servers should be cleaned up.
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    TableReadState* readState = (TableReadState*) scanState->fdw_state;
    Assert(readState);

    if (readState->execExplainOnly) {
        pfree(readState);
        return;
    }

    Oid relationId = RelationGetRelid(scanState->ss.ss_currentRelation);
    if (!readState->isKeyBased) {
        #ifdef VIDARDB
        if (readState->useColumn) {
            /*
             * unmap for this backend process should already be done in
             * IterateForeignScan. Now release resource of worker process.
             */
            RangeQueryArgs args;
            args.opid = readState->operationId;
            args.buf = &readState->buf;
            args.bufLen = &readState->bufLen;
            KVClearRangeQueryRequest(relationId, &args);
        } else {
            CloseCursorArgs args;
            args.opid = readState->operationId;
            args.buf = readState->buf;
            KVCloseCursorRequest(relationId, &args);
        }
        #else
        CloseCursorArgs args;
        args.opid = readState->operationId;
        args.buf = readState->buf;
        KVCloseCursorRequest(relationId, &args);
        #endif
    }

    KVCloseRequest(relationId);
    pfree(readState);
}

static void AddForeignUpdateTargets(Query* parsetree, RangeTblEntry* tableEntry,
                                    Relation targetRelation) {
    printf("\n-----------------%s----------------------\n", __func__);
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    /*
     * We are using first column as row identification column, so we are adding
     * that into target list.
     */
    Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(targetRelation), 0);

    Var* var = makeVar(parsetree->resultRelation, 1, attr->atttypid,
                       attr->atttypmod, InvalidOid, 0);

    /* Wrap it in a TLE with the right name ... */
    const char* attrname = NameStr(attr->attname);
    TargetEntry* entry = makeTargetEntry((Expr*) var,
                                         list_length(parsetree->targetList) + 1,
                                         pstrdup(attrname), true);

    /* ... and add it to the query's targetlist */
    parsetree->targetList = lappend(parsetree->targetList, entry);
}

static List* PlanForeignModify(PlannerInfo* root, ModifyTable* plan,
                               Index resultRelation, int subplanIndex) {
    printf("\n-----------------%s----------------------\n", __func__);
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
     * rangetable index. subplanIndex identifies which target of the
     * ModifyTable plan node this is, counting from zero; use this if you want
     * to index into plan->plans or other substructure of the plan node.
     *
     * If the PlanForeignModify pointer is set to NULL, no additional
     * plan-time actions are taken, and the fdw_private list delivered to
     * BeginForeignModify will be NIL.
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    #ifdef VIDARDB
    if (plan->operation == CMD_INSERT) {
        /* for insert, no upward info, we have to fetch from scratch */
        TablePlanState* planState = palloc0(sizeof(TablePlanState));

        RangeTblEntry* rangeTable = planner_rt_fetch(resultRelation, root);
        Oid foreignTableId = rangeTable->relid;
        planState->fdwOptions = KVGetOptions(foreignTableId);

        /*
         * Core code already has some lock on each rel being planned, so we can
         * use NoLock here.
         */
        Relation relation = table_open(foreignTableId, NoLock);
        TupleDesc tupleDescriptor = RelationGetDescr(relation);
        planState->attrCount = tupleDescriptor->natts;
        table_close(relation, NoLock);

        planState->toUpdateDelete = false;
        planState->targetAttrs = NIL;
        return list_make1(planState);
    } else {
        /* for delete and update, fdw_private comes from GetForeignRelSize */
        RelOptInfo* baserel = root->simple_rel_array[resultRelation];
        TablePlanState* planState = baserel->fdw_private;
        planState->toUpdateDelete = true;

        /* We disallow updates to the first column */
        if (plan->operation == CMD_UPDATE) {
            RangeTblEntry* rangeTable = planner_rt_fetch(resultRelation, root);
            Bitmapset* attrs = bms_copy(rangeTable->updatedCols);
            AttrNumber col = -1;
            while ((col = bms_first_member(attrs)) >= 0) {
                col += FirstLowInvalidHeapAttributeNumber;
                if (col <= InvalidAttrNumber) {  /* shouldn't happen */
                    ereport(ERROR, errmsg("InvalidAttrNumber in %s", __func__));
                }
                if (col == 1) {
                    ereport(ERROR, errmsg("row identifier column update is "
                                          "not supported."));
                }
                if (col > 1) {
                    break;
                }
            }
        }

        return list_make1(baserel->fdw_private);
    }
    #else
    return NULL;
    #endif
}

static void BeginForeignModify(ModifyTableState* modifyTableState,
                               ResultRelInfo* resultRelInfo, List* fdwPrivate,
                               int subplanIndex, int executorFlags) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * Begin executing a foreign table modification operation. This routine is
     * called during executor startup. It should perform any initialization
     * needed prior to the actual table modifications. Subsequently,
     * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
     * called for each tuple to be inserted, updated, or deleted.
     *
     * modifyTableState is the overall state of the ModifyTable plan node being
     * executed; global data about the plan and execution state is available
     * via this structure. resultRelInfo is the ResultRelInfo struct describing
     * the target foreign table. (The ri_FdwState field of ResultRelInfo is
     * available for the FDW to store any private state it needs for this
     * operation.) fdw_private contains the private data generated by
     * PlanForeignModify, if any. subplanIndex identifies which target of the
     * ModifyTable plan node this is. executorFlags contains flag bits
     * describing the executor's operating mode for this plan node.
     *
     * Note that when (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) is true, this
     * function should not perform any externally-visible actions; it should
     * only do the minimum required to make the node state valid for
     * ExplainForeignModify and EndForeignModify.
     *
     * If the BeginForeignModify pointer is set to NULL, no action is taken
     * during executor startup.
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    TableWriteState* writeState = palloc0(sizeof(TableWriteState));

    CmdType operation = modifyTableState->operation;
    writeState->operation = operation;

    Relation relation = resultRelInfo->ri_RelationDesc;
    Oid foreignTableId = RelationGetRelid(relation);
    table_open(foreignTableId, RowExclusiveLock);

    #ifdef VIDARDB
    TablePlanState* planState = (TablePlanState*) linitial(fdwPrivate);
    writeState->useColumn = planState->fdwOptions->useColumn;
    writeState->targetAttrs = planState->targetAttrs;
    #endif

    if (operation == CMD_INSERT) {
        OpenArgs args;
        SetRelationComparatorOpts(relation, &args.opts);
        KVFdwOptions* fdwOptions = KVGetOptions(foreignTableId);
        args.path = fdwOptions->filename;
        #ifdef VIDARDB
        args.useColumn = planState->fdwOptions->useColumn;
        args.attrCount = planState->attrCount;
        #endif
        KVOpenRequest(foreignTableId, &args);
    }

    resultRelInfo->ri_FdwState = (void*) writeState;

    #ifdef VIDARDB
    /* we are sure it is no longer need, once it appears here */
    pfree(planState);
    #endif
}

static void SerializeTuple(StringInfo key, StringInfo val,
                           TupleTableSlot* tupleSlot) {
    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    int count = tupleDescriptor->natts;

    for (int index = 0; index < count; index++) {

        Datum datum = tupleSlot->tts_values[index];
        if (tupleSlot->tts_isnull[index]) {
            if (index == 0) {
                ereport(ERROR, errmsg("first column cannot be null!"));
            }

            SerializeNullAttribute(tupleDescriptor, index, val);
        } else {
            SerializeAttribute(tupleDescriptor, index, datum,
                               index == 0 ? key : val);
        }
    }
}

static TupleTableSlot* ExecForeignInsert(EState* executorState,
                                         ResultRelInfo* resultRelInfo,
                                         TupleTableSlot* slot,
                                         TupleTableSlot* planSlot) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * Insert one tuple into the foreign table. executorState is global
     * execution state for the query. resultRelInfo is the ResultRelInfo struct
     * describing the target foreign table. slot contains the tuple to be
     * inserted; it will match the rowtype definition of the foreign table.
     * planSlot contains the tuple that was generated by the ModifyTable plan
     * node's subplan; it differs from slot in possibly containing additional
     * "junk" columns. (The planSlot is typically of little interest for INSERT
     * cases, but is provided for completeness.)
     *
     * The return value is either a slot containing the data that was actually
     * inserted (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually inserted
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the INSERT query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away returning
     * some or all columns depending on the contents of the RETURNING clause.
     * However, some slot must be returned to indicate success, or the query's
     * reported rowcount will be wrong.
     *
     * If the ExecForeignInsert pointer is set to NULL, attempts to insert into
     * the foreign table will fail with an error message.
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    TupleDesc tupleDescriptor = slot->tts_tupleDescriptor;

    bool shouldFree;
    HeapTuple heapTuple = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
    if (HeapTupleHasExternal(heapTuple)) {
        /* detoast any toasted attributes */
        HeapTuple newTuple = toast_flatten_tuple(heapTuple, tupleDescriptor);
        ExecForceStoreHeapTuple(newTuple, slot, shouldFree);
        shouldFree = false;
    }

    slot_getallattrs(slot);

    StringInfo key = makeStringInfo();
    StringInfo val = makeStringInfo();

    Relation relation = resultRelInfo->ri_RelationDesc;
    Oid foreignTableId = RelationGetRelid(relation);

    SerializeTuple(key, val, slot);

    PutArgs args;
    args.keyLen = key->len;
    args.valLen = val->len;
    args.key = key->data;
    args.val = val->data;
    KVPutRequest(foreignTableId, &args);

    if (shouldFree) {
        pfree(heapTuple);
    }

    return slot;
}

static TupleTableSlot* ExecForeignUpdate(EState* executorState,
                                         ResultRelInfo* resultRelInfo,
                                         TupleTableSlot* slot,
                                         TupleTableSlot* planSlot) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * Update one tuple in the foreign table. executorState is global execution
     * state for the query. resultRelInfo is the ResultRelInfo struct describing
     * the target foreign table. slot contains the new data for the tuple; it
     * will match the rowtype definition of the foreign table. planSlot contains
     * the tuple that was generated by the ModifyTable plan node's subplan; it
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
     * RETURNING clause. Hence, the FDW could choose to optimize away returning
     * some or all columns depending on the contents of the RETURNING clause.
     * However, some slot must be returned to indicate success, or the query's
     * reported rowcount will be wrong.
     *
     * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
     * foreign table will fail with an error message.
     *
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    TupleDesc tupleDescriptor = slot->tts_tupleDescriptor;
    bool shouldFree;
    HeapTuple heapTuple = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
    if (HeapTupleHasExternal(heapTuple)) {
        /* detoast any toasted attributes */
        HeapTuple newTuple = toast_flatten_tuple(heapTuple, tupleDescriptor);
        ExecForceStoreHeapTuple(newTuple, slot, shouldFree);
        shouldFree = false;
    }

    slot_getallattrs(slot);

    StringInfo key = makeStringInfo();
    StringInfo val = makeStringInfo();

    Relation relation = resultRelInfo->ri_RelationDesc;
    Oid foreignTableId = RelationGetRelid(relation);

    SerializeTuple(key, val, slot);

    PutArgs args;
    args.keyLen = key->len;
    args.valLen = val->len;
    args.key = key->data;
    args.val = val->data;
    KVPutRequest(foreignTableId, &args);

    if (shouldFree) {
        pfree(heapTuple);
    }

    return slot;
}

static TupleTableSlot* ExecForeignDelete(EState* executorState,
                                         ResultRelInfo* resultRelInfo,
                                         TupleTableSlot* slot,
                                         TupleTableSlot* planSlot) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * Delete one tuple from the foreign table. executorState is global
     * execution state for the query. resultRelInfo is the ResultRelInfo struct
     * describing the target foreign table. slot contains nothing useful upon
     * call, but can be used to hold the returned tuple. planSlot contains the
     * tuple that was generated by the ModifyTable plan node's subplan; in
     * particular, it will carry any junk columns that were requested by
     * AddForeignUpdateTargets. The junk column(s) must be used to identify
     * the tuple to be deleted.
     *
     * The return value is either a slot containing the row that was deleted,
     * or NULL if no row was deleted (typically as a result of triggers). The
     * passed-in slot can be used to hold the tuple to be returned.
     *
     * The data in the returned slot is used only if the DELETE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away returning
     * some or all columns depending on the contents of the RETURNING clause.
     * However, some slot must be returned to indicate success, or the query's
     * reported rowcount will be wrong.
     *
     * If the ExecForeignDelete pointer is set to NULL, attempts to delete
     * from the foreign table will fail with an error message.
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    slot_getallattrs(planSlot);

    StringInfo key = makeStringInfo();
    StringInfo val = makeStringInfo();

    Relation relation = resultRelInfo->ri_RelationDesc;
    Oid foreignTableId = RelationGetRelid(relation);

    SerializeTuple(key, val, planSlot);

    /* Get the previous value */
    char* v;
    uint64 vLen;

    GetArgs getArgs;
    getArgs.key = key->data;
    getArgs.keyLen = key->len;
    getArgs.val = &v;
    getArgs.valLen = &vLen;
    KVGetRequest(foreignTableId, &getArgs);

    /* Delete the specified key */
    DeleteArgs delArgs;
    delArgs.key = key->data;
    delArgs.keyLen = key->len;
    KVDeleteRequest(foreignTableId, &delArgs);

    /* Key not exists */
    if (vLen == 0) {
        return NULL;
    }

    /* Save the previous value into slot */
    #ifdef VIDARDB
    TableWriteState* writeState = (TableWriteState*) resultRelInfo->ri_FdwState;
    if (writeState->useColumn) {
        DeserializeColumnTuple(key->data, key->len, v, vLen, slot,
                               writeState->targetAttrs, true);
    } else {
        DeserializeTuple(key->data, key->len, v, vLen, slot);
    }
    #else
    DeserializeTuple(key->data, key->len, v, vLen, slot);
    #endif

    ExecStoreVirtualTuple(slot);
    return slot;
}

static void EndForeignModify(EState* executorState, ResultRelInfo* resultRelInfo) {
    printf("\n-----------------%s----------------------\n", __func__);
    /*
     * End the table update and release resources. It is normally not important
     * to release palloc'd memory, but for example open files and connections
     * to remote servers should be cleaned up.
     *
     * If the EndForeignModify pointer is set to NULL, no action is taken during
     * executor shutdown.
     */

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    TableWriteState* writeState = (TableWriteState*) resultRelInfo->ri_FdwState;

    if (writeState) {
        Relation relation = resultRelInfo->ri_RelationDesc;
        Oid foreignTableId = RelationGetRelid(relation);

        CmdType operation = writeState->operation;
        if (operation == CMD_INSERT) {
            KVCloseRequest(foreignTableId);
        }

        /* CMD_UPDATE and CMD_DELETE close will be taken care of by endScan */
        table_close(relation, RowExclusiveLock);

        pfree(writeState);
    }
}

static void ExplainForeignScan(ForeignScanState* scanState,
                               struct ExplainState*  explainState) {
    printf("\n-----------------%s----------------------\n", __func__);
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));
}

static void ExplainForeignModify(ModifyTableState* modifyTableState,
                                 ResultRelInfo* relationInfo, List* fdwPrivate,
                                int subplanIndex,
                                struct ExplainState* explainState) {
    printf("\n-----------------%s----------------------\n", __func__);
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));
}

static bool AnalyzeForeignTable(Relation relation,
                                AcquireSampleRowsFunc* acquireSampleRowsFunc,
                                BlockNumber* totalPageCount) {
    printf("\n-----------------%s----------------------\n", __func__);
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

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    return false;
}

Datum kv_fdw_handler(PG_FUNCTION_ARGS) {
    printf("\n-----------------%s----------------------\n", __func__);
    FdwRoutine* routine = makeNode(FdwRoutine);

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    /*
     * assign the handlers for the FDW
     *
     * This function might be called a number of times. In particular, it is
     * likely to be called for each INSERT statement. For an explanation, see
     * core postgres file src/optimizer/plan/createplan.c where it calls
     * GetFdwRoutineByRelId(().
     */

    /* these are required */
    routine->GetForeignRelSize = GetForeignRelSize;
    routine->GetForeignPaths = GetForeignPaths;
    routine->GetForeignPlan = GetForeignPlan;
    routine->BeginForeignScan = BeginForeignScan;
    routine->IterateForeignScan = IterateForeignScan;
    routine->ReScanForeignScan = ReScanForeignScan;
    routine->EndForeignScan = EndForeignScan;

    /* remainder are optional - use NULL if not required */
    /* support for insert / update / delete */
    routine->AddForeignUpdateTargets = AddForeignUpdateTargets;
    routine->PlanForeignModify = PlanForeignModify;
    routine->BeginForeignModify = BeginForeignModify;
    routine->ExecForeignInsert = ExecForeignInsert;
    routine->ExecForeignUpdate = ExecForeignUpdate;
    routine->ExecForeignDelete = ExecForeignDelete;
    routine->EndForeignModify = EndForeignModify;

    /* support for EXPLAIN */
    routine->ExplainForeignScan = ExplainForeignScan;
    routine->ExplainForeignModify = ExplainForeignModify;

    /* support for ANALYSE */
    routine->AnalyzeForeignTable = AnalyzeForeignTable;

    PG_RETURN_POINTER(routine);
}

Datum kv_fdw_validator(PG_FUNCTION_ARGS) {
    printf("\n-----------------%s----------------------\n", __func__);
    //List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));

    ereport(DEBUG1, errmsg("entering function %s", __func__));

    /* make sure the options are valid */

    /* no options are supported */

    /*if (list_length(options_list) > 0) {
        ereport(ERROR, errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                       errmsg("invalid options"),
                       errhint("FDW does not support any options"));
    }*/

    PG_RETURN_VOID();
}
