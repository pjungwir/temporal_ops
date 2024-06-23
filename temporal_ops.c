#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <executor/functions.h>
#include <nodes/nodes.h>
#include <nodes/supportnodes.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/syscache.h>

/*
 * Borrow this from util/adt/ri_triggers.c
 * since we do similar SQL-building to there:
 */
#define MAX_QUOTED_NAME_LEN  (NAMEDATALEN*2+3)
#define MAX_QUOTED_REL_NAME_LEN  (MAX_QUOTED_NAME_LEN*2)

PG_MODULE_MAGIC;

Datum temporal_semijoin_support(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(temporal_semijoin_support);

/*
 * quoteOneName --- safely quote a single SQL name
 *
 * buffer must be MAX_QUOTED_NAME_LEN long (includes room for \0)
 *
 * (Copied from ri_triggers.c.)
 */
static void
quoteOneName(char *buffer, const char *name)
{
    /* Rather than trying to be smart, just always quote it. */
    *buffer++ = '"';
    while (*name)
    {
        if (*name == '"')
            *buffer++ = '"';
        *buffer++ = *name++;
    }
    *buffer++ = '"';
    *buffer = '\0';
}

/*
 * temporal_semijoin_sql - build SQL for semijoin query
 */
static void
temporal_semijoin_sql(
    char *left_table,
    char *left_id_col,
    char *left_valid_col,
    char *right_table,
    char *right_id_col,
    char *right_valid_col,
    char **result
) {
    StringInfoData q;
    char left_table_q[MAX_QUOTED_REL_NAME_LEN];
    char left_id_col_q[MAX_QUOTED_NAME_LEN];
    char left_valid_col_q[MAX_QUOTED_NAME_LEN];
    char right_table_q[MAX_QUOTED_REL_NAME_LEN];
    char right_id_col_q[MAX_QUOTED_NAME_LEN];
    char right_valid_col_q[MAX_QUOTED_NAME_LEN];
    char *subquery_alias;

    quoteOneName(left_table_q, left_table);
    quoteOneName(left_id_col_q, left_id_col);
    quoteOneName(left_valid_col_q, left_valid_col);
    quoteOneName(right_table_q, right_table);
    quoteOneName(right_id_col_q, right_id_col);
    quoteOneName(right_valid_col_q, right_valid_col);

    // TODO: When we let you select extra columns from the left_table,
    // we will need to check for conflicts against those too.
    if (strcmp("j", left_table) == 0 || strcmp("j", right_table) == 0)
    {
        if (strcmp("j1", left_table) == 0 || strcmp("j1", right_table) == 0)
            subquery_alias = "j2";
        else
            subquery_alias = "j1";
    }
    else
        subquery_alias = "j";

    /*
     * SELECT  a.id, UNNEST(multirange(a.valid_at) * j.valid_at) AS valid_at
     * FROM    a
     * JOIN (
     *   SELECT  b.id, range_agg(b.valid_at) AS valid_at
     *   FROM    b
     *   GROUP BY b.id
     * ) AS j
     * ON a.id = j.id AND a.valid_at && j.valid_at;
     */
    initStringInfo(&q);
    appendStringInfo(&q,
            "SELECT %1$s.%2$s, UNNEST(multirange(%1$s.%3$s) * %7$s.%3$s) AS %3$s\n"
            "FROM %1$s\n"
            "JOIN (\n"
            "  SELECT %4$s.%5$s, range_agg(%4$s.%6$s) AS %6$s\n"
            "  FROM %4$s\n"
            "  GROUP BY %4$s.%5$s\n"
            ") AS %7$s\n"
            "ON %1$s.%2$s = %7$s.%5$s AND %1$s.%3$s && %7$s.%3$s",
            left_table_q, left_id_col_q, left_valid_col_q,
            right_table_q, right_id_col_q, right_valid_col_q,
            subquery_alias);

    *result = q.data;
}

static bool getarg_cstring(FuncExpr *expr, int n, char **result)
{
    Node *node;
    Const *c;

    node = lfirst(list_nth_cell(expr->args, n));
    if (!IsA(node, Const))
    {
        ereport(WARNING, (errmsg("temporal_semijoin called with non-Const parameters")));
        return false;
    }

    c = (Const *) node;
    if (c->consttype != TEXTOID)
    {
        ereport(WARNING, (errmsg("temporal_semijoin called with non-TEXT parameters")));
        return false;
    }

    *result = TextDatumGetCString(c->constvalue);
    return true;
}

/*
 * Inline the function call.
 *
 * Postgres does this automatically for SRF SQL functions
 * (provided they qualify), but since temporal_semijoin
 * generates its SQL from its parameters, it must be PLPGSQL instead.
 * Still we can use inline_set_returning_function
 * from optimizer/util/clauses.c for guidance.
 * We want to return a Query node.
 *
 * XXX: Alas, Postgres doesn't know what to do with a Query node in this context.
 * It needs to be a RangeTblFunction.
 *
 * What we should do instead is add a new kind of SupportRequest that return a SQL string,
 * and then we inline that.
 * That way you can inline plpgsql functions that generate a SELECT from their (necessarily constant) arguments.
 * Call it SupportRequestInlineSRF
 */
Datum
temporal_semijoin_support(PG_FUNCTION_ARGS)
{
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    SupportRequestSimplify *req;
    FuncExpr *expr;
    HeapTuple func_tuple;
    SQLFunctionParseInfoPtr pinfo;
    char *left_table;
    char *left_id_col;
    char *left_valid_col;
    char *right_table;
    char *right_id_col;
    char *right_valid_col;
    char *sql;
    List *raw_parsetree_list;
    List *querytree_list;
    Query *querytree;

    /* We only handle Simplify support requests. */
    if (!IsA(rawreq, SupportRequestSimplify))
        PG_RETURN_POINTER(NULL);

    req = (SupportRequestSimplify *) rawreq;
    expr = req->fcall;

    if (list_length(expr->args) != 6)
    {
        ereport(WARNING, (errmsg("temporal_semijoin called with %d args but expected 6", list_length(expr->args)))); 
        PG_RETURN_POINTER(NULL);
    }

    /*
     * Extract strings from the func's arguments.
     * They must all be Const and TEXT.
     */
    if (!getarg_cstring(expr, 0, &left_table))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 1, &left_id_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 2, &left_valid_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 3, &right_table))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 4, &right_id_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 5, &right_valid_col))
        PG_RETURN_POINTER(NULL);

    /*
     * Everything looks good. Build a Node tree for the query.
     * For now it's easiest to let Postgres do it for us,
     * as if it were inlining a SQL function
     * (see inline_set_returning_function in optimizer/util/clauses.c).
     */
    temporal_semijoin_sql(
            left_table,
            left_id_col,
            left_valid_col,
            right_table,
            right_id_col,
            right_valid_col,
            &sql);

    func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(expr->funcid));
    if (!HeapTupleIsValid(func_tuple))
        elog(ERROR, "cache lookup failed for function %u", expr->funcid);

    /*
     * Set up to handle parameters while parsing the function body.
     * Actually there are no parameters used within the generated SQL.
     * But pass the temporal_semijoin function anyway.
     */
    pinfo = prepare_sql_fn_parse_info(func_tuple,
                                      (Node *) expr,
                                      expr->inputcollid);
    /*
     * prepare_sql_fn_parse_info copies everything it needs out of the struct,
     * so we don't need to keep holding on to this:
     */
    ReleaseSysCache(func_tuple);

    /*
     * Parse, analyze, and rewrite (unlike inline_function(), we can't
     * skip rewriting here).  We can fail as soon as we find more than one
     * query, though.
     */
    raw_parsetree_list = pg_parse_query(sql);
    if (list_length(raw_parsetree_list) != 1)
    {
        ereport(WARNING, (errmsg("temporal_semijoin parsed to more than one node")));
        PG_RETURN_POINTER(NULL);
    }

    querytree_list = pg_analyze_and_rewrite_withcb(
            linitial(raw_parsetree_list),
            sql,
            (ParserSetupHook) sql_fn_parser_setup,
            pinfo, NULL);
    if (list_length(querytree_list) != 1)
    {
        ereport(WARNING, (errmsg("temporal_semijoin parsed to more than one node")));
        PG_RETURN_POINTER(NULL);
    }
    querytree = linitial(querytree_list);

    if (!IsA(querytree, Query))
    {
        ereport(WARNING,
                 (errmsg("temporal_semijoin didn't parse to a Query"),
                 errdetail("Got this instead: %s", nodeToString(querytree))));
        PG_RETURN_POINTER(NULL);
    }

    /* We got a Query, so return it for inlining. */

    PG_RETURN_POINTER(querytree);
}
