#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_class.h>
#include <catalog/pg_type.h>
#include <executor/functions.h>
#include <fmgr.h>
#include <nodes/nodes.h>
#include <nodes/supportnodes.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

/*
 * Borrow this from util/adt/ri_triggers.c
 * since we do similar SQL-building to there:
 */
#define MAX_QUOTED_NAME_LEN  (NAMEDATALEN*2+3)
#define MAX_QUOTED_REL_NAME_LEN  (MAX_QUOTED_NAME_LEN*2)

PG_MODULE_MAGIC;

Datum noop_support(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(noop_support);

Datum temporal_semijoin_support(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(temporal_semijoin_support);

Datum temporal_antijoin_support(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(temporal_antijoin_support);

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
 * Returns (unquoted) schema and table name
 * based on the nth parameter to the function in expr.
 *
 * It must be a Const node of Oid type.
 */
static bool getarg_table_name(FuncExpr *expr, int n, char *func_name, char **nspname, char **relname)
{
    Node *node;
    Const *c;
    HeapTuple tp;
    Form_pg_class reltup;

    node = lfirst(list_nth_cell(expr->args, n));
    if (!IsA(node, Const))
    {
        ereport(WARNING, (errmsg("%s called with non-Const parameters", func_name)));
        return false;
    }

    c = (Const *) node;
    if (c->consttype != REGCLASSOID)
    {
        ereport(WARNING, (errmsg("%s called with non-regclass parameters", func_name)));
        return false;
    }

    tp = SearchSysCache1(RELOID, c->constvalue);
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for relation %u", DatumGetObjectId(c->constvalue));
    reltup = (Form_pg_class) GETSTRUCT(tp);
    *relname = NameStr(reltup->relname);
    *nspname = get_namespace_name_or_temp(reltup->relnamespace);

    ReleaseSysCache(tp);

    return true;
}

/*
 * Returns an unquoted string in result,
 * based on the nth parameter to the function in expr.
 *
 * It must be a Const node of TEXT type.
 *
 * expr - the function call we're supporting
 * n - the nth arg (0-indexed)
 * func_name - the name of the user-facing func (for constructing error messages)
 */
static bool getarg_cstring(FuncExpr *expr, int n, char *func_name, char **result)
{
    Node *node;
    Const *c;

    node = lfirst(list_nth_cell(expr->args, n));
    if (!IsA(node, Const))
    {
        ereport(WARNING, (errmsg("%s called with non-Const parameters", func_name)));
        return false;
    }

    c = (Const *) node;
    if (c->consttype != TEXTOID)
    {
        ereport(WARNING, (errmsg("%s called with non-TEXT parameters", func_name)));
        return false;
    }

    *result = TextDatumGetCString(c->constvalue);
    return true;
}

/*
 * build_query - parse the given SQL and return a Query node.
 *
 * sql - the sql to parse
 * req - the support request object
 * func_name - the name of the user-facing func (for constructing error messages)
 */
static Query *build_query(char *sql, SupportRequestInlineInFrom *req, char *func_name) {
    FuncExpr *expr = (FuncExpr *) req->rtfunc->funcexpr;
    SQLFunctionParseInfoPtr pinfo;
    List *raw_parsetree_list;
    List *querytree_list;
    Query *querytree;

    /*
     * Set up to handle parameters while parsing the function body.
     * Actually there are no parameters used within the generated SQL.
     * But pass the temporal_semijoin function anyway.
     */
    pinfo = prepare_sql_fn_parse_info(req->proc,
                                      (Node *) expr,
                                      expr->inputcollid);
    /*
     * Parse, analyze, and rewrite (unlike inline_function(), we can't
     * skip rewriting here).  We can fail as soon as we find more than one
     * query, though.
     */
    raw_parsetree_list = pg_parse_query(sql);
    if (list_length(raw_parsetree_list) != 1)
    {
        ereport(WARNING, (errmsg("%s parsed to more than one node", func_name)));
        return NULL;
    }

    /* Analyze the parse tree as if it were a SQL-language body. */
    querytree_list = pg_analyze_and_rewrite_withcb(
            linitial(raw_parsetree_list),
            sql,
            (ParserSetupHook) sql_fn_parser_setup,
            pinfo, NULL);
    if (list_length(querytree_list) != 1)
    {
        ereport(WARNING, (errmsg("%s parsed to more than one node", func_name)));
        return NULL;
    }
    querytree = linitial(querytree_list);

    if (!IsA(querytree, Query))
    {
        ereport(WARNING,
                 (errmsg("%s didn't parse to a Query", func_name),
                 errdetail("Got this instead: %s", nodeToString(querytree))));
        return NULL;
    }

    /* We got a Query, so return it for inlining. */

    return querytree;
}


/*
 * temporal_semijoin_sql - build SQL for semijoin query
 */
static void
temporal_semijoin_sql(
    char *left_schema,
    char *left_table,
    char *left_id_col,
    char *left_valid_col,
    char *right_schema,
    char *right_table,
    char *right_id_col,
    char *right_valid_col,
    char **result
) {
    StringInfoData q;
    char *left_nsp_table_q;
    char left_table_q[MAX_QUOTED_NAME_LEN];
    char left_id_col_q[MAX_QUOTED_NAME_LEN];
    char left_valid_col_q[MAX_QUOTED_NAME_LEN];
    char *right_nsp_table_q;
    char right_table_q[MAX_QUOTED_NAME_LEN];
    char right_id_col_q[MAX_QUOTED_NAME_LEN];
    char right_valid_col_q[MAX_QUOTED_NAME_LEN];
    char *result_valid_col_q;    // TODO: parameterize this too (optionally)
    char *subquery_alias;

    left_nsp_table_q = quote_qualified_identifier(left_schema, left_table);
    quoteOneName(left_table_q, left_table);
    quoteOneName(left_id_col_q, left_id_col);
    quoteOneName(left_valid_col_q, left_valid_col);
    right_nsp_table_q = quote_qualified_identifier(right_schema, right_table);
    quoteOneName(right_table_q, right_table);
    quoteOneName(right_id_col_q, right_id_col);
    quoteOneName(right_valid_col_q, right_valid_col);
    result_valid_col_q = left_valid_col_q;

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
     * FROM    public.a
     * JOIN (
     *   SELECT  b.id, range_agg(b.valid_at) AS valid_at
     *   FROM    public.b
     *   GROUP BY b.id
     * ) AS j
     * ON a.id = j.id AND a.valid_at && j.valid_at;
     */
    initStringInfo(&q);
    appendStringInfo(&q,
            "SELECT %2$s.%3$s, UNNEST(multirange(%2$s.%4$s) * %9$s.%8$s) AS %10$s\n"
            "FROM %1$s\n"
            "JOIN (\n"
            "  SELECT %6$s.%7$s, range_agg(%6$s.%8$s) AS %8$s\n"
            "  FROM %5$s\n"
            "  GROUP BY %6$s.%7$s\n"
            ") AS %9$s\n"
            "ON %2$s.%3$s = %9$s.%7$s AND %2$s.%4$s && %9$s.%8$s",
            left_nsp_table_q, left_table_q, left_id_col_q, left_valid_col_q,
            right_nsp_table_q, right_table_q, right_id_col_q, right_valid_col_q,
            subquery_alias, result_valid_col_q);

    *result = q.data;
}

/*
 * Just for testing: replace the real support function with this,
 * so that you can force the PL/pgSQL implementation to run.
 */
Datum
noop_support(PG_FUNCTION_ARGS)
{
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    ereport(NOTICE, (errmsg("noop_support %u", rawreq->type)));
    PG_RETURN_POINTER(NULL);
}

/*
 * Inline the function call.
 *
 * Postgres does this automatically for SRF SQL functions
 * (provided they qualify), but since temporal_semijoin
 * generates its SQL from its parameters, it must be PL/pgSQL instead.
 * As of v19 we can use SupportRequestInlineInFrom to return a Query node,
 * so that Postgres can inline it into the outer query.
 */
Datum
temporal_semijoin_support(PG_FUNCTION_ARGS)
{
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    SupportRequestInlineInFrom *req;
    FuncExpr *expr;
    char *left_schema;
    char *left_table;
    char *left_id_col;
    char *left_valid_col;
    char *right_schema;
    char *right_table;
    char *right_id_col;
    char *right_valid_col;
    char *sql;
    Query *querytree;

    /* We only handle InlineInFrom support requests. */
    if (!IsA(rawreq, SupportRequestInlineInFrom))
        PG_RETURN_POINTER(NULL);

    req = (SupportRequestInlineInFrom *) rawreq;
    expr = (FuncExpr *) req->rtfunc->funcexpr;

    if (list_length(expr->args) != 6)
    {
        ereport(WARNING, (errmsg("temporal_semijoin called with %d args but expected 6", list_length(expr->args))));
        PG_RETURN_POINTER(NULL);
    }

    /*
     * Extract strings from the func's arguments.
     * They must all be Const and TEXT.
     */
    if (!getarg_table_name(expr, 0, "temporal_semijoin", &left_schema, &left_table))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 1, "temporal_semijoin", &left_id_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 2, "temporal_semijoin", &left_valid_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_table_name(expr, 3, "temporal_semijoin", &right_schema, &right_table))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 4, "temporal_semijoin", &right_id_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 5, "temporal_semijoin", &right_valid_col))
        PG_RETURN_POINTER(NULL);

    /*
     * Everything looks good. Build a Node tree for the query.
     * For now it's easiest to let Postgres do it for us,
     * as if it were inlining a SQL function
     * (see inline_set_returning_function in optimizer/util/clauses.c).
     */
    temporal_semijoin_sql(
            left_schema,
            left_table,
            left_id_col,
            left_valid_col,
            right_schema,
            right_table,
            right_id_col,
            right_valid_col,
            &sql);

    querytree = build_query(sql, req, "temporal_semijoin");

    PG_RETURN_POINTER(querytree);
}

/*
 * temporal_antijoin_sql - build SQL for antijoin query
 *
 * Table names must already be quoted and namespaced.
 */
static void
temporal_antijoin_sql(
    char *left_schema,
    char *left_table,
    char *left_id_col,
    char *left_valid_col,
    char *right_schema,
    char *right_table,
    char *right_id_col,
    char *right_valid_col,
    char **result
) {
    StringInfoData q;
    char *left_nsp_table_q;
    char left_table_q[MAX_QUOTED_NAME_LEN];
    char left_id_col_q[MAX_QUOTED_NAME_LEN];
    char left_valid_col_q[MAX_QUOTED_NAME_LEN];
    char *right_nsp_table_q;
    char right_table_q[MAX_QUOTED_NAME_LEN];
    char right_id_col_q[MAX_QUOTED_NAME_LEN];
    char right_valid_col_q[MAX_QUOTED_NAME_LEN];
    char *result_valid_col_q;
    char *subquery_alias;

    left_nsp_table_q = quote_qualified_identifier(left_schema, left_table);
    quoteOneName(left_table_q, left_table);
    quoteOneName(left_id_col_q, left_id_col);
    quoteOneName(left_valid_col_q, left_valid_col);
    right_nsp_table_q = quote_qualified_identifier(right_schema, right_table);
    quoteOneName(right_table_q, right_table);
    quoteOneName(right_id_col_q, right_id_col);
    quoteOneName(right_valid_col_q, right_valid_col);
    result_valid_col_q = left_valid_col_q;

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
     * SELECT  a.id,
     *         UNNEST(CASE WHEN j.valid_at IS NULL THEN multirange(a.valid_at)
     *                     ELSE multirange(a.valid_at) - j.valid_at END) AS valid_at
     * FROM    a
     * LEFT JOIN (
     *   SELECT  b.id, range_agg(b.valid_at) AS valid_at
     *   FROM    b
     *   GROUP BY b.id
     * ) AS j
     * ON a.id = j.id AND a.valid_at && j.valid_at
     * WHERE   NOT isempty(a.valid_at);
     */
    initStringInfo(&q);
    appendStringInfo(&q,
            "SELECT %2$s.%3$s, UNNEST(CASE WHEN %9$s.%8$s IS NULL THEN multirange(%2$s.%4$s)\n"
            "                              ELSE multirange(%2$s.%4$s) - %9$s.%8$s END) AS %10$s\n"
            "FROM %1$s\n"
            "LEFT JOIN (\n"
            "  SELECT %6$s.%7$s, range_agg(%6$s.%8$s) AS %8$s\n"
            "  FROM %5$s\n"
            "  GROUP BY %6$s.%7$s\n"
            ") AS %9$s\n"
            "ON %2$s.%3$s = %9$s.%7$s AND %2$s.%4$s && %9$s.%8$s"
            "WHERE NOT isempty(%2$s.%4$s)",
            left_nsp_table_q, left_table_q, left_id_col_q, left_valid_col_q,
            right_nsp_table_q, right_table_q, right_id_col_q, right_valid_col_q,
            subquery_alias, result_valid_col_q);

    *result = q.data;
}

/*
 * Inline the temporal_antijoin function call.
 */
Datum
temporal_antijoin_support(PG_FUNCTION_ARGS)
{
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    SupportRequestInlineInFrom *req;
    FuncExpr *expr;
    char *left_schema;
    char *left_table;
    char *left_id_col;
    char *left_valid_col;
    char *right_schema;
    char *right_table;
    char *right_id_col;
    char *right_valid_col;
    char *sql;
    Query *querytree;

    /* We only handle InlineInFrom support requests. */
    if (!IsA(rawreq, SupportRequestInlineInFrom))
        PG_RETURN_POINTER(NULL);

    req = (SupportRequestInlineInFrom *) rawreq;
    expr = (FuncExpr *) req->rtfunc->funcexpr;

    if (list_length(expr->args) != 6)
    {
        ereport(WARNING, (errmsg("temporal_antijoin called with %d args but expected 6", list_length(expr->args))));
        PG_RETURN_POINTER(NULL);
    }

    /*
     * Extract strings from the func's arguments.
     * They must all be Const and TEXT.
     */
    if (!getarg_table_name(expr, 0, "temporal_antijoin", &left_schema, &left_table))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 1, "temporal_antijoin", &left_id_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 2, "temporal_antijoin", &left_valid_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_table_name(expr, 3, "temporal_antijoin", &right_schema, &right_table))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 4, "temporal_antijoin", &right_id_col))
        PG_RETURN_POINTER(NULL);
    if (!getarg_cstring(expr, 5, "temporal_antijoin", &right_valid_col))
        PG_RETURN_POINTER(NULL);

    /*
     * Everything looks good. Build a Node tree for the query.
     * For now it's easiest to let Postgres do it for us,
     * as if it were inlining a SQL function
     * (see inline_set_returning_function in optimizer/util/clauses.c).
     */
    temporal_antijoin_sql(
            left_schema,
            left_table,
            left_id_col,
            left_valid_col,
            right_schema,
            right_table,
            right_id_col,
            right_valid_col,
            &sql);

    querytree = build_query(sql, req, "temporal_antijoin");

    PG_RETURN_POINTER(querytree);
}
