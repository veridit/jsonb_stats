#include "postgres.h"
#include "fmgr.h"

#include "utils/jsonb.h"

PG_MODULE_MAGIC;

// Function prototypes
Datum stat_c(PG_FUNCTION_ARGS);
Datum jsonb_stats_sfunc_c(PG_FUNCTION_ARGS);
Datum jsonb_stats_summary_accum_c(PG_FUNCTION_ARGS);
Datum jsonb_stats_to_summary_round_c(PG_FUNCTION_ARGS);
Datum jsonb_stats_summary_merge_c(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(stat_c);
Datum
stat_c(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("function \"stat_c\" not implemented")));
    PG_RETURN_NULL(); // Unreachable
}


PG_FUNCTION_INFO_V1(jsonb_stats_sfunc_c);
Datum
jsonb_stats_sfunc_c(PG_FUNCTION_ARGS)
{
    Jsonb *state = PG_GETARG_JSONB_P(0);
    // text *code = PG_GETARG_TEXT_PP(1);
    // Jsonb *stat_val = PG_GETARG_JSONB_P(2);

    // Stub implementation: just return the state unchanged.
    PG_RETURN_JSONB_P(state);
}

PG_FUNCTION_INFO_V1(jsonb_stats_summary_accum_c);
Datum
jsonb_stats_summary_accum_c(PG_FUNCTION_ARGS)
{
    Jsonb *state = PG_GETARG_JSONB_P(0);
    // Jsonb *stats = PG_GETARG_JSONB_P(1);

    // Stub implementation: just return the state unchanged.
    PG_RETURN_JSONB_P(state);
}

PG_FUNCTION_INFO_V1(jsonb_stats_to_summary_round_c);
Datum
jsonb_stats_to_summary_round_c(PG_FUNCTION_ARGS)
{
    Jsonb *state = PG_GETARG_JSONB_P(0);

    // Stub implementation: just return the state unchanged.
    PG_RETURN_JSONB_P(state);
}

PG_FUNCTION_INFO_V1(jsonb_stats_summary_merge_c);
Datum
jsonb_stats_summary_merge_c(PG_FUNCTION_ARGS)
{
    Jsonb *a = PG_GETARG_JSONB_P(0);
    // Jsonb *b = PG_GETARG_JSONB_P(1);

    // Stub implementation: just return the first argument unchanged.
    PG_RETURN_JSONB_P(a);
}
