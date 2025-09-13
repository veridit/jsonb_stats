#include "postgres.h"
#include "fmgr.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC;

// Forward declarations for internal jsonb functions
JsonbIterator *JsonbIteratorInit(JsonbContainer *container);
uint32 JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested);

// Function prototypes
Datum stat(PG_FUNCTION_ARGS);
Datum stats(PG_FUNCTION_ARGS);
static int	compare_jsonb_string_values(const JsonbValue *a, const JsonbValue *b);
Datum jsonb_stats_sfunc(PG_FUNCTION_ARGS);
static void push_summary(JsonbParseState **parse_state, JsonbValue *state_val, JsonbValue *stat_v, bool init);
Datum jsonb_stats_accum(PG_FUNCTION_ARGS);
Datum jsonb_stats_final(PG_FUNCTION_ARGS);



PG_FUNCTION_INFO_V1(stat);
Datum
stat(PG_FUNCTION_ARGS)
{
	Datum		value_datum = PG_GETARG_DATUM(0);
	Oid			value_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
	JsonbPair	pairs[2];
	JsonbValue	v_object;
	Jsonb	   *res;

	if (value_type == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("could not determine input data type")));

	/* "type" key/value pair. Ordered alphabetically. */
	pairs[0].key.type = jbvString;
	pairs[0].key.val.string.val = "type";
	pairs[0].key.val.string.len = 4;
	pairs[0].value.type = jbvString;

	if (value_type == INT4OID)
		pairs[0].value.val.string.val = "int";
	else if (value_type == FLOAT8OID)
		pairs[0].value.val.string.val = "float";
	else if (value_type == BOOLOID)
		pairs[0].value.val.string.val = "bool";
	else if (value_type == TEXTOID)
		pairs[0].value.val.string.val = "str";
	else if (value_type == DATEOID)
		pairs[0].value.val.string.val = "date";
	else if (value_type == NUMERICOID)
		pairs[0].value.val.string.val = "dec2";
	else if (get_element_type(value_type) != InvalidOid)
		pairs[0].value.val.string.val = "arr";
	else
		pairs[0].value.val.string.val = format_type_be(value_type); /* fallback */
	pairs[0].value.val.string.len = strlen(pairs[0].value.val.string.val);

	/* "value" key/value pair */
	pairs[1].key.type = jbvString;
	pairs[1].key.val.string.val = "value";
	pairs[1].key.val.string.len = 5;

	switch (value_type)
	{
		case INT4OID:
			{
				int32		ival = DatumGetInt32(value_datum);
				Datum		n = DirectFunctionCall1(int4_numeric, Int32GetDatum(ival));

				pairs[1].value.type = jbvNumeric;
				pairs[1].value.val.numeric = DatumGetNumeric(n);
				break;
			}
		case FLOAT8OID:
			{
				Datum n = DirectFunctionCall1(float8_numeric, value_datum);
				pairs[1].value.type = jbvNumeric;
				pairs[1].value.val.numeric = DatumGetNumeric(n);
				break;
			}
		case BOOLOID:
			{
				pairs[1].value.type = jbvBool;
				pairs[1].value.val.boolean = DatumGetBool(value_datum);
				break;
			}
		case TEXTOID:
			{
				char	   *s = text_to_cstring(DatumGetTextPP(value_datum));

				pairs[1].value.type = jbvString;
				pairs[1].value.val.string.val = s;
				pairs[1].value.val.string.len = strlen(s);
				break;
			}
		case DATEOID:
			{
				char	   *s = DatumGetCString(DirectFunctionCall1(date_out, value_datum));

				pairs[1].value.type = jbvString;
				pairs[1].value.val.string.val = s;
				pairs[1].value.val.string.len = strlen(s);
				break;
			}
		case NUMERICOID:
			{
				pairs[1].value.type = jbvNumeric;
				pairs[1].value.val.numeric = DatumGetNumeric(value_datum);
				break;
			}
		default:
			{
				if (get_element_type(value_type) != InvalidOid)
				{
					/*
					 * array_to_json gives TEXT, which jsonb_in can parse. The
					 * resulting jsonb must be copied into the current memory
					 * context, as the result of DirectFunctionCall1 is in a
					 * short-lived context.
					 */
					Datum		json_text = DirectFunctionCall1(array_to_json, value_datum);
					Datum		jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(text_to_cstring(DatumGetTextPP(json_text))));
					Jsonb	   *jb_tmp = DatumGetJsonbP(jsonb_datum);
					Jsonb	   *jb = palloc(VARSIZE(jb_tmp));

					memcpy(jb, jb_tmp, VARSIZE(jb_tmp));

					pairs[1].value.type = jbvBinary;
					pairs[1].value.val.binary.data = &jb->root;
					pairs[1].value.val.binary.len = VARSIZE_ANY_EXHDR(jb);
				}
				else
				{
					/* For any other type, just treat it as a string */
					Oid			typoutput;
					bool		typisvarlena;
					char	   *s;

					getTypeOutputInfo(value_type, &typoutput, &typisvarlena);
					s = OidOutputFunctionCall(typoutput, value_datum);
					pairs[1].value.type = jbvString;
					pairs[1].value.val.string.val = s;
					pairs[1].value.val.string.len = strlen(s);
				}
				break;
			}
	}

	v_object.type = jbvObject;
	v_object.val.object.nPairs = 2;
	v_object.val.object.pairs = pairs;

	res = JsonbValueToJsonb(&v_object);
	PG_RETURN_JSONB_P(res);
}

PG_FUNCTION_INFO_V1(stats);
Datum
stats(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonbParseState *ps = NULL;
	JsonbIterator *it;
	JsonbValue	k,
				v;
	JsonbValue	type_k,
				type_v;
	bool		inserted = false;
	Jsonb	   *res;

	if (!JB_ROOT_IS_OBJECT(jb))
		PG_RETURN_JSONB_P(jb);

	type_k.type = jbvString;
	type_k.val.string.val = "type";
	type_k.val.string.len = 4;
	type_v.type = jbvString;
	type_v.val.string.val = "stats";
	type_v.val.string.len = 5;

	it = JsonbIteratorInit(&jb->root);
	(void) JsonbIteratorNext(&it, &k, false);	/* begin object */

	pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

	while (JsonbIteratorNext(&it, &k, false) == WJB_KEY)
	{
		if (!inserted && compare_jsonb_string_values(&k, &type_k) > 0)
		{
			pushJsonbValue(&ps, WJB_KEY, &type_k);
			pushJsonbValue(&ps, WJB_VALUE, &type_v);
			inserted = true;
		}
		pushJsonbValue(&ps, WJB_KEY, &k);
		(void) JsonbIteratorNext(&it, &v, true);
		pushJsonbValue(&ps, WJB_VALUE, &v);
	}

	if (!inserted)
	{
		pushJsonbValue(&ps, WJB_KEY, &type_k);
		pushJsonbValue(&ps, WJB_VALUE, &type_v);
	}

	res = JsonbValueToJsonb(pushJsonbValue(&ps, WJB_END_OBJECT, NULL));
	PG_RETURN_JSONB_P(res);
}

static int
compare_jsonb_string_values(const JsonbValue *a, const JsonbValue *b)
{
	int			cmp;

	cmp = strncmp(a->val.string.val, b->val.string.val,
				  a->val.string.len < b->val.string.len ? a->val.string.len : b->val.string.len);
	if (cmp == 0)
		return a->val.string.len - b->val.string.len;
	return cmp;
}

static int
qsort_strcmp(const void *a, const void *b)
{
	return strcmp(*(const char **) a, *(const char **) b);
}

PG_FUNCTION_INFO_V1(jsonb_stats_sfunc);
Datum
jsonb_stats_sfunc(PG_FUNCTION_ARGS)
{
	Jsonb	   *state = PG_GETARG_JSONB_P(0);
	text	   *code_text = PG_GETARG_TEXT_PP(1);
	Jsonb	   *stat_val = PG_GETARG_JSONB_P(2);
	char	   *code = text_to_cstring(code_text);
	JsonbParseState *parse_state = NULL;
	JsonbIterator *it;
	JsonbValue	v_key,
				v_val,
				new_key,
				new_val,
				type_key,
				type_val;
	bool		inserted = false;
	Jsonb	   *res;
	uint32		r;
	MemoryContext agg_context,
				old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "jsonb_stats_sfunc called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	if (!JB_ROOT_IS_OBJECT(state))
		elog(ERROR, "jsonb_stats_sfunc state must be a jsonb object");

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	it = JsonbIteratorInit(&state->root);

	new_key.type = jbvString;
	new_key.val.string.val = code;
	new_key.val.string.len = strlen(code);
	{
		Jsonb *jb_stat_val = palloc(VARSIZE(stat_val));
		memcpy(jb_stat_val, stat_val, VARSIZE(stat_val));
		new_val.type = jbvBinary;
		new_val.val.binary.data = &jb_stat_val->root;
		new_val.val.binary.len = VARSIZE_ANY_EXHDR(stat_val);
	}

	type_key.type = jbvString;
	type_key.val.string.val = "type";
	type_key.val.string.len = 4;
	type_val.type = jbvString;
	type_val.val.string.val = "stats";
	type_val.val.string.len = 5;

	(void) JsonbIteratorNext(&it, &v_key, false);	/* consume WJB_BEGIN_OBJECT */
	r = JsonbIteratorNext(&it, &v_key, false);

	if (r == WJB_END_OBJECT)
	{
		/* On first call, state is empty. Create object with new key and type key. */
		if (compare_jsonb_string_values(&new_key, &type_key) < 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &new_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &new_val);
			pushJsonbValue(&parse_state, WJB_KEY, &type_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &type_val);
		}
		else
		{
			pushJsonbValue(&parse_state, WJB_KEY, &type_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &type_val);
			pushJsonbValue(&parse_state, WJB_KEY, &new_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &new_val);
		}
	}
	else
	{
		/* State is not empty, merge new key in. It should already have a type key. */
		do
		{
			if (!inserted && compare_jsonb_string_values(&new_key, &v_key) < 0)
			{
				pushJsonbValue(&parse_state, WJB_KEY, &new_key);
				pushJsonbValue(&parse_state, WJB_VALUE, &new_val);
				inserted = true;
			}

			pushJsonbValue(&parse_state, WJB_KEY, &v_key);
			(void) JsonbIteratorNext(&it, &v_val, true);
			if (v_val.type == jbvBinary)
			{
				Jsonb	   *jb_val = palloc(VARHDRSZ + v_val.val.binary.len);

				SET_VARSIZE(jb_val, VARHDRSZ + v_val.val.binary.len);
				memcpy(&jb_val->root, v_val.val.binary.data, v_val.val.binary.len);
				v_val.val.binary.data = &jb_val->root;
			}
			pushJsonbValue(&parse_state, WJB_VALUE, &v_val);
		}
		while (JsonbIteratorNext(&it, &v_key, false) == WJB_KEY);


		if (!inserted)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &new_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &new_val);
		}
	}

	res = JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL));

	MemoryContextSwitchTo(old_context);

	PG_RETURN_JSONB_P(res);
}


static void
push_summary_init(JsonbParseState **ps, JsonbValue *stat_v)
{
	JsonbValue	v_k, v, key, val;
	char	   *type_name;
	JsonbIterator *it;
	Jsonb	   *stat_jb = JsonbValueToJsonb(stat_v); /* Ensure it's a materialized jsonb */

	it = JsonbIteratorInit(&stat_jb->root);
	(void) JsonbIteratorNext(&it, &v, false);	/* consume WJB_BEGIN_OBJECT */
	(void) JsonbIteratorNext(&it, &v_k, false);	/* consume "type" key */
	(void) JsonbIteratorNext(&it, &v, true);	/* consume "type" value */
	type_name = pnstrdup(v.val.string.val, v.val.string.len);
	(void) JsonbIteratorNext(&it, &v_k, false);	/* consume "value" key */
	(void) JsonbIteratorNext(&it, &v, true);	/* consume "value" value */

	pushJsonbValue(ps, WJB_BEGIN_OBJECT, NULL);
	key.type = jbvString;

	if (strcmp(type_name, "int") == 0 || strcmp(type_name, "float") == 0 || strcmp(type_name, "dec2") == 0)
	{
		Datum		new_sum = NumericGetDatum(v.val.numeric);
		if (strcmp(type_name, "dec2") == 0)
		{
			Datum n_100 = DirectFunctionCall1(int4_numeric, Int32GetDatum(100));
			new_sum = DirectFunctionCall2(numeric_round, DirectFunctionCall2(numeric_mul, new_sum, n_100), Int32GetDatum(0));
		}
		val.type = jbvNumeric;
		key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(ps, WJB_KEY, &key);
		val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1))); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.val.numeric = DatumGetNumeric(new_sum); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "mean"; key.val.string.len = 4; pushJsonbValue(ps, WJB_KEY, &key);
		val.val.numeric = DatumGetNumeric(new_sum); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.val.numeric = DatumGetNumeric(new_sum); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.val.numeric = DatumGetNumeric(new_sum); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "sum_sq_diff"; key.val.string.len = 11; pushJsonbValue(ps, WJB_KEY, &key);
		val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(0))); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvString;
		if (strcmp(type_name, "int") == 0) { val.val.string.val = "int_agg"; val.val.string.len = 7; }
		else if (strcmp(type_name, "float") == 0) { val.val.string.val = "float_agg"; val.val.string.len = 9; }
		else { val.val.string.val = "dec2_agg"; val.val.string.len = 8; }
		pushJsonbValue(ps, WJB_VALUE, &val);
	}
	/* Add other types here in subsequent steps */

	pushJsonbValue(ps, WJB_END_OBJECT, NULL);
	pfree(type_name);
}

PG_FUNCTION_INFO_V1(jsonb_stats_accum);
Datum
jsonb_stats_accum(PG_FUNCTION_ARGS)
{
	Jsonb	   *state = PG_GETARG_JSONB_P(0);
	Jsonb	   *stats = PG_GETARG_JSONB_P(1);
	JsonbParseState *ps = NULL;
	JsonbIterator *it_state, *it_stats;
	JsonbValue	k_state, v_state, k_stats, v_stats;
	uint32		r_state, r_stats;
	int			cmp;
	MemoryContext agg_context, old_context;
	Jsonb	   *res;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "jsonb_stats_accum called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	it_state = JsonbIteratorInit(&state->root);
	it_stats = JsonbIteratorInit(&stats->root);
	r_state = JsonbIteratorNext(&it_state, &k_state, false);
	r_stats = JsonbIteratorNext(&it_stats, &k_stats, false);

	pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);
	r_state = JsonbIteratorNext(&it_state, &k_state, false);
	r_stats = JsonbIteratorNext(&it_stats, &k_stats, false);

	while (r_state == WJB_KEY || r_stats == WJB_KEY)
	{
		if (r_stats == WJB_KEY && k_stats.type == jbvString && k_stats.val.string.len == 4 && strncmp(k_stats.val.string.val, "type", 4) == 0)
		{
			(void) JsonbIteratorNext(&it_stats, &v_stats, true);
			r_stats = JsonbIteratorNext(&it_stats, &k_stats, false);
			continue;
		}

		if (r_state != WJB_KEY) cmp = 1;
		else if (r_stats != WJB_KEY) cmp = -1;
		else cmp = compare_jsonb_string_values(&k_state, &k_stats);

		if (cmp < 0) /* Key only in state, copy it */
		{
			pushJsonbValue(&ps, WJB_KEY, &k_state);
			(void) JsonbIteratorNext(&it_state, &v_state, true);
			pushJsonbValue(&ps, WJB_VALUE, &v_state);
			r_state = JsonbIteratorNext(&it_state, &k_state, false);
		}
		else if (cmp > 0) /* Key only in stats, initialize a new summary */
		{
			pushJsonbValue(&ps, WJB_KEY, &k_stats);
			(void) JsonbIteratorNext(&it_stats, &v_stats, true);
			push_summary_init(&ps, &v_stats);
			r_stats = JsonbIteratorNext(&it_stats, &k_stats, false);
		}
		else /* Key in both, for now just copy old state */
		{
			pushJsonbValue(&ps, WJB_KEY, &k_state);
			(void) JsonbIteratorNext(&it_state, &v_state, true);
			(void) JsonbIteratorNext(&it_stats, &v_stats, true);
			pushJsonbValue(&ps, WJB_VALUE, &v_state);
			r_state = JsonbIteratorNext(&it_state, &k_state, false);
			r_stats = JsonbIteratorNext(&it_stats, &k_stats, false);
		}
	}

	res = JsonbValueToJsonb(pushJsonbValue(&ps, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(old_context);
	PG_RETURN_JSONB_P(res);
}

PG_FUNCTION_INFO_V1(jsonb_stats_final);
Datum
jsonb_stats_final(PG_FUNCTION_ARGS)
{
	Jsonb	   *summary = PG_GETARG_JSONB_P(0);
	JsonbParseState *parse_state = NULL;
	JsonbIterator *it;
	JsonbValue	k,
				v;
	bool		type_inserted = false;
	JsonbValue	type_key,
				type_val;

	/*
	 * Stubbed for stability. This function just adds the top-level 'type'
	 * key without processing the nested summaries, which is where the crash
	 * occurs.
	 */

	if (!JB_ROOT_IS_OBJECT(summary))
		PG_RETURN_JSONB_P(summary);

	it = JsonbIteratorInit(&summary->root);
	(void) JsonbIteratorNext(&it, &k, false);	/* consume { */
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	type_key.type = jbvString;
	type_key.val.string.val = "type";
	type_key.val.string.len = 4;
	type_val.type = jbvString;
	type_val.val.string.val = "stats_agg";
	type_val.val.string.len = 9;

	while (JsonbIteratorNext(&it, &k, false) == WJB_KEY)
	{
		if (!type_inserted && compare_jsonb_string_values(&k, &type_key) > 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &type_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &type_val);
			type_inserted = true;
		}

		pushJsonbValue(&parse_state, WJB_KEY, &k);
		(void) JsonbIteratorNext(&it, &v, true);
		pushJsonbValue(&parse_state, WJB_VALUE, &v);
	}

	if (!type_inserted)
	{
		pushJsonbValue(&parse_state, WJB_KEY, &type_key);
		pushJsonbValue(&parse_state, WJB_VALUE, &type_val);
	}

	PG_RETURN_JSONB_P(JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL)));
}

PG_FUNCTION_INFO_V1(jsonb_stats_merge);
Datum
jsonb_stats_merge(PG_FUNCTION_ARGS)
{
	/* Stubbed for stability: returns the first state. */
	Jsonb	   *a = PG_GETARG_JSONB_P(0);
	PG_RETURN_JSONB_P(a);
}

