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
static void push_summary(JsonbParseState **parse_state, JsonbValue *state_val, JsonbIterator **it, bool init);
Datum jsonb_stats_accum(PG_FUNCTION_ARGS);
Datum jsonb_stats_final(PG_FUNCTION_ARGS);
static void
merge_summaries(JsonbParseState **ps, JsonbValue *v_a, JsonbValue *v_b)
{
	JsonbValue	key_finder;
	JsonbValue *v_type_ptr_a,
			   *v_type_ptr_b;
	char	   *type_a,
			   *type_b;
	Jsonb	   *jb_a;
	Jsonb	   *jb_b;

	jb_a = JsonbValueToJsonb(v_a);

	jb_b = JsonbValueToJsonb(v_b);

	key_finder.type = jbvString;
	key_finder.val.string.val = "type";
	key_finder.val.string.len = 4;

	v_type_ptr_a = findJsonbValueFromContainer(&jb_a->root, JB_FOBJECT, &key_finder);
	v_type_ptr_b = findJsonbValueFromContainer(&jb_b->root, JB_FOBJECT, &key_finder);

	if (v_type_ptr_a == NULL || v_type_ptr_b == NULL)
		elog(ERROR, "malformed summary object: 'type' key is missing");

	type_a = pnstrdup(v_type_ptr_a->val.string.val, v_type_ptr_a->val.string.len);
	type_b = pnstrdup(v_type_ptr_b->val.string.val, v_type_ptr_b->val.string.len);

	elog(DEBUG1, "merge_summaries: merging type_a=%s and type_b=%s", type_a, type_b);

	if (strcmp(type_a, type_b) != 0)
		elog(ERROR, "type mismatch in summary merge: %s vs %s", type_a, type_b);

	pushJsonbValue(ps, WJB_BEGIN_OBJECT, NULL);

	if (strcmp(type_a, "int_agg") == 0 || strcmp(type_a, "dec2_agg") == 0 || strcmp(type_a, "float_agg") == 0)
	{
		Datum		sum_a,
					count_a,
					mean_a,
					min_a,
					max_a,
					sum_sq_diff_a,
					sum_b,
					count_b,
					mean_b,
					min_b,
					max_b,
					sum_sq_diff_b;
		Datum		total_count,
					delta;
		JsonbValue *v_ptr;
		JsonbValue	key,
					val;

#define FIND_NUMERIC_MERGE(container, key_name, var) \
	key_finder.val.string.val = key_name; \
	key_finder.val.string.len = strlen(key_name); \
	v_ptr = findJsonbValueFromContainer(container, JB_FOBJECT, &key_finder); \
	var = NumericGetDatum(v_ptr->val.numeric)

		FIND_NUMERIC_MERGE(&jb_a->root, "sum", sum_a);
		FIND_NUMERIC_MERGE(&jb_a->root, "count", count_a);
		FIND_NUMERIC_MERGE(&jb_a->root, "mean", mean_a);
		FIND_NUMERIC_MERGE(&jb_a->root, "min", min_a);
		FIND_NUMERIC_MERGE(&jb_a->root, "max", max_a);
		FIND_NUMERIC_MERGE(&jb_a->root, "sum_sq_diff", sum_sq_diff_a);
		FIND_NUMERIC_MERGE(&jb_b->root, "sum", sum_b);
		FIND_NUMERIC_MERGE(&jb_b->root, "count", count_b);
		FIND_NUMERIC_MERGE(&jb_b->root, "mean", mean_b);
		FIND_NUMERIC_MERGE(&jb_b->root, "min", min_b);
		FIND_NUMERIC_MERGE(&jb_b->root, "max", max_b);
		FIND_NUMERIC_MERGE(&jb_b->root, "sum_sq_diff", sum_sq_diff_b);


		total_count = DirectFunctionCall2(numeric_add, count_a, count_b);
		delta = DirectFunctionCall2(numeric_sub, mean_b, mean_a);

		key.type = jbvString;
		key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(total_count); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DatumGetBool(DirectFunctionCall2(numeric_gt, max_a, max_b)) ? max_a : max_b); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "mean"; key.val.string.len = 4; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_add, mean_a, DirectFunctionCall2(numeric_div, DirectFunctionCall2(numeric_mul, delta, count_b), total_count))); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DatumGetBool(DirectFunctionCall2(numeric_lt, min_a, min_b)) ? min_a : min_b); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_add, sum_a, sum_b)); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "sum_sq_diff"; key.val.string.len = 11; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_add, DirectFunctionCall2(numeric_add, sum_sq_diff_a, sum_sq_diff_b), DirectFunctionCall2(numeric_mul, DirectFunctionCall2(numeric_mul, delta, delta), DirectFunctionCall2(numeric_div, DirectFunctionCall2(numeric_mul, count_a, count_b), total_count)))); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvString;
		if (strcmp(type_a, "int_agg") == 0)
		{
			val.val.string.val = "int_agg";
			val.val.string.len = 7;
		}
		else if (strcmp(type_a, "float_agg") == 0)
		{
			val.val.string.val = "float_agg";
			val.val.string.len = 9;
		}
		else
		{
			val.val.string.val = "dec2_agg";
			val.val.string.len = 8;
		}
		pushJsonbValue(ps, WJB_VALUE, &val);
	}
	else if (strcmp(type_a, "str_agg") == 0 || strcmp(type_a, "bool_agg") == 0 || strcmp(type_a, "arr_agg") == 0)
	{
		JsonbValue *v_counts_a_ptr,
				   *v_counts_b_ptr;
		JsonbIterator *it_a,
				   *it_b;
		JsonbValue	k_a_counts,
					v_a_counts,
					k_b_counts,
					v_b_counts,
					key,
					val;
		uint32		r_a,
					r_b;
		Jsonb *counts_a_jsonb, *counts_b_jsonb;

		if (strcmp(type_a, "arr_agg") == 0)
		{
			JsonbValue *v_ptr;
			Datum		count_a, count_b, total_count;

			key_finder.val.string.val = "count"; key_finder.val.string.len = 5;
			v_ptr = findJsonbValueFromContainer(&jb_a->root, JB_FOBJECT, &key_finder); count_a = NumericGetDatum(v_ptr->val.numeric);
			v_ptr = findJsonbValueFromContainer(&jb_b->root, JB_FOBJECT, &key_finder); count_b = NumericGetDatum(v_ptr->val.numeric);
			total_count = DirectFunctionCall2(numeric_add, count_a, count_b);

			key.type = jbvString;
			key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(ps, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(total_count); pushJsonbValue(ps, WJB_VALUE, &val);
		}

		key_finder.val.string.val = "counts";
		key_finder.val.string.len = 6;
		v_counts_a_ptr = findJsonbValueFromContainer(&jb_a->root, JB_FOBJECT, &key_finder);
		v_counts_b_ptr = findJsonbValueFromContainer(&jb_b->root, JB_FOBJECT, &key_finder);

		counts_a_jsonb = JsonbValueToJsonb(v_counts_a_ptr);

		counts_b_jsonb = JsonbValueToJsonb(v_counts_b_ptr);

		it_a = JsonbIteratorInit(&counts_a_jsonb->root);
		it_b = JsonbIteratorInit(&counts_b_jsonb->root);
		(void) JsonbIteratorNext(&it_a, &k_a_counts, false);
		(void) JsonbIteratorNext(&it_b, &k_b_counts, false);
		r_a = JsonbIteratorNext(&it_a, &k_a_counts, false);
		r_b = JsonbIteratorNext(&it_b, &k_b_counts, false);

		key.type = jbvString;
		key.val.string.val = "counts"; key.val.string.len = 6;
		pushJsonbValue(ps, WJB_KEY, &key);
		pushJsonbValue(ps, WJB_BEGIN_OBJECT, NULL);
		while (r_a == WJB_KEY && r_b == WJB_KEY)
		{
			int			cmp = compare_jsonb_string_values(&k_a_counts, &k_b_counts);

			if (cmp < 0)
			{
				pushJsonbValue(ps, WJB_KEY, &k_a_counts);
				(void) JsonbIteratorNext(&it_a, &v_a_counts, true);
				pushJsonbValue(ps, WJB_VALUE, &v_a_counts);
				r_a = JsonbIteratorNext(&it_a, &k_a_counts, false);
			}
			else if (cmp > 0)
			{
				pushJsonbValue(ps, WJB_KEY, &k_b_counts);
				(void) JsonbIteratorNext(&it_b, &v_b_counts, true);
				pushJsonbValue(ps, WJB_VALUE, &v_b_counts);
				r_b = JsonbIteratorNext(&it_b, &k_b_counts, false);
			}
			else
			{
				Datum		new_count;

				pushJsonbValue(ps, WJB_KEY, &k_a_counts);
				(void) JsonbIteratorNext(&it_a, &v_a_counts, true);
				(void) JsonbIteratorNext(&it_b, &v_b_counts, true);
				new_count = DirectFunctionCall2(numeric_add,
												NumericGetDatum(v_a_counts.val.numeric),
												NumericGetDatum(v_b_counts.val.numeric));
				val.type = jbvNumeric;
				val.val.numeric = DatumGetNumeric(new_count);
				pushJsonbValue(ps, WJB_VALUE, &val);
				r_a = JsonbIteratorNext(&it_a, &k_a_counts, false);
				r_b = JsonbIteratorNext(&it_b, &k_b_counts, false);
			}
		}
		while (r_a == WJB_KEY)
		{
			pushJsonbValue(ps, WJB_KEY, &k_a_counts);
			(void) JsonbIteratorNext(&it_a, &v_a_counts, true);
			pushJsonbValue(ps, WJB_VALUE, &v_a_counts);
			r_a = JsonbIteratorNext(&it_a, &k_a_counts, false);
		}
		while (r_b == WJB_KEY)
		{
			pushJsonbValue(ps, WJB_KEY, &k_b_counts);
			(void) JsonbIteratorNext(&it_b, &v_b_counts, true);
			pushJsonbValue(ps, WJB_VALUE, &v_b_counts);
			r_b = JsonbIteratorNext(&it_b, &k_b_counts, false);
		}
		pushJsonbValue(ps, WJB_END_OBJECT, NULL);

		key.val.string.val = "type"; key.val.string.len = 4;
		pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvString;
		if (strcmp(type_a, "str_agg") == 0)
			val.val.string.val = "str_agg";
		else if (strcmp(type_a, "bool_agg") == 0)
			val.val.string.val = "bool_agg";
		else
			val.val.string.val = "arr_agg";
		val.val.string.len = strlen(val.val.string.val);
		pushJsonbValue(ps, WJB_VALUE, &val);
	}

	pushJsonbValue(ps, WJB_END_OBJECT, NULL);
	pfree(type_a);
	pfree(type_b);
}



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
push_summary(JsonbParseState **parse_state, JsonbValue *state_val, JsonbIterator **it, bool init)
{
	JsonbValue	v_k,
				v;
	char	   *type_name;
	char	   *summary_type_name;
	uint32		r;
	JsonbParseState *ps_new = NULL;
	JsonbValue	new_v;
	Jsonb	   *new_summary_jb;

	/* For update, just copy the old state for now */
	if (!init)
	{
		JsonbValue *v_state_type_ptr,
				   *v_state_val_ptr,
					key_finder;
		char	   *state_type_name;
		Jsonb	   *state_jb;

		(void) JsonbIteratorNext(it, &v, false);	/* consume stat WJB_BEGIN_OBJECT */
		(void) JsonbIteratorNext(it, &v_k, false);	/* stat "type" key */
		(void) JsonbIteratorNext(it, &v, true); /* stat "type" val */
		type_name = pnstrdup(v.val.string.val, v.val.string.len);
		(void) JsonbIteratorNext(it, &v_k, false);	/* stat "value" key */
		(void) JsonbIteratorNext(it, &v, true); /* stat "value" val */
		(void) JsonbIteratorNext(it, &v_k, false);	/* consume stat WJB_END_OBJECT */

		state_jb = JsonbValueToJsonb(state_val);

		key_finder.type = jbvString;
		key_finder.val.string.val = "type";
		key_finder.val.string.len = 4;
		v_state_type_ptr = findJsonbValueFromContainer(&state_jb->root, JB_FOBJECT, &key_finder);
		if (v_state_type_ptr == NULL)
			elog(ERROR, "summary object is missing 'type' key");

		state_type_name = pnstrdup(v_state_type_ptr->val.string.val, v_state_type_ptr->val.string.len);

		if (strcmp(type_name, "int") != 0 && strcmp(type_name, "str") != 0 && strcmp(type_name, "bool") != 0 && strcmp(type_name, "dec2") != 0 && strcmp(type_name, "float") != 0)
		{
			if (strcmp(state_type_name, "arr_agg") != 0)
				elog(ERROR, "type mismatch in summary update");
		}
		else if (strcmp(type_name, "int") == 0)
		{
			if (strcmp(state_type_name, "int_agg") != 0)
				elog(ERROR, "type mismatch in summary update");
		}
		else if (strcmp(type_name, "float") == 0)
		{
			if (strcmp(state_type_name, "float_agg") != 0)
				elog(ERROR, "type mismatch in summary update");
		}
		else if (strcmp(type_name, "str") == 0)
		{
			if (strcmp(state_type_name, "str_agg") != 0)
				elog(ERROR, "type mismatch in summary update");
		}
		else if (strcmp(type_name, "bool") == 0)
		{
			if (strcmp(state_type_name, "bool_agg") != 0)
				elog(ERROR, "type mismatch in summary update");
		}
		else if (strcmp(type_name, "dec2") == 0)
		{
			if (strcmp(state_type_name, "dec2_agg") != 0)
				elog(ERROR, "type mismatch in summary update");
		}


		if (strcmp(type_name, "int") == 0 || strcmp(type_name, "dec2") == 0 || strcmp(type_name, "float") == 0)
		{
			Datum		sum,
						count,
						mean,
						min,
						max,
						sum_sq_diff,
						delta,
						new_val_numeric;
			JsonbValue	key,
						val;

			pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);

			if (strcmp(type_name, "dec2") == 0)
			{
				Datum n_100 = DirectFunctionCall1(int4_numeric, Int32GetDatum(100));
				Datum n_val = DirectFunctionCall2(numeric_mul, NumericGetDatum(v.val.numeric), n_100);
				new_val_numeric = DirectFunctionCall2(numeric_round, n_val, Int32GetDatum(0));
			}
			else
			{
				new_val_numeric = NumericGetDatum(v.val.numeric);
			}

#define FIND_NUMERIC(key_name, var) \
	key_finder.val.string.val = key_name; \
	key_finder.val.string.len = strlen(key_name); \
	v_state_val_ptr = findJsonbValueFromContainer(&state_jb->root, JB_FOBJECT, &key_finder); \
	var = NumericGetDatum(v_state_val_ptr->val.numeric)

			FIND_NUMERIC("sum", sum);
			FIND_NUMERIC("count", count);
			FIND_NUMERIC("mean", mean);
			FIND_NUMERIC("min", min);
			FIND_NUMERIC("max", max);
			FIND_NUMERIC("sum_sq_diff", sum_sq_diff);

			sum = DirectFunctionCall2(numeric_add, sum, new_val_numeric);
			count = DirectFunctionCall2(numeric_add, count, DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));
			delta = DirectFunctionCall2(numeric_sub, new_val_numeric, mean);
			mean = DirectFunctionCall2(numeric_add, mean, DirectFunctionCall2(numeric_div, delta, count));
			min = DatumGetBool(DirectFunctionCall2(numeric_lt, new_val_numeric, min)) ? new_val_numeric : min;
			max = DatumGetBool(DirectFunctionCall2(numeric_gt, new_val_numeric, max)) ? new_val_numeric : max;
			sum_sq_diff = DirectFunctionCall2(numeric_add, sum_sq_diff,
											  DirectFunctionCall2(numeric_mul, delta,
																  DirectFunctionCall2(numeric_sub, new_val_numeric, mean)));

			key.type = jbvString;
			key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(count); pushJsonbValue(&ps_new, WJB_VALUE, &val);
			key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(max); pushJsonbValue(&ps_new, WJB_VALUE, &val);
			key.val.string.val = "mean"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(mean); pushJsonbValue(&ps_new, WJB_VALUE, &val);
			key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(min); pushJsonbValue(&ps_new, WJB_VALUE, &val);
			key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(sum); pushJsonbValue(&ps_new, WJB_VALUE, &val);
			key.val.string.val = "sum_sq_diff"; key.val.string.len = 11; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(sum_sq_diff); pushJsonbValue(&ps_new, WJB_VALUE, &val);
			key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvString;
			if (strcmp(type_name, "int") == 0)
			{
				val.val.string.val = "int_agg"; val.val.string.len = 7;
			}
			else if (strcmp(type_name, "float") == 0)
			{
				val.val.string.val = "float_agg"; val.val.string.len = 9;
			}
			else
			{
				val.val.string.val = "dec2_agg"; val.val.string.len = 8;
			}
			pushJsonbValue(&ps_new, WJB_VALUE, &val);
		}
		else if (strcmp(type_name, "str") == 0 || strcmp(type_name, "bool") == 0)
		{
			JsonbValue	key,
						val,
						v_stat_key;
			JsonbIterator *counts_it;
			JsonbValue	counts_k,
						counts_v;
			bool		inserted = false;
			Jsonb	   *counts_jsonb;

			pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
			if (v.type == jbvBool)
			{
				v_stat_key.type = jbvString;
				v_stat_key.val.string.val = v.val.boolean ? "true" : "false";
				v_stat_key.val.string.len = v.val.boolean ? 4 : 5;
			}
			else
			{
				v_stat_key = v;
			}

			key.type = jbvString;
			key.val.string.val = "counts"; key.val.string.len = 6;
			pushJsonbValue(&ps_new, WJB_KEY, &key);
			pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);

			key_finder.val.string.val = "counts";
			key_finder.val.string.len = 6;
			v_state_val_ptr = findJsonbValueFromContainer(&state_jb->root, JB_FOBJECT, &key_finder);

			counts_jsonb = JsonbValueToJsonb(v_state_val_ptr);
			counts_it = JsonbIteratorInit(&counts_jsonb->root);
			(void) JsonbIteratorNext(&counts_it, &counts_k, false); /* consume { */
			while (JsonbIteratorNext(&counts_it, &counts_k, false) == WJB_KEY)
			{
				int			cmp = compare_jsonb_string_values(&counts_k, &v_stat_key);

				if (!inserted && cmp > 0)
				{
					pushJsonbValue(&ps_new, WJB_KEY, &v_stat_key);
					val.type = jbvNumeric;
					val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));
					pushJsonbValue(&ps_new, WJB_VALUE, &val);
					inserted = true;
				}
				(void) JsonbIteratorNext(&counts_it, &counts_v, true);
				if (cmp == 0)
				{
					Datum		new_count = DirectFunctionCall2(numeric_add,
															  NumericGetDatum(counts_v.val.numeric),
															  DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));

					pushJsonbValue(&ps_new, WJB_KEY, &counts_k);
					val.type = jbvNumeric;
					val.val.numeric = DatumGetNumeric(new_count);
					pushJsonbValue(&ps_new, WJB_VALUE, &val);
					inserted = true;
				}
				else
				{
					pushJsonbValue(&ps_new, WJB_KEY, &counts_k);
					pushJsonbValue(&ps_new, WJB_VALUE, &counts_v);
				}
			}
			if (!inserted)
			{
				pushJsonbValue(&ps_new, WJB_KEY, &v_stat_key);
				val.type = jbvNumeric;
				val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));
				pushJsonbValue(&ps_new, WJB_VALUE, &val);
			}
			pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL);

			key.val.string.val = "type"; key.val.string.len = 4;
			pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvString;
			if (strcmp(state_type_name, "str_agg") == 0)
				val.val.string.val = "str_agg";
			else
				val.val.string.val = "bool_agg";
			val.val.string.len = strlen(val.val.string.val);
			pushJsonbValue(&ps_new, WJB_VALUE, &val);
		}
		else /* array_summary */
		{
			Datum		count;
			JsonbValue	key,
						val;
			int			new_elements_count = 0;
			char	  **new_elements = NULL;

			pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
			key_finder.val.string.val = "count";
			key_finder.val.string.len = 5;
			v_state_val_ptr = findJsonbValueFromContainer(&state_jb->root, JB_FOBJECT, &key_finder);
			count = NumericGetDatum(v_state_val_ptr->val.numeric);
			count = DirectFunctionCall2(numeric_add, count, DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));

			/* Parse new elements from the jsonb array */
			if (v.type == jbvBinary)
			{
				Jsonb	   *jb_v = JsonbValueToJsonb(&v);
				JsonbIterator *arr_it;

				if (JB_ROOT_IS_ARRAY(&jb_v->root))
				{
					JsonbValue	elem;
					uint32		r_arr;
					int			new_elements_capacity = 8;

					new_elements = palloc(sizeof(char *) * new_elements_capacity);
					arr_it = JsonbIteratorInit(&jb_v->root);

					(void) JsonbIteratorNext(&arr_it, &elem, true); /* consume [ */
					while ((r_arr = JsonbIteratorNext(&arr_it, &elem, true)) != WJB_END_ARRAY)
					{
						char	   *elem_str;

						switch (elem.type)
						{
							case jbvString:
								elem_str = pnstrdup(elem.val.string.val, elem.val.string.len);
								break;
							case jbvNumeric:
								elem_str = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(elem.val.numeric)));
								break;
							case jbvBool:
								elem_str = elem.val.boolean ? "true" : "false";
								break;
							case jbvNull:
								elem_str = "null";
								break;
							default:
								continue;
						}

						if (new_elements_count >= new_elements_capacity)
						{
							new_elements_capacity *= 2;
							new_elements = repalloc(new_elements, sizeof(char *) * new_elements_capacity);
						}
						new_elements[new_elements_count++] = elem_str;
					}
				}
			}

			/* Keys must be in alphabetical order for jsonb */
			key.type = jbvString;
			key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(count); pushJsonbValue(&ps_new, WJB_VALUE, &val);

			key.val.string.val = "counts";
			key.val.string.len = 6;
			pushJsonbValue(&ps_new, WJB_KEY, &key);

			/* Begin merging old and new counts */
			pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
			{
				JsonbParseState *new_counts_ps = NULL;
				JsonbValue *v_new_counts_jsonb;
				JsonbIterator *it_old_counts, *it_new_counts;
				JsonbValue	k_old, v_old, k_new, v_new;
				uint32      r_old, r_new;
				Jsonb	   *old_counts_jsonb, *new_counts_jsonb;

				qsort(new_elements, new_elements_count, sizeof(char *), qsort_strcmp);

				/* 2. Build a temporary counts object for the new elements */
				pushJsonbValue(&new_counts_ps, WJB_BEGIN_OBJECT, NULL);
				if (new_elements_count > 0)
				{
					char	   *current_element = new_elements[0];
					int			current_count = 1;

					for (int i = 1; i < new_elements_count; i++)
					{
						if (strcmp(current_element, new_elements[i]) == 0)
						{
							current_count++;
						}
						else
						{
							key.val.string.val = current_element; key.val.string.len = strlen(current_element); pushJsonbValue(&new_counts_ps, WJB_KEY, &key);
							val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(current_count))); pushJsonbValue(&new_counts_ps, WJB_VALUE, &val);
							current_element = new_elements[i];
							current_count = 1;
						}
					}
					key.val.string.val = current_element; key.val.string.len = strlen(current_element); pushJsonbValue(&new_counts_ps, WJB_KEY, &key);
					val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(current_count))); pushJsonbValue(&new_counts_ps, WJB_VALUE, &val);
				}
				v_new_counts_jsonb = pushJsonbValue(&new_counts_ps, WJB_END_OBJECT, NULL);

				/* 3. Merge old and new counts */
				key_finder.val.string.val = "counts"; key_finder.val.string.len = 6;
				v_state_val_ptr = findJsonbValueFromContainer(&state_jb->root, JB_FOBJECT, &key_finder);
				old_counts_jsonb = JsonbValueToJsonb(v_state_val_ptr);
				new_counts_jsonb = JsonbValueToJsonb(v_new_counts_jsonb);

				it_old_counts = JsonbIteratorInit(&old_counts_jsonb->root);
				it_new_counts = JsonbIteratorInit(&new_counts_jsonb->root);
				(void) JsonbIteratorNext(&it_old_counts, &k_old, false);
				(void) JsonbIteratorNext(&it_new_counts, &k_new, false);
				r_old = JsonbIteratorNext(&it_old_counts, &k_old, false);
				r_new = JsonbIteratorNext(&it_new_counts, &k_new, false);

				while(r_old == WJB_KEY && r_new == WJB_KEY)
				{
					int cmp = compare_jsonb_string_values(&k_old, &k_new);
					if (cmp < 0)
					{
						pushJsonbValue(&ps_new, WJB_KEY, &k_old);
						(void) JsonbIteratorNext(&it_old_counts, &v_old, true);
						pushJsonbValue(&ps_new, WJB_VALUE, &v_old);
						r_old = JsonbIteratorNext(&it_old_counts, &k_old, false);
					}
					else if (cmp > 0)
					{
						pushJsonbValue(&ps_new, WJB_KEY, &k_new);
						(void) JsonbIteratorNext(&it_new_counts, &v_new, true);
						pushJsonbValue(&ps_new, WJB_VALUE, &v_new);
						r_new = JsonbIteratorNext(&it_new_counts, &k_new, false);
					}
					else
					{
						Datum new_count;
						pushJsonbValue(&ps_new, WJB_KEY, &k_old);
						(void) JsonbIteratorNext(&it_old_counts, &v_old, true);
						(void) JsonbIteratorNext(&it_new_counts, &v_new, true);
						new_count = DirectFunctionCall2(numeric_add, NumericGetDatum(v_old.val.numeric), NumericGetDatum(v_new.val.numeric));
						val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_count);
						pushJsonbValue(&ps_new, WJB_VALUE, &val);
						r_old = JsonbIteratorNext(&it_old_counts, &k_old, false);
						r_new = JsonbIteratorNext(&it_new_counts, &k_new, false);
					}
				}
				while (r_old == WJB_KEY)
				{
					pushJsonbValue(&ps_new, WJB_KEY, &k_old);
					(void) JsonbIteratorNext(&it_old_counts, &v_old, true);
					pushJsonbValue(&ps_new, WJB_VALUE, &v_old);
					r_old = JsonbIteratorNext(&it_old_counts, &k_old, false);
				}
				while (r_new == WJB_KEY)
				{
					pushJsonbValue(&ps_new, WJB_KEY, &k_new);
					(void) JsonbIteratorNext(&it_new_counts, &v_new, true);
					pushJsonbValue(&ps_new, WJB_VALUE, &v_new);
					r_new = JsonbIteratorNext(&it_new_counts, &k_new, false);
				}

				if (new_elements)
				{
					for(int i = 0; i < new_elements_count; i++)
						pfree(new_elements[i]);
					pfree(new_elements);
				}
			}
			pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL);

			key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
			val.type = jbvString; val.val.string.val = "arr_agg"; val.val.string.len = 7; pushJsonbValue(&ps_new, WJB_VALUE, &val);
		}

		new_summary_jb = JsonbValueToJsonb(pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL));
		new_v.type = jbvBinary;
		new_v.val.binary.data = &new_summary_jb->root;
		new_v.val.binary.len = VARSIZE_ANY_EXHDR(new_summary_jb);
		pushJsonbValue(parse_state, WJB_VALUE, &new_v);

		pfree(type_name);
		pfree(state_type_name);
		return;
	}

	r = JsonbIteratorNext(it, &v, false);
	if (r != WJB_BEGIN_OBJECT)
		elog(ERROR, "malformed stat object, expected object start");

	(void) JsonbIteratorNext(it, &v_k, false);	/* "type" key */
	(void) JsonbIteratorNext(it, &v, true);
	type_name = pnstrdup(v.val.string.val, v.val.string.len);

	(void) JsonbIteratorNext(&it, &v_k, false);	/* "value" */
	(void) JsonbIteratorNext(&it, &v, true);

	pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);

	if (strcmp(type_name, "int") == 0 || strcmp(type_name, "dec2") == 0 || strcmp(type_name, "float") == 0)
	{
		JsonbValue	key,
					val;
		Datum		new_sum,
					new_count,
					new_mean,
					new_min,
					new_max,
					new_sum_sq_diff;

		if (strcmp(type_name, "dec2") == 0)
		{
			Datum n_100 = DirectFunctionCall1(int4_numeric, Int32GetDatum(100));
			Datum n_val = DirectFunctionCall2(numeric_mul, NumericGetDatum(v.val.numeric), n_100);
			new_sum = DirectFunctionCall2(numeric_round, n_val, Int32GetDatum(0));
		}
		else
		{
			new_sum = NumericGetDatum(v.val.numeric);
		}
		new_count = Int32GetDatum(1);
		new_mean = new_sum;
		new_min = new_sum;
		new_max = new_sum;
		new_sum_sq_diff = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));

		key.type = jbvString;

		/* The order of keys must be alphabetical for jsonb */
		key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, new_count)); pushJsonbValue(&ps_new, WJB_VALUE, &val);
		key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_max); pushJsonbValue(&ps_new, WJB_VALUE, &val);
		key.val.string.val = "mean"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_mean); pushJsonbValue(&ps_new, WJB_VALUE, &val);
		key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_min); pushJsonbValue(&ps_new, WJB_VALUE, &val);
		key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_sum); pushJsonbValue(&ps_new, WJB_VALUE, &val);
		key.val.string.val = "sum_sq_diff"; key.val.string.len = 11; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_sum_sq_diff); pushJsonbValue(&ps_new, WJB_VALUE, &val);
		key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvString;
		if (strcmp(type_name, "int") == 0)
		{
			val.val.string.val = "int_agg"; val.val.string.len = 7;
		}
		else if (strcmp(type_name, "float") == 0)
		{
			val.val.string.val = "float_agg"; val.val.string.len = 9;
		}
		else
		{
			val.val.string.val = "dec2_agg"; val.val.string.len = 8;
		}
		pushJsonbValue(&ps_new, WJB_VALUE, &val);
	}
	else if (strcmp(type_name, "str") == 0 || strcmp(type_name, "bool") == 0)
	{
		JsonbValue	key, val, v_stat_key;

		if (v.type == jbvBool)
		{
			v_stat_key.type = jbvString;
			v_stat_key.val.string.val = v.val.boolean ? "true" : "false";
			v_stat_key.val.string.len = v.val.boolean ? 4 : 5;
		}
		else
		{
			v_stat_key = v;
		}

		key.type = jbvString;
		key.val.string.val = "counts"; key.val.string.len = 6;
		pushJsonbValue(&ps_new, WJB_KEY, &key);
		pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
		pushJsonbValue(&ps_new, WJB_KEY, &v_stat_key);
		val.type = jbvNumeric;
		val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));
		pushJsonbValue(&ps_new, WJB_VALUE, &val);
		pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL);

		if (strcmp(type_name, "str") == 0)
			summary_type_name = "str_agg";
		else
			summary_type_name = "bool_agg";

		key.val.string.val = "type"; key.val.string.len = 4;
		pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvString; val.val.string.val = summary_type_name; val.val.string.len = strlen(summary_type_name);
		pushJsonbValue(&ps_new, WJB_VALUE, &val);
	}
	else
	{
		/* Summary for arrays and other types */
		JsonbValue	key,
					val;
		int			elements_count = 0;

		/* Keys must be in alphabetical order for jsonb */
		key.type = jbvString;
		key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1))); pushJsonbValue(&ps_new, WJB_VALUE, &val);

		key.val.string.val = "counts"; key.val.string.len = 6; pushJsonbValue(&ps_new, WJB_KEY, &key);
		if (v.type == jbvBinary)
		{
			Jsonb	   *jb_v = JsonbValueToJsonb(&v);
			JsonbIterator *arr_it;

			if (JB_ROOT_IS_ARRAY(&jb_v->root))
			{
				JsonbValue	elem;
				uint32		r_arr;
				char	  **elements = NULL;
				int			elements_capacity = 8;

				elements = palloc(sizeof(char *) * elements_capacity);
				arr_it = JsonbIteratorInit(&jb_v->root);

				(void) JsonbIteratorNext(&arr_it, &elem, true); /* consume [ */
				while ((r_arr = JsonbIteratorNext(&arr_it, &elem, true)) != WJB_END_ARRAY)
				{
					char	   *elem_str;

					switch (elem.type)
					{
						case jbvString:
							elem_str = pnstrdup(elem.val.string.val, elem.val.string.len);
							break;
						case jbvNumeric:
							elem_str = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(elem.val.numeric)));
							break;
						case jbvBool:
							elem_str = elem.val.boolean ? "true" : "false";
							break;
						case jbvNull:
							elem_str = "null";
							break;
						default:
							continue;
					}

					if (elements_count >= elements_capacity)
					{
						elements_capacity *= 2;
						elements = repalloc(elements, sizeof(char *) * elements_capacity);
					}
					elements[elements_count++] = elem_str;
				}
				qsort(elements, elements_count, sizeof(char *), qsort_strcmp);

				pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
				if (elements_count > 0)
				{
					char	   *current_element = elements[0];
					int			current_count = 1;

					for (int i = 1; i < elements_count; i++)
					{
						if (strcmp(current_element, elements[i]) == 0)
						{
							current_count++;
						}
						else
						{
							key.val.string.val = current_element; key.val.string.len = strlen(current_element); pushJsonbValue(&ps_new, WJB_KEY, &key);
							val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(current_count))); pushJsonbValue(&ps_new, WJB_VALUE, &val);
							current_element = elements[i];
							current_count = 1;
						}
					}
					key.val.string.val = current_element; key.val.string.len = strlen(current_element); pushJsonbValue(&ps_new, WJB_KEY, &key);
					val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(current_count))); pushJsonbValue(&ps_new, WJB_VALUE, &val);
				}
				pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL);
				for (int i = 0; i < elements_count; i++)
					pfree(elements[i]);
				pfree(elements);
			}
			else
			{
				pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
				pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL);
			}
		}
		else
		{
			pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
			pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL);
		}

		key.type = jbvString;
		key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
		val.type = jbvString; val.val.string.val = "arr_agg"; val.val.string.len = 7; pushJsonbValue(&ps_new, WJB_VALUE, &val);
	}

	new_summary_jb = JsonbValueToJsonb(pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL));
	new_v.type = jbvBinary;
	new_v.val.binary.data = &new_summary_jb->root;
	new_v.val.binary.len = VARSIZE_ANY_EXHDR(new_summary_jb);
	pushJsonbValue(parse_state, WJB_VALUE, &new_v);

	pfree(type_name);

	r = JsonbIteratorNext(it, &v, false);
	if (r != WJB_END_OBJECT)
		elog(ERROR, "malformed stat object, expected object end");
}

PG_FUNCTION_INFO_V1(jsonb_stats_accum);
Datum
jsonb_stats_accum(PG_FUNCTION_ARGS)
{
	Jsonb	   *state = PG_GETARG_JSONB_P(0);
	Jsonb	   *stats = PG_GETARG_JSONB_P(1);
	JsonbParseState *parse_state = NULL;
	JsonbIterator *state_it,
			   *stats_it;
	JsonbValue	state_key,
				state_val,
				stats_key,
				stats_val;
	uint32		r_state,
				r_stats;
	int			cmp;
	MemoryContext agg_context,
				old_context;
	Jsonb	   *res;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "jsonb_stats_accum called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	if (!JB_ROOT_IS_OBJECT(state) || !JB_ROOT_IS_OBJECT(stats))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("state and stats must be jsonb objects")));

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	state_it = JsonbIteratorInit(&state->root);
	stats_it = JsonbIteratorInit(&stats->root);

	r_state = JsonbIteratorNext(&state_it, &state_key, false);	/* consume WJB_BEGIN_OBJECT */
	r_stats = JsonbIteratorNext(&stats_it, &stats_key, false);	/* consume WJB_BEGIN_OBJECT */

	r_state = JsonbIteratorNext(&state_it, &state_key, false);
	r_stats = JsonbIteratorNext(&stats_it, &stats_key, false);

	while (r_state == WJB_KEY || r_stats == WJB_KEY)
	{
		if (r_stats == WJB_KEY && stats_key.type == jbvString && stats_key.val.string.len == 4 && strncmp(stats_key.val.string.val, "type", 4) == 0)
		{
			(void) JsonbIteratorNext(&stats_it, &stats_val, true);
			r_stats = JsonbIteratorNext(&stats_it, &stats_key, false);
			continue;
		}

		if (r_state == WJB_KEY && state_key.type == jbvString && state_key.val.string.len == 4 && strncmp(state_key.val.string.val, "type", 4) == 0)
		{
			(void) JsonbIteratorNext(&state_it, &state_val, true);
			r_state = JsonbIteratorNext(&state_it, &state_key, false);
			continue;
		}

		if (r_state != WJB_KEY)
			cmp = 1;
		else if (r_stats != WJB_KEY)
			cmp = -1;
		else
			cmp = compare_jsonb_string_values(&state_key, &stats_key);

		if (cmp < 0)
		{
			/* Key is only in state, copy it over */
			pushJsonbValue(&parse_state, WJB_KEY, &state_key);
			(void) JsonbIteratorNext(&state_it, &state_val, true);
			if (state_val.type == jbvBinary)
			{
				Jsonb	   *jb_val = palloc(VARHDRSZ + state_val.val.binary.len);

				SET_VARSIZE(jb_val, VARHDRSZ + state_val.val.binary.len);
				memcpy(&jb_val->root, state_val.val.binary.data, state_val.val.binary.len);
				state_val.val.binary.data = &jb_val->root;
			}
			pushJsonbValue(&parse_state, WJB_VALUE, &state_val);
			r_state = JsonbIteratorNext(&state_it, &state_key, false);
		}
		else if (cmp > 0)
		{
			/* Key is only in stats, initialize a new summary */
			pushJsonbValue(&parse_state, WJB_KEY, &stats_key);
			push_summary(&parse_state, NULL, &stats_it, true);
			r_stats = JsonbIteratorNext(&stats_it, &stats_key, false);
		}
		else
		{
			/* Key is in both, update the summary */
			pushJsonbValue(&parse_state, WJB_KEY, &state_key);
			(void) JsonbIteratorNext(&state_it, &state_val, true);
			push_summary(&parse_state, &state_val, &stats_it, false);
			r_state = JsonbIteratorNext(&state_it, &state_key, false);
			r_stats = JsonbIteratorNext(&stats_it, &stats_key, false);
		}
	}

	res = JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL));

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
				v,
			   *v_type_ptr;
	Jsonb	   *summary_jb;
	JsonbValue	key_finder;
	bool		type_inserted = false;
	JsonbValue	type_key,
				type_val;

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

	key_finder.type = jbvString;
	key_finder.val.string.val = "type";
	key_finder.val.string.len = 4;

	while (JsonbIteratorNext(&it, &k, false) == WJB_KEY)
	{
		if (!type_inserted && compare_jsonb_string_values(&k, &type_key) > 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &type_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &type_val);
			type_inserted = true;
		}
		(void) JsonbIteratorNext(&it, &v, true);
		summary_jb = JsonbValueToJsonb(&v);
		v_type_ptr = findJsonbValueFromContainer(&summary_jb->root, JB_FOBJECT, &key_finder);
		if (v_type_ptr && v_type_ptr->type == jbvString)
		{
			char	   *type_name = pnstrdup(v_type_ptr->val.string.val, v_type_ptr->val.string.len);

			if (strcmp(type_name, "int_agg") == 0 || strcmp(type_name, "float_agg") == 0)
			{
				JsonbValue *count_ptr, *max_ptr, *mean_ptr, *min_ptr, *sum_ptr, *sum_sq_diff_ptr;
				Datum count_val, mean_val, sum_sq_diff_val, variance = (Datum) 0, stddev = (Datum) 0, cv_pct = (Datum) 0;
				JsonbParseState *ps_new = NULL;
				JsonbValue	new_v, key, val;
				Jsonb *new_summary_jb;

#define FIND_SUMMARY_VAL(keyname, var) \
    key_finder.val.string.val = keyname; \
    key_finder.val.string.len = strlen(keyname); \
    var = findJsonbValueFromContainer(&summary_jb->root, JB_FOBJECT, &key_finder);

				FIND_SUMMARY_VAL("count", count_ptr);
				FIND_SUMMARY_VAL("max", max_ptr);
				FIND_SUMMARY_VAL("mean", mean_ptr);
				FIND_SUMMARY_VAL("min", min_ptr);
				FIND_SUMMARY_VAL("sum", sum_ptr);
				FIND_SUMMARY_VAL("sum_sq_diff", sum_sq_diff_ptr);

				count_val = NumericGetDatum(count_ptr->val.numeric);
				mean_val = NumericGetDatum(mean_ptr->val.numeric);
				sum_sq_diff_val = NumericGetDatum(sum_sq_diff_ptr->val.numeric);

				if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, count_val, DirectFunctionCall1(int4_numeric, Int32GetDatum(1)))) > 0)
				{
					variance = DirectFunctionCall2(numeric_div, sum_sq_diff_val, DirectFunctionCall2(numeric_sub, count_val, DirectFunctionCall1(int4_numeric, Int32GetDatum(1))));
					if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, variance, DirectFunctionCall1(int4_numeric, Int32GetDatum(0)))) >= 0)
						stddev = DirectFunctionCall1(numeric_sqrt, variance);
				}

				if (stddev != (Datum) 0 && DatumGetInt32(DirectFunctionCall2(numeric_cmp, mean_val, DirectFunctionCall1(int4_numeric, Int32GetDatum(0)))) != 0)
				{
					Datum cv = DirectFunctionCall2(numeric_div, stddev, mean_val);
					cv_pct = DirectFunctionCall2(numeric_mul, cv, DirectFunctionCall1(int4_numeric, Int32GetDatum(100)));
				}

				pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
				key.type = jbvString;

#define PUSH_NUMERIC_VAL(keyname, jsonb_val_ptr) \
    key.val.string.val = keyname; key.val.string.len = strlen(keyname); pushJsonbValue(&ps_new, WJB_KEY, &key); \
    val.type = jbvNumeric; val.val.numeric = jsonb_val_ptr->val.numeric; \
    pushJsonbValue(&ps_new, WJB_VALUE, &val);

#define PUSH_ROUNDED_DATUM(keyname, datum_val, precision) \
    key.val.string.val = keyname; key.val.string.len = strlen(keyname); pushJsonbValue(&ps_new, WJB_KEY, &key); \
    if (datum_val != (Datum) 0) { \
        val.type = jbvNumeric; \
        val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_round, datum_val, Int32GetDatum(precision))); \
    } else { \
        val.type = jbvNull; \
    } \
    pushJsonbValue(&ps_new, WJB_VALUE, &val);

				PUSH_ROUNDED_DATUM("coefficient_of_variation_pct", cv_pct, 2);
				PUSH_NUMERIC_VAL("count", count_ptr);
				PUSH_NUMERIC_VAL("max", max_ptr);
				PUSH_ROUNDED_DATUM("mean", mean_val, 2);
				PUSH_NUMERIC_VAL("min", min_ptr);
				PUSH_ROUNDED_DATUM("stddev", stddev, 2);
				PUSH_NUMERIC_VAL("sum", sum_ptr);
				PUSH_ROUNDED_DATUM("sum_sq_diff", sum_sq_diff_val, 2);
				key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
				val.type = jbvString;
				if (strcmp(type_name, "int_agg") == 0)
				{
					val.val.string.val = "int_agg"; val.val.string.len = 7;
				}
				else
				{
					val.val.string.val = "float_agg"; val.val.string.len = 9;
				}
				pushJsonbValue(&ps_new, WJB_VALUE, &val);
				PUSH_ROUNDED_DATUM("variance", variance, 2);

				new_summary_jb = JsonbValueToJsonb(pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL));
				new_v.type = jbvBinary;
				new_v.val.binary.data = &new_summary_jb->root;
				new_v.val.binary.len = VARSIZE_ANY_EXHDR(new_summary_jb);
				pushJsonbValue(&parse_state, WJB_KEY, &k);
				pushJsonbValue(&parse_state, WJB_VALUE, &new_v);
			}
			else if (strcmp(type_name, "dec2_agg") == 0)
			{
				JsonbValue *count_ptr, *max_ptr, *mean_ptr, *min_ptr, *sum_ptr, *sum_sq_diff_ptr;
				Datum count_val, mean_val, sum_sq_diff_val, variance = (Datum) 0, stddev = (Datum) 0, cv_pct = (Datum) 0;
				Datum n_100 = DirectFunctionCall1(int4_numeric, Int32GetDatum(100));
				JsonbParseState *ps_new = NULL;
				JsonbValue	new_v, key, val;
				Jsonb *new_summary_jb;

				FIND_SUMMARY_VAL("count", count_ptr);
				FIND_SUMMARY_VAL("max", max_ptr);
				FIND_SUMMARY_VAL("mean", mean_ptr);
				FIND_SUMMARY_VAL("min", min_ptr);
				FIND_SUMMARY_VAL("sum", sum_ptr);
				FIND_SUMMARY_VAL("sum_sq_diff", sum_sq_diff_ptr);

				count_val = NumericGetDatum(count_ptr->val.numeric);
				mean_val = DirectFunctionCall2(numeric_div, NumericGetDatum(mean_ptr->val.numeric), n_100);
				sum_sq_diff_val = DirectFunctionCall2(numeric_div, DirectFunctionCall2(numeric_div, NumericGetDatum(sum_sq_diff_ptr->val.numeric), n_100), n_100);

				if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, count_val, DirectFunctionCall1(int4_numeric, Int32GetDatum(1)))) > 0)
				{
					variance = DirectFunctionCall2(numeric_div, sum_sq_diff_val, DirectFunctionCall2(numeric_sub, count_val, DirectFunctionCall1(int4_numeric, Int32GetDatum(1))));
					if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, variance, DirectFunctionCall1(int4_numeric, Int32GetDatum(0)))) >= 0)
						stddev = DirectFunctionCall1(numeric_sqrt, variance);
				}

				if (stddev != (Datum) 0 && DatumGetInt32(DirectFunctionCall2(numeric_cmp, mean_val, DirectFunctionCall1(int4_numeric, Int32GetDatum(0)))) != 0)
				{
					Datum cv = DirectFunctionCall2(numeric_div, stddev, mean_val);
					cv_pct = DirectFunctionCall2(numeric_mul, cv, DirectFunctionCall1(int4_numeric, Int32GetDatum(100)));
				}

				pushJsonbValue(&ps_new, WJB_BEGIN_OBJECT, NULL);
				key.type = jbvString;

				PUSH_ROUNDED_DATUM("coefficient_of_variation_pct", cv_pct, 2);
				PUSH_NUMERIC_VAL("count", count_ptr);

				key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
				val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_div, NumericGetDatum(max_ptr->val.numeric), n_100));
				pushJsonbValue(&ps_new, WJB_VALUE, &val);

				PUSH_ROUNDED_DATUM("mean", mean_val, 2);

				key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
				val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_div, NumericGetDatum(min_ptr->val.numeric), n_100));
				pushJsonbValue(&ps_new, WJB_VALUE, &val);

				PUSH_ROUNDED_DATUM("stddev", stddev, 2);

				key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(&ps_new, WJB_KEY, &key);
				val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_div, NumericGetDatum(sum_ptr->val.numeric), n_100));
				pushJsonbValue(&ps_new, WJB_VALUE, &val);

				PUSH_ROUNDED_DATUM("sum_sq_diff", sum_sq_diff_val, 2);
				key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(&ps_new, WJB_KEY, &key);
				val.type = jbvString; val.val.string.val = "dec2_agg"; val.val.string.len = 8; pushJsonbValue(&ps_new, WJB_VALUE, &val);
				PUSH_ROUNDED_DATUM("variance", variance, 2);

				new_summary_jb = JsonbValueToJsonb(pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL));
				new_v.type = jbvBinary;
				new_v.val.binary.data = &new_summary_jb->root;
				new_v.val.binary.len = VARSIZE_ANY_EXHDR(new_summary_jb);
				pushJsonbValue(&parse_state, WJB_KEY, &k);
				pushJsonbValue(&parse_state, WJB_VALUE, &new_v);
			}
			else
			{
				pushJsonbValue(&parse_state, WJB_KEY, &k);
				pushJsonbValue(&parse_state, WJB_VALUE, &v);
			}
			pfree(type_name);
		}
		else
		{
			pushJsonbValue(&parse_state, WJB_KEY, &k);
			pushJsonbValue(&parse_state, WJB_VALUE, &v);
		}
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
	Jsonb	   *a = PG_GETARG_JSONB_P(0);
	Jsonb	   *b = PG_GETARG_JSONB_P(1);
	JsonbParseState *parse_state = NULL;
	JsonbIterator *it_a,
			   *it_b;
	JsonbValue	k_a,
				v_a,
				k_b,
				v_b;
	uint32		r_a,
				r_b;
	int			cmp;
	MemoryContext agg_context,
				old_context;
	Jsonb	   *res;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "jsonb_stats_merge called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	if (!JB_ROOT_IS_OBJECT(a))
		PG_RETURN_JSONB_P(b);
	if (!JB_ROOT_IS_OBJECT(b))
		PG_RETURN_JSONB_P(a);

	it_a = JsonbIteratorInit(&a->root);
	it_b = JsonbIteratorInit(&b->root);

	r_a = JsonbIteratorNext(&it_a, &k_a, false);
	r_b = JsonbIteratorNext(&it_b, &k_b, false);

	if (r_a != WJB_BEGIN_OBJECT || r_b != WJB_BEGIN_OBJECT)
		elog(ERROR, "expected two objects");

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	r_a = JsonbIteratorNext(&it_a, &k_a, false);
	r_b = JsonbIteratorNext(&it_b, &k_b, false);

	while (r_a == WJB_KEY || r_b == WJB_KEY)
	{
		if (r_a == WJB_KEY && k_a.type == jbvString && k_a.val.string.len == 4 && strncmp(k_a.val.string.val, "type", 4) == 0)
		{
			(void) JsonbIteratorNext(&it_a, &v_a, true);
			r_a = JsonbIteratorNext(&it_a, &k_a, false);
			continue;
		}
		if (r_b == WJB_KEY && k_b.type == jbvString && k_b.val.string.len == 4 && strncmp(k_b.val.string.val, "type", 4) == 0)
		{
			(void) JsonbIteratorNext(&it_b, &v_b, true);
			r_b = JsonbIteratorNext(&it_b, &k_b, false);
			continue;
		}

		if (r_a != WJB_KEY)
			cmp = 1;
		else if (r_b != WJB_KEY)
			cmp = -1;
		else
			cmp = compare_jsonb_string_values(&k_a, &k_b);


		if (cmp < 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &k_a);
			(void) JsonbIteratorNext(&it_a, &v_a, true);
			if (v_a.type == jbvBinary)
			{
				Jsonb	   *jb_val = palloc(VARHDRSZ + v_a.val.binary.len);

				SET_VARSIZE(jb_val, VARHDRSZ + v_a.val.binary.len);
				memcpy(&jb_val->root, v_a.val.binary.data, v_a.val.binary.len);
				v_a.val.binary.data = &jb_val->root;
			}
			pushJsonbValue(&parse_state, WJB_VALUE, &v_a);
			r_a = JsonbIteratorNext(&it_a, &k_a, false);
		}
		else if (cmp > 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &k_b);
			(void) JsonbIteratorNext(&it_b, &v_b, true);
			if (v_b.type == jbvBinary)
			{
				Jsonb	   *jb_val = palloc(VARHDRSZ + v_b.val.binary.len);

				SET_VARSIZE(jb_val, VARHDRSZ + v_b.val.binary.len);
				memcpy(&jb_val->root, v_b.val.binary.data, v_b.val.binary.len);
				v_b.val.binary.data = &jb_val->root;
			}
			pushJsonbValue(&parse_state, WJB_VALUE, &v_b);
			r_b = JsonbIteratorNext(&it_b, &k_b, false);
		}
		else
		{
			pushJsonbValue(&parse_state, WJB_KEY, &k_a);
			(void) JsonbIteratorNext(&it_a, &v_a, true);
			(void) JsonbIteratorNext(&it_b, &v_b, true);
			merge_summaries(&parse_state, &v_a, &v_b);
			r_a = JsonbIteratorNext(&it_a, &k_a, false);
			r_b = JsonbIteratorNext(&it_b, &k_b, false);
		}
	}

	res = JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL));

	MemoryContextSwitchTo(old_context);

	PG_RETURN_JSONB_P(res);
}

