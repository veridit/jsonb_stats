#include "postgres.h"
#include "fmgr.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC;

// Forward declarations for internal jsonb functions
JsonbIterator *JsonbIteratorInit(JsonbContainer *container);
uint32 JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested);

// Function prototypes
Datum stat(PG_FUNCTION_ARGS);
static int	compare_jsonb_string_values(const JsonbValue *a, const JsonbValue *b);
Datum jsonb_stats_sfunc(PG_FUNCTION_ARGS);
static void push_summary(JsonbParseState **parse_state, JsonbValue *state_val, JsonbIterator **it, bool init);
Datum jsonb_stats_summary_accum(PG_FUNCTION_ARGS);
Datum jsonb_stats_to_summary_round(PG_FUNCTION_ARGS);
static void merge_summaries(JsonbParseState **ps, JsonbValue *v_a, JsonbValue *v_b);
Datum jsonb_stats_summary_merge(PG_FUNCTION_ARGS);


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
	pairs[0].value.val.string.val = format_type_be(value_type);
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
		default:
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
				break;
			}
	}

	v_object.type = jbvObject;
	v_object.val.object.nPairs = 2;
	v_object.val.object.pairs = pairs;

	res = JsonbValueToJsonb(&v_object);
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
				new_val;
	bool		inserted = false;
	Jsonb	   *res;

	if (!JB_ROOT_IS_OBJECT(state))
		elog(ERROR, "jsonb_stats_sfunc state must be a jsonb object");

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	it = JsonbIteratorInit(&state->root);

	new_key.type = jbvString;
	new_key.val.string.val = code;
	new_key.val.string.len = strlen(code);
	new_val.type = jbvBinary;
	new_val.val.binary.data = &stat_val->root;
	new_val.val.binary.len = VARSIZE_ANY_EXHDR(stat_val);

	(void) JsonbIteratorNext(&it, &v_key, false); /* consume WJB_BEGIN_OBJECT */

	while (JsonbIteratorNext(&it, &v_key, false) == WJB_KEY)
	{
		if (!inserted && compare_jsonb_string_values(&new_key, &v_key) < 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &new_key);
			pushJsonbValue(&parse_state, WJB_VALUE, &new_val);
			inserted = true;
		}

		pushJsonbValue(&parse_state, WJB_KEY, &v_key);
		(void) JsonbIteratorNext(&it, &v_val, true);
		pushJsonbValue(&parse_state, WJB_VALUE, &v_val);
	}

	if (!inserted)
	{
		pushJsonbValue(&parse_state, WJB_KEY, &new_key);
		pushJsonbValue(&parse_state, WJB_VALUE, &new_val);
	}

	res = JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL));

	PG_RETURN_JSONB_P(res);
}

static void
push_summary(JsonbParseState **parse_state, JsonbValue *state_val, JsonbIterator **it, bool init)
{
	JsonbValue	v_k,
				v;
	char	   *type_name;
	uint32		r;

	/* For update, just copy the old state for now */
	if (!init)
	{
		JsonbValue *v_state_type_ptr,
				   *v_state_val_ptr,
					key_finder;
		char	   *state_type_name;

		(void) JsonbIteratorNext(it, &v, false);	/* consume stat WJB_BEGIN_OBJECT */
		(void) JsonbIteratorNext(it, &v_k, false);	/* stat "type" key */
		(void) JsonbIteratorNext(it, &v, true); /* stat "type" val */
		type_name = pnstrdup(v.val.string.val, v.val.string.len);
		(void) JsonbIteratorNext(it, &v_k, false);	/* stat "value" key */
		(void) JsonbIteratorNext(it, &v, true); /* stat "value" val */
		(void) JsonbIteratorNext(it, &v_k, false);	/* consume stat WJB_END_OBJECT */

		key_finder.type = jbvString;
		key_finder.val.string.val = "type";
		key_finder.val.string.len = 4;
		v_state_type_ptr = findJsonbValueFromContainer(state_val->val.binary.data, JB_FOBJECT, &key_finder);
		if (v_state_type_ptr == NULL)
			elog(ERROR, "summary object is missing 'type' key");

		state_type_name = pnstrdup(v_state_type_ptr->val.string.val, v_state_type_ptr->val.string.len);

		if (strcmp(type_name, "integer") != 0 && strcmp(type_name, "text") != 0 && strcmp(type_name, "boolean") != 0)
		{
			if (strcmp(state_type_name, "array_summary") != 0)
				elog(ERROR, "type mismatch in summary update");
		}
		else if (strcmp(type_name, "integer") == 0)
		{
			if (strcmp(state_type_name, "integer_summary") != 0)
				elog(ERROR, "type mismatch in summary update");
		}
		else
		{
			if (strcmp(psprintf("%s_summary", type_name), state_type_name) != 0)
				elog(ERROR, "type mismatch in summary update");
		}


		if (strcmp(type_name, "integer") == 0)
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

			pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);

			new_val_numeric = NumericGetDatum(v.val.numeric);

#define FIND_NUMERIC(key_name, var) \
	key_finder.val.string.val = key_name; \
	key_finder.val.string.len = strlen(key_name); \
	v_state_val_ptr = findJsonbValueFromContainer(state_val->val.binary.data, JB_FOBJECT, &key_finder); \
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
			key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(count); pushJsonbValue(parse_state, WJB_VALUE, &val);
			key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(max); pushJsonbValue(parse_state, WJB_VALUE, &val);
			key.val.string.val = "mean"; key.val.string.len = 4; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(mean); pushJsonbValue(parse_state, WJB_VALUE, &val);
			key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(min); pushJsonbValue(parse_state, WJB_VALUE, &val);
			key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(sum); pushJsonbValue(parse_state, WJB_VALUE, &val);
			key.val.string.val = "sum_sq_diff"; key.val.string.len = 11; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(sum_sq_diff); pushJsonbValue(parse_state, WJB_VALUE, &val);
			key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvString; val.val.string.val = "integer_summary"; val.val.string.len = 15; pushJsonbValue(parse_state, WJB_VALUE, &val);
			pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
		}
		else if (strcmp(state_type_name, "text_summary") == 0 || strcmp(state_type_name, "boolean_summary") == 0)
		{
			JsonbValue	key,
						val,
						v_stat_key;
			JsonbIterator *counts_it;
			JsonbValue	counts_k,
						counts_v;
			bool		inserted = false;
			Jsonb	   *counts_jsonb;

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

			pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);

			key.type = jbvString;
			key.val.string.val = "counts"; key.val.string.len = 6;
			pushJsonbValue(parse_state, WJB_KEY, &key);
			pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);

			key_finder.val.string.val = "counts";
			key_finder.val.string.len = 6;
			v_state_val_ptr = findJsonbValueFromContainer(state_val->val.binary.data, JB_FOBJECT, &key_finder);

			counts_jsonb = JsonbValueToJsonb(v_state_val_ptr);
			counts_it = JsonbIteratorInit(&counts_jsonb->root);
			(void) JsonbIteratorNext(&counts_it, &counts_k, false); /* consume { */
			while (JsonbIteratorNext(&counts_it, &counts_k, false) == WJB_KEY)
			{
				int			cmp = compare_jsonb_string_values(&counts_k, &v_stat_key);

				if (!inserted && cmp > 0)
				{
					pushJsonbValue(parse_state, WJB_KEY, &v_stat_key);
					val.type = jbvNumeric;
					val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));
					pushJsonbValue(parse_state, WJB_VALUE, &val);
					inserted = true;
				}
				(void) JsonbIteratorNext(&counts_it, &counts_v, true);
				if (cmp == 0)
				{
					Datum		new_count = DirectFunctionCall2(numeric_add,
															  NumericGetDatum(counts_v.val.numeric),
															  DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));

					pushJsonbValue(parse_state, WJB_KEY, &counts_k);
					val.type = jbvNumeric;
					val.val.numeric = DatumGetNumeric(new_count);
					pushJsonbValue(parse_state, WJB_VALUE, &val);
					inserted = true;
				}
				else
				{
					pushJsonbValue(parse_state, WJB_KEY, &counts_k);
					pushJsonbValue(parse_state, WJB_VALUE, &counts_v);
				}
			}
			if (!inserted)
			{
				pushJsonbValue(parse_state, WJB_KEY, &v_stat_key);
				val.type = jbvNumeric;
				val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));
				pushJsonbValue(parse_state, WJB_VALUE, &val);
			}
			pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);

			key.val.string.val = "type"; key.val.string.len = 4;
			pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvString; val.val.string.val = psprintf("%s", state_type_name); val.val.string.len = strlen(val.val.string.val);
			pushJsonbValue(parse_state, WJB_VALUE, &val);
			pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
		}
		else /* array_summary */
		{
			Datum		count,
						elements_count_old,
						elements_count_new;
			JsonbValue	key,
						val;
			int			new_elements_count = 0;
			int			type_name_len;
			char	  **new_elements = NULL;

			pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);

			key_finder.val.string.val = "count";
			key_finder.val.string.len = 5;
			v_state_val_ptr = findJsonbValueFromContainer(state_val->val.binary.data, JB_FOBJECT, &key_finder);
			count = NumericGetDatum(v_state_val_ptr->val.numeric);
			count = DirectFunctionCall2(numeric_add, count, DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));

			/* Parse new elements to get their count */
			type_name_len = strlen(type_name);
			if (type_name_len > 2 && strcmp(type_name + type_name_len - 2, "[]") == 0)
			{
				if (v.val.string.len > 2) /* Not an empty array "{}" */
				{
					char *arr_str = pnstrdup(v.val.string.val + 1, v.val.string.len - 2);
					char *p = arr_str;
					char *element_start = p;
					bool in_quotes = false;
					int new_elements_capacity = 8;
					new_elements = palloc(sizeof(char *) * new_elements_capacity);

					for (p = arr_str; *p; p++)
					{
						if (*p == '"')
							in_quotes = !in_quotes;
						else if (*p == ',' && !in_quotes)
						{
							*p = '\0';
							if (new_elements_count >= new_elements_capacity)
							{
								new_elements_capacity *= 2;
								new_elements = repalloc(new_elements, sizeof(char *) * new_elements_capacity);
							}
							new_elements[new_elements_count++] = element_start;
							element_start = p + 1;
						}
					}
					if (new_elements_count >= new_elements_capacity)
					{
						new_elements_capacity++;
						new_elements = repalloc(new_elements, sizeof(char *) * new_elements_capacity);
					}
					new_elements[new_elements_count++] = element_start;

					for (int i = 0; i < new_elements_count; i++)
					{
						int len = strlen(new_elements[i]);
						if (len >= 2 && new_elements[i][0] == '"' && new_elements[i][len - 1] == '"')
						{
							memmove(new_elements[i], new_elements[i] + 1, len - 2);
							new_elements[i][len - 2] = '\0';
						}
						new_elements[i] = pstrdup(new_elements[i]);
					}
					pfree(arr_str);
				}
			}

			key_finder.val.string.val = "elements_count";
			key_finder.val.string.len = 14;
			v_state_val_ptr = findJsonbValueFromContainer(state_val->val.binary.data, JB_FOBJECT, &key_finder);
			elements_count_old = NumericGetDatum(v_state_val_ptr->val.numeric);
			elements_count_new = DirectFunctionCall2(numeric_add, elements_count_old, DirectFunctionCall1(int4_numeric, Int32GetDatum(new_elements_count)));

			/* Keys must be in alphabetical order for jsonb */
			key.type = jbvString;
			key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(count); pushJsonbValue(parse_state, WJB_VALUE, &val);

			key.val.string.val = "counts";
			key.val.string.len = 6;
			pushJsonbValue(parse_state, WJB_KEY, &key);

			/* Begin merging old and new counts */
			pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);
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
				v_state_val_ptr = findJsonbValueFromContainer(state_val->val.binary.data, JB_FOBJECT, &key_finder);
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
						pushJsonbValue(parse_state, WJB_KEY, &k_old);
						(void) JsonbIteratorNext(&it_old_counts, &v_old, true);
						pushJsonbValue(parse_state, WJB_VALUE, &v_old);
						r_old = JsonbIteratorNext(&it_old_counts, &k_old, false);
					}
					else if (cmp > 0)
					{
						pushJsonbValue(parse_state, WJB_KEY, &k_new);
						(void) JsonbIteratorNext(&it_new_counts, &v_new, true);
						pushJsonbValue(parse_state, WJB_VALUE, &v_new);
						r_new = JsonbIteratorNext(&it_new_counts, &k_new, false);
					}
					else
					{
						Datum new_count;
						pushJsonbValue(parse_state, WJB_KEY, &k_old);
						(void) JsonbIteratorNext(&it_old_counts, &v_old, true);
						(void) JsonbIteratorNext(&it_new_counts, &v_new, true);
						new_count = DirectFunctionCall2(numeric_add, NumericGetDatum(v_old.val.numeric), NumericGetDatum(v_new.val.numeric));
						val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_count);
						pushJsonbValue(parse_state, WJB_VALUE, &val);
						r_old = JsonbIteratorNext(&it_old_counts, &k_old, false);
						r_new = JsonbIteratorNext(&it_new_counts, &k_new, false);
					}
				}
				while (r_old == WJB_KEY)
				{
					pushJsonbValue(parse_state, WJB_KEY, &k_old);
					(void) JsonbIteratorNext(&it_old_counts, &v_old, true);
					pushJsonbValue(parse_state, WJB_VALUE, &v_old);
					r_old = JsonbIteratorNext(&it_old_counts, &k_old, false);
				}
				while (r_new == WJB_KEY)
				{
					pushJsonbValue(parse_state, WJB_KEY, &k_new);
					(void) JsonbIteratorNext(&it_new_counts, &v_new, true);
					pushJsonbValue(parse_state, WJB_VALUE, &v_new);
					r_new = JsonbIteratorNext(&it_new_counts, &k_new, false);
				}

				if (new_elements)
				{
					for(int i = 0; i < new_elements_count; i++)
						pfree(new_elements[i]);
					pfree(new_elements);
				}
			}
			pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);

			key.val.string.val = "elements_count"; key.val.string.len = 14; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(elements_count_new); pushJsonbValue(parse_state, WJB_VALUE, &val);

			key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(parse_state, WJB_KEY, &key);
			val.type = jbvString; val.val.string.val = "array_summary"; val.val.string.len = 13; pushJsonbValue(parse_state, WJB_VALUE, &val);
			pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
		}
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

	(void) JsonbIteratorNext(it, &v_k, false);	/* "value" */
	(void) JsonbIteratorNext(it, &v, true);

	pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);

	if (strcmp(type_name, "integer") == 0)
	{
		JsonbValue	key,
					val;
		Datum		new_sum,
					new_count,
					new_mean,
					new_min,
					new_max,
					new_sum_sq_diff;

		new_sum = NumericGetDatum(v.val.numeric);
		new_count = Int32GetDatum(1);
		new_mean = new_sum;
		new_min = new_sum;
		new_max = new_sum;
		new_sum_sq_diff = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));

		key.type = jbvString;

		/* The order of keys must be alphabetical for jsonb */
		key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, new_count)); pushJsonbValue(parse_state, WJB_VALUE, &val);
		key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_max); pushJsonbValue(parse_state, WJB_VALUE, &val);
		key.val.string.val = "mean"; key.val.string.len = 4; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_mean); pushJsonbValue(parse_state, WJB_VALUE, &val);
		key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_min); pushJsonbValue(parse_state, WJB_VALUE, &val);
		key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_sum); pushJsonbValue(parse_state, WJB_VALUE, &val);
		key.val.string.val = "sum_sq_diff"; key.val.string.len = 11; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(new_sum_sq_diff); pushJsonbValue(parse_state, WJB_VALUE, &val);
		key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvString; val.val.string.val = "integer_summary"; val.val.string.len = 15; pushJsonbValue(parse_state, WJB_VALUE, &val);
	}
	else if (strcmp(type_name, "text") == 0 || strcmp(type_name, "boolean") == 0)
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
		pushJsonbValue(parse_state, WJB_KEY, &key);
		pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);
		pushJsonbValue(parse_state, WJB_KEY, &v_stat_key);
		val.type = jbvNumeric;
		val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1)));
		pushJsonbValue(parse_state, WJB_VALUE, &val);
		pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);

		key.val.string.val = "type"; key.val.string.len = 4;
		pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvString; val.val.string.val = psprintf("%s_summary", type_name); val.val.string.len = strlen(val.val.string.val);
		pushJsonbValue(parse_state, WJB_VALUE, &val);
	}
	else
	{
		/* Summary for arrays and other types */
		JsonbValue	key,
					val;
		int			elements_count = 0;
		int			type_name_len = strlen(type_name);
		bool		is_array = (type_name_len > 2 && strcmp(type_name + type_name_len - 2, "[]") == 0);
		char	   *arr_str;
		int			len;

		/* Keys must be in alphabetical order for jsonb */
		key.type = jbvString;
		key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(1))); pushJsonbValue(parse_state, WJB_VALUE, &val);

		key.val.string.val = "counts"; key.val.string.len = 6; pushJsonbValue(parse_state, WJB_KEY, &key);
		if (is_array && v.val.string.len > 2)
		{
			char	  **elements;
			int			elements_capacity;
			char	   *p;
			char	   *element_start;
			bool		in_quotes;

			arr_str = pnstrdup(v.val.string.val + 1, v.val.string.len - 2);
			elements = palloc(sizeof(char *) * 8);
			elements_capacity = 8;
			p = arr_str;
			element_start = p;
			in_quotes = false;

			for (p = arr_str; *p; p++)
			{
				if (*p == '"')
					in_quotes = !in_quotes;
				else if (*p == ',' && !in_quotes)
				{
					*p = '\0';
					if (elements_count >= elements_capacity)
					{
						elements_capacity *= 2;
						elements = repalloc(elements, sizeof(char *) * elements_capacity);
					}
					elements[elements_count++] = element_start;
					element_start = p + 1;
				}
			}
			if (elements_count >= elements_capacity)
			{
				elements_capacity++;
				elements = repalloc(elements, sizeof(char *) * elements_capacity);
			}
			elements[elements_count++] = element_start;

			for (int i = 0; i < elements_count; i++)
			{
				len = strlen(elements[i]);

				if (len >= 2 && elements[i][0] == '"' && elements[i][len - 1] == '"')
				{
					memmove(elements[i], elements[i] + 1, len - 2);
					elements[i][len - 2] = '\0';
				}
				elements[i] = pstrdup(elements[i]);
			}

			qsort(elements, elements_count, sizeof(char *), qsort_strcmp);

			pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);
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
						key.val.string.val = current_element; key.val.string.len = strlen(current_element); pushJsonbValue(parse_state, WJB_KEY, &key);
						val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(current_count))); pushJsonbValue(parse_state, WJB_VALUE, &val);
						current_element = elements[i];
						current_count = 1;
					}
				}
				key.val.string.val = current_element; key.val.string.len = strlen(current_element); pushJsonbValue(parse_state, WJB_KEY, &key);
				val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(current_count))); pushJsonbValue(parse_state, WJB_VALUE, &val);
			}
			pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
			pfree(arr_str);
			pfree(elements);
		}
		else
		{
			pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);
			pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
		}

		key.val.string.val = "elements_count"; key.val.string.len = 14; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(elements_count))); pushJsonbValue(parse_state, WJB_VALUE, &val);

		key.type = jbvString;
		key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(parse_state, WJB_KEY, &key);
		val.type = jbvString; val.val.string.val = "array_summary"; val.val.string.len = 13; pushJsonbValue(parse_state, WJB_VALUE, &val);
	}

	pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
	pfree(type_name);

	r = JsonbIteratorNext(it, &v, false);
	if (r != WJB_END_OBJECT)
		elog(ERROR, "malformed stat object, expected object end");
}

PG_FUNCTION_INFO_V1(jsonb_stats_summary_accum);
Datum
jsonb_stats_summary_accum(PG_FUNCTION_ARGS)
{
	Jsonb	   *state = PG_GETARG_JSONB_P(0);
	Jsonb	   *stats = PG_GETARG_JSONB_P(1);
	JsonbParseState *parse_state = NULL;
	JsonbIterator *state_it,
			   *stats_it;
	JsonbValue	state_key,
				state_val,
				stats_key;
	uint32		r_state,
				r_stats;
	MemoryContext agg_context,
				old_context;
	Jsonb	   *res;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "jsonb_stats_summary_accum called in non-aggregate context");

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

	while (r_state == WJB_KEY && r_stats == WJB_KEY)
	{
		int			cmp = compare_jsonb_string_values(&state_key, &stats_key);

		if (cmp < 0)
		{
			/* Key is only in state, copy it over */
			pushJsonbValue(&parse_state, WJB_KEY, &state_key);
			(void) JsonbIteratorNext(&state_it, &state_val, true);
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

	/* Copy over remaining state keys */
	while (r_state == WJB_KEY)
	{
		pushJsonbValue(&parse_state, WJB_KEY, &state_key);
		(void) JsonbIteratorNext(&state_it, &state_val, true);
		pushJsonbValue(&parse_state, WJB_VALUE, &state_val);
		r_state = JsonbIteratorNext(&state_it, &state_key, false);
	}

	/* Initialize summaries for remaining stats keys */
	while (r_stats == WJB_KEY)
	{
		pushJsonbValue(&parse_state, WJB_KEY, &stats_key);
		push_summary(&parse_state, NULL, &stats_it, true);
		r_stats = JsonbIteratorNext(&stats_it, &stats_key, false);
	}

	res = JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL));

	MemoryContextSwitchTo(old_context);

	PG_RETURN_JSONB_P(res);
}

PG_FUNCTION_INFO_V1(jsonb_stats_to_summary_round);
Datum
jsonb_stats_to_summary_round(PG_FUNCTION_ARGS)
{
	Jsonb	   *summary = PG_GETARG_JSONB_P(0);
	JsonbParseState *parse_state = NULL;
	JsonbIterator *it;
	JsonbValue	k,
				v,
			   *v_type_ptr;
	JsonbValue	key_finder;

	if (!JB_ROOT_IS_OBJECT(summary))
		PG_RETURN_JSONB_P(summary);

	it = JsonbIteratorInit(&summary->root);
	(void) JsonbIteratorNext(&it, &k, false);	/* consume { */
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	key_finder.type = jbvString;
	key_finder.val.string.val = "type";
	key_finder.val.string.len = 4;

	while (JsonbIteratorNext(&it, &k, false) == WJB_KEY)
	{
		(void) JsonbIteratorNext(&it, &v, true);
		v_type_ptr = findJsonbValueFromContainer(v.val.binary.data, JB_FOBJECT, &key_finder);
		if (v_type_ptr && v_type_ptr->type == jbvString)
		{
			char	   *type_name = pnstrdup(v_type_ptr->val.string.val, v_type_ptr->val.string.len);

			if (strcmp(type_name, "integer_summary") == 0)
			{
				JsonbValue *count_ptr, *max_ptr, *mean_ptr, *min_ptr, *sum_ptr, *sum_sq_diff_ptr;
				Datum count_val, mean_val, sum_sq_diff_val, variance = (Datum) 0, stddev = (Datum) 0, cv_pct = (Datum) 0;
				JsonbParseState *ps_new = NULL;
				JsonbValue	new_v, key, val;

#define FIND_SUMMARY_VAL(keyname, var) \
    key_finder.val.string.val = keyname; \
    key_finder.val.string.len = strlen(keyname); \
    var = findJsonbValueFromContainer(v.val.binary.data, JB_FOBJECT, &key_finder);

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
				val.type = jbvString; val.val.string.val = "integer_summary"; val.val.string.len = 15; pushJsonbValue(&ps_new, WJB_VALUE, &val);
				PUSH_ROUNDED_DATUM("variance", variance, 2);

				new_v.type = jbvBinary;
				new_v.val.binary.data = &JsonbValueToJsonb(pushJsonbValue(&ps_new, WJB_END_OBJECT, NULL))->root;
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

	PG_RETURN_JSONB_P(JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL)));
}

static void
merge_summaries(JsonbParseState **ps, JsonbValue *v_a, JsonbValue *v_b)
{
	JsonbValue	key_finder;
	JsonbValue *v_type_ptr_a,
			   *v_type_ptr_b;
	char	   *type_a,
			   *type_b;

	key_finder.type = jbvString;
	key_finder.val.string.val = "type";
	key_finder.val.string.len = 4;

	v_type_ptr_a = findJsonbValueFromContainer(v_a->val.binary.data, JB_FOBJECT, &key_finder);
	v_type_ptr_b = findJsonbValueFromContainer(v_b->val.binary.data, JB_FOBJECT, &key_finder);

	type_a = pnstrdup(v_type_ptr_a->val.string.val, v_type_ptr_a->val.string.len);
	type_b = pnstrdup(v_type_ptr_b->val.string.val, v_type_ptr_b->val.string.len);

	if (strcmp(type_a, type_b) != 0)
		elog(ERROR, "type mismatch in summary merge: %s vs %s", type_a, type_b);

	pushJsonbValue(ps, WJB_BEGIN_OBJECT, NULL);

	if (strcmp(type_a, "integer_summary") == 0)
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

		FIND_NUMERIC_MERGE(v_a->val.binary.data, "sum", sum_a);
		FIND_NUMERIC_MERGE(v_a->val.binary.data, "count", count_a);
		FIND_NUMERIC_MERGE(v_a->val.binary.data, "mean", mean_a);
		FIND_NUMERIC_MERGE(v_a->val.binary.data, "min", min_a);
		FIND_NUMERIC_MERGE(v_a->val.binary.data, "max", max_a);
		FIND_NUMERIC_MERGE(v_a->val.binary.data, "sum_sq_diff", sum_sq_diff_a);
		FIND_NUMERIC_MERGE(v_b->val.binary.data, "sum", sum_b);
		FIND_NUMERIC_MERGE(v_b->val.binary.data, "count", count_b);
		FIND_NUMERIC_MERGE(v_b->val.binary.data, "mean", mean_b);
		FIND_NUMERIC_MERGE(v_b->val.binary.data, "min", min_b);
		FIND_NUMERIC_MERGE(v_b->val.binary.data, "max", max_b);
		FIND_NUMERIC_MERGE(v_b->val.binary.data, "sum_sq_diff", sum_sq_diff_b);

		total_count = DirectFunctionCall2(numeric_add, count_a, count_b);
		delta = DirectFunctionCall2(numeric_sub, mean_b, mean_a);

		key.type = jbvString;
		key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(total_count); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "max"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DatumGetBool(DirectFunctionCall2(numeric_gt, max_a, max_b)) ? max_a : max_b); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "mean"; key.val.string.len = 4; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_div, DirectFunctionCall2(numeric_add, DirectFunctionCall2(numeric_mul, mean_a, count_a), DirectFunctionCall2(numeric_mul, mean_b, count_b)), total_count)); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "min"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DatumGetBool(DirectFunctionCall2(numeric_lt, min_a, min_b)) ? min_a : min_b); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "sum"; key.val.string.len = 3; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_add, sum_a, sum_b)); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "sum_sq_diff"; key.val.string.len = 11; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_add, DirectFunctionCall2(numeric_add, sum_sq_diff_a, sum_sq_diff_b), DirectFunctionCall2(numeric_mul, DirectFunctionCall2(numeric_mul, delta, delta), DirectFunctionCall2(numeric_div, DirectFunctionCall2(numeric_mul, count_a, count_b), total_count)))); pushJsonbValue(ps, WJB_VALUE, &val);
		key.val.string.val = "type"; key.val.string.len = 4; pushJsonbValue(ps, WJB_KEY, &key);
		val.type = jbvString; val.val.string.val = "integer_summary"; val.val.string.len = 15; pushJsonbValue(ps, WJB_VALUE, &val);

	}
	else if (strcmp(type_a, "text_summary") == 0 || strcmp(type_a, "boolean_summary") == 0 || strcmp(type_a, "array_summary") == 0)
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

		if (strcmp(type_a, "array_summary") == 0)
		{
			JsonbValue *v_ptr;
			Datum		count_a, count_b, total_count, elements_count_a, elements_count_b, total_elements_count;

			key_finder.val.string.val = "count"; key_finder.val.string.len = 5;
			v_ptr = findJsonbValueFromContainer(v_a->val.binary.data, JB_FOBJECT, &key_finder); count_a = NumericGetDatum(v_ptr->val.numeric);
			v_ptr = findJsonbValueFromContainer(v_b->val.binary.data, JB_FOBJECT, &key_finder); count_b = NumericGetDatum(v_ptr->val.numeric);
			total_count = DirectFunctionCall2(numeric_add, count_a, count_b);

			key_finder.val.string.val = "elements_count"; key_finder.val.string.len = 14;
			v_ptr = findJsonbValueFromContainer(v_a->val.binary.data, JB_FOBJECT, &key_finder); elements_count_a = NumericGetDatum(v_ptr->val.numeric);
			v_ptr = findJsonbValueFromContainer(v_b->val.binary.data, JB_FOBJECT, &key_finder); elements_count_b = NumericGetDatum(v_ptr->val.numeric);
			total_elements_count = DirectFunctionCall2(numeric_add, elements_count_a, elements_count_b);

			key.type = jbvString;
			key.val.string.val = "count"; key.val.string.len = 5; pushJsonbValue(ps, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(total_count); pushJsonbValue(ps, WJB_VALUE, &val);
			key.val.string.val = "elements_count"; key.val.string.len = 14; pushJsonbValue(ps, WJB_KEY, &key);
			val.type = jbvNumeric; val.val.numeric = DatumGetNumeric(total_elements_count); pushJsonbValue(ps, WJB_VALUE, &val);
		}

		key_finder.val.string.val = "counts";
		key_finder.val.string.len = 6;
		v_counts_a_ptr = findJsonbValueFromContainer(v_a->val.binary.data, JB_FOBJECT, &key_finder);
		v_counts_b_ptr = findJsonbValueFromContainer(v_b->val.binary.data, JB_FOBJECT, &key_finder);

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
		val.type = jbvString; val.val.string.val = psprintf("%s", type_a); val.val.string.len = strlen(val.val.string.val);
		pushJsonbValue(ps, WJB_VALUE, &val);
	}

	pushJsonbValue(ps, WJB_END_OBJECT, NULL);
	pfree(type_a);
	pfree(type_b);
}

PG_FUNCTION_INFO_V1(jsonb_stats_summary_merge);
Datum
jsonb_stats_summary_merge(PG_FUNCTION_ARGS)
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

	while (r_a == WJB_KEY && r_b == WJB_KEY)
	{
		int			cmp = compare_jsonb_string_values(&k_a, &k_b);

		if (cmp < 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &k_a);
			(void) JsonbIteratorNext(&it_a, &v_a, true);
			pushJsonbValue(&parse_state, WJB_VALUE, &v_a);
			r_a = JsonbIteratorNext(&it_a, &k_a, false);
		}
		else if (cmp > 0)
		{
			pushJsonbValue(&parse_state, WJB_KEY, &k_b);
			(void) JsonbIteratorNext(&it_b, &v_b, true);
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

	while (r_a == WJB_KEY)
	{
		pushJsonbValue(&parse_state, WJB_KEY, &k_a);
		(void) JsonbIteratorNext(&it_a, &v_a, true);
		pushJsonbValue(&parse_state, WJB_VALUE, &v_a);
		r_a = JsonbIteratorNext(&it_a, &k_a, false);
	}
	while (r_b == WJB_KEY)
	{
		pushJsonbValue(&parse_state, WJB_KEY, &k_b);
		(void) JsonbIteratorNext(&it_b, &v_b, true);
		pushJsonbValue(&parse_state, WJB_VALUE, &v_b);
		r_b = JsonbIteratorNext(&it_b, &k_b, false);
	}

	PG_RETURN_JSONB_P(JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL)));
}
