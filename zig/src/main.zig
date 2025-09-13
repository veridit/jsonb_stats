const std = @import("std");
const pgzx = @import("pgzx");
const pg = pgzx.pg;

comptime {
    pgzx.PG_MODULE_MAGIC();

    pgzx.PG_FUNCTION_V1("stat_text", stat_text);
}

/// stat(text) -> jsonb
/// This is the proof-of-concept implementation for text values.
fn stat_text(fcinfo: pg.FunctionCallInfo) !pg.Datum {
    // 1. Get the text argument
    const text_datum = pgzx.utils.GET_ARG_DATUM(fcinfo, 0);
    const text_val = try pgzx.text.fromDatum(text_datum);

    // 2. Build the jsonb object: {"type": "str", "value": "..."}
    var jb = pgzx.jsonb.JsonbBuilder.init(null);
    defer jb.deinit();

    try jb.beginObject();
    try jb.pushObjectKey("type");
    try jb.pushString("str");
    try jb.pushObjectKey("value");
    try jb.pushString(text_val);
    try jb.endObject();

    const result = try jb.finish();

    // 3. Return the new jsonb as a Datum
    return pgzx.utils.RETURN_JSONB(result);
}
