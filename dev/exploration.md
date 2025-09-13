# Exploration of Naming Conventions for `type`

## Introduction: The Need for Self-Describing Structures

The user's insight about adding a `type` discriminator to the top-level objects (`stats` and `stats_agg`) is a crucial refinement. Without it, our discriminated union is incomplete. A consumer of the JSONB data would have to infer the object's nature from its shape (e.g., "does it contain keys whose values have a `type` and `value` field? It must be a `stats` object"). This is brittle.

By adding a top-level `type`, we create a fully self-describing system at every level:

-   **`stat`**: `{"type": "integer", "value": 10}`. The `type` field describes the value. The object itself has no top-level type, as it is the base unit.
-   **`stats`**: `{"type": "stats", "num_employees": {"type": "integer", ...}}`. The top-level `type` declares it as a collection of `stat` objects.
-   **`stats_agg`**: `{"type": "stats_agg", "num_employees": {"type": "int_agg", ...}}`. The top-level `type` declares it as a collection of summary objects.

This creates a clear, parseable hierarchy and completes the discriminated union pattern.

## Goal

The purpose of this document is to explore and decide upon a naming convention for the `type` field within the `jsonb_stats` structures (`stat` and `stats_agg`). The chosen convention must be rigorous, clear, and developer-friendly, with a primary goal of forming a perfect **discriminated union**.

## Guiding Principle: Discriminated Unions

A discriminated union is a data structure where a single field (the "discriminator," in our case `type`) unambiguously determines the exact shape and nature of the object. This is a powerful concept that enables:

1.  **Type Safety**: In languages like TypeScript, it allows for compile-time checks, ensuring that if `type` is `"integer_summary"`, the object is guaranteed to have fields like `sum` and `mean`.
2.  **Rigorous Logic**: It eliminates ambiguity. There is no guesswork about what fields an object contains or what kind of data it represents.
3.  **Maintainability**: It makes the code that consumes this data (in SQL or in client applications) simpler, safer, and easier to reason about.

Any chosen scheme will be evaluated primarily on how well it upholds this principle.

---

### Option 1: Explicit Suffixes with Top-Level Types (Recommended)

This approach combines explicit suffixes with top-level type discriminators, creating a fully self-describing system.

-   **Top-Level Types**: `stats`, `stats_agg`.
-   **`stat` types**: `integer`, `float`, `text`, `boolean`, `date`, `array`, etc. (Directly uses PostgreSQL-inspired type names).
-   **`summary` types**: `integer_summary`, `float_summary`, `text_summary`, `boolean_summary`, `date_summary`, `array_summary`, etc.

#### Example `stats` object
```json
{
    "type": "stats",
    "num_employees": {"type": "integer", "value": 150},
    "is_profitable": {"type": "boolean", "value": true},
    "industry":      {"type": "text",    "value": "tech"}
}
```

#### Example `stats_agg`
```json
{
    "type": "stats_agg",
    "num_employees": {
        "type": "int_agg",
        "count": 3,
        "sum": 2700,
        ...
    },
    "revenue": {
        "type": "float_summary",
        "count": 3,
        "sum": 50000.75,
        ...
    }
}
```

#### Analysis
-   **Pros**:
    -   **Perfect Discriminated Union**: The `type` value is unique and precisely defines the object's structure and the nature of its contents.
    -   **Maximal Rigor**: It preserves the distinction between different base types (e.g., `integer` vs. `numeric`), which is critical for a foundational library that prioritizes statistical correctness.
    -   **Clarity**: The `_summary` suffix makes it immediately obvious that the object is an aggregate, fulfilling a key requirement.
-   **Cons**:
    -   The type names are more verbose than other options.

### Refining Option 1: Conciseness and Clarity

While Option 1 is functionally correct, it can be improved based on two points from the design discussion:
1.  The `_summary` suffix, while explicit, is long.
2.  Using PostgreSQL-native type names (like `text`) might be less portable or intuitive for client-side consumers than more generic names (like `string`).

A good alternative for `_summary` is `_agg` (for "aggregate"). It is shorter, semantically clear to developers, and avoids potential confusion with the `sum` field in numeric summaries.

We can also adopt shorter, more common names for the base types.

#### A Deeper Look at Numeric Types: Performance, Precision, and Practicality

The primary driver for this C extension is **performance**. The volume of data makes the PL/pgSQL equivalent too slow. Therefore, our design for numeric types must prioritize speed while offering practical levels of precision for common statistical survey use cases.

##### The Performance vs. Precision Trade-off

PostgreSQL offers several numeric types, each with different performance and precision characteristics. Our goal is to expose these trade-offs to the user so they can choose the right tool for the job.

1.  **Perfect Precision (`numeric`)**: PostgreSQL's `numeric` type is exact and essential for financial data where precision is non-negotiable. However, this comes at a significant performance cost. Calculations are slow, and to preserve precision when serializing to JSON, values *must* be stored as strings. We will defer implementing a `numeric_agg` type until a clear use case arises that justifies this performance trade-off.

2.  **High Performance (`int`, `float`)**: For many statistical use cases, native machine types are ideal.
    *   `int` (PostgreSQL `bigint`): Uses `int64`. It is extremely fast for integer-only data.
    *   `float` (PostgreSQL `float8`): Uses `double`. It is very fast for general-purpose floating-point math but is subject to binary floating-point inaccuracies. This is often acceptable for scientific or statistical data where some imprecision is inherent.

3.  **The Pragmatic Middle Ground (`decimal2`)**: A very common requirement in survey data is for numbers with a fixed, two-decimal precision (e.g., monetary values, some percentages). A full `numeric` implementation is overkill and too slow for this. A standard `float` risks precision errors (e.g., `0.1 + 0.2` not being exactly `0.3`).

    The optimal solution is a specialized `dec2` type which uses a hybrid approach:
    - **External Representation**: In both `stat` and `stats_agg` objects, `dec2` values are represented as standard JSON `number`s (floats), rounded to two decimal places. This ensures they are human-readable and compatible with external tools.
    - **Internal Representation**: For aggregation, the `dec2` values are converted to scaled `int64`s (by multiplying by 100). All calculations are performed with fast integer arithmetic to avoid floating-point errors and ensure precision. The final results are then un-scaled before being output.
    This provides the speed of integer arithmetic and the precision of fixed-point decimals, while presenting a simple, standard float interface to the user.

This leads to a refined, performance-oriented proposal for our type system:

##### Final Naming Refinements for Brevity

To ensure the type names are as ergonomic as possible, we will adopt common abbreviations for the longest names.

-   `string` -> `str`
-   `array` -> `arr`
-   `decimal2` -> `dec2`

A note on `float` vs. `dec`: While `dec` is shorter, the name `float` is the industry standard for `float8`/`double` data types. Using `dec` would create ambiguity, as it is the common abbreviation for `decimal`, which is an exact numeric type that `float` is not. Therefore, to maintain clarity and correctness, we will retain the name `float`.

The final proposed type system is:

-   **Top-Level Types**: `stats`, `stats_agg`.
-   **`stat` types**:
    -   `int`: For `bigint` values.
    -   `float`: For `float8` values.
    -   `dec2`: For fixed two-decimal-place values. Represented as a JSON `number` but calculated internally as a scaled integer.
    -   `str`, `bool`, `date`, `arr`.
-   **`summary` types**:
    -   `int_agg`: Summary of `int` values.
    -   `float_agg`: Summary of `float` values.
    -   `dec2_agg`: Summary of `dec2` values.
    -   `str_agg`, `bool_agg`, `date_agg`, `arr_agg`.

All numeric fields in all summaries will be represented as JSON `number`s for maximum performance and client compatibility. The `dec2` type provides exact two-decimal precision through internal scaled-integer arithmetic, while still appearing as a standard float in the JSON output.

---

### Option 2: Simple Generic Types (`statbus` Style)

This approach uses simple, generic names for summary types, similar to primitive JSON types.

-   **`stat` types**: `integer`, `float`, `text`, `boolean`, ...
-   **`summary` types**: `number`, `string`, `boolean`, `array`.

#### Example `stats_agg`
```json
{
    "num_employees": {
        "type": "number",
        "count": 3,
        "sum": 2700,
        ...
    },
    "revenue": {
        "type": "number",
        "count": 3,
        "sum": 50000.75,
        ...
    }
}
```

#### Analysis
-   **Pros**:
    -   Concise type names.
-   **Cons**:
    -   **Fails the Discriminated Union Principle**: This is a critical flaw. The type `"number"` is ambiguous. It could represent a summary of `integer` or `float`. The consumer of this data has no way to know the precision of the underlying calculations. This ambiguity undermines the goal of rigor.

---

### Option 3: Nested Type Information

This approach uses a generic top-level type (`summary`) and a secondary field to specify the base type.

-   **`summary` object structure**:
    ```json
    {
        "type": "summary",
        "base_type": "integer",
        "count": 3,
        ...
    }
    ```

#### Analysis
-   **Pros**:
    -   Very structured and explicit from a data modeling perspective.
-   **Cons**:
    -   Requires checking two fields (`type` and `base_type`) to discriminate the type, which is more complex and less ergonomic for client-side code. A single discriminator is always preferable.
    -   Significantly more verbose.

---

## Conclusion and Recommendation

**The Refined version of Option 1** is the superior choice.

It provides the best balance of clarity, rigor, and developer experience. By using concise type names (`int`, `string`) and the `_agg` suffix, we achieve a scheme that is both ergonomic and fully self-describing. It directly aligns with the core principle of using a discriminated union at every level and supports the project's goal of being a robust, foundational extension that other tools like `statbus` can rely on for correctness. This design is also perfectly suited for generating type-safe client libraries (e.g., in TypeScript).
