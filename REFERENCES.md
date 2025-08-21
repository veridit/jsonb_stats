# C-Language Extension Development in PostgreSQL

This document serves as a quick reference for developers working on the `jsonb_stats` project, summarizing key concepts from the official PostgreSQL documentation for writing C-language extensions.

## C-Language Functions

Reference: [PostgreSQL Documentation: C-Language Functions](https://www.postgresql.org/docs/current/xfunc-c.html)

### Key Concepts

*   **Dynamic Loading**: C functions are compiled into shared libraries (`.so` files) and loaded by the server on demand. Each shared library must contain a `PG_MODULE_MAGIC;` macro in one of its source files.
*   **Version 1 Calling Convention**: This is the standard for C functions.
    *   Functions must be declared as `Datum funcname(PG_FUNCTION_ARGS)`.
    *   A `PG_FUNCTION_INFO_V1(funcname);` macro call is required for each function.
    *   Arguments are accessed via `PG_GETARG_...()` macros.
    *   Results are returned via `PG_RETURN_...()` macros.
*   **Memory Management**:
    *   Use `palloc()` and `pfree()` instead of `malloc()` and `free()`.
    *   Memory allocated with `palloc()` is managed within PostgreSQL's memory contexts and is automatically freed at the end of a transaction, preventing leaks.
    *   For data that must persist across multiple function calls (e.g., in aggregates or set-returning functions), use the appropriate long-lived memory context.
*   **Data Types**:
    *   Data can be passed by value or by reference.
    *   Variable-length types (like `text` or `jsonb`) have a header. Use macros to work with them.
*   **NULL Handling**:
    *   Functions can be declared `STRICT` in SQL, which means they are not called if any input is NULL. PostgreSQL handles this automatically.
    *   For non-`STRICT` functions, use `PG_ARGISNULL(arg_index)` to check for NULLs and `PG_RETURN_NULL()` to return a NULL value.

## User-Defined Aggregates

Reference: [PostgreSQL Documentation: User-Defined Aggregates](https://www.postgresql.org/docs/current/xaggr.html)

### Key Concepts

*   **State Machine**: Aggregates operate as a state machine.
    *   `stype`: The data type of the internal state. For complex states without a SQL equivalent, `internal` is used.
    *   `sfunc` (State Transition Function): Called for each input row. It takes the current state and the input value(s) and returns the new state.
    *   `finalfunc` (Final Function): Called after all rows are processed. It takes the final state and returns the aggregate result. This is optional.
*   **State Management in C**:
    *   The `AggCheckCallContext` function can be used to determine if a C function is being called as part of an aggregate.
    *   This allows for optimizations, such as modifying the state value in-place, which is safe for transition values.
    *   The second argument to `AggCheckCallContext` can retrieve the memory context for aggregate state values, which is crucial for data that must persist between calls.
*   **`finalfunc_extra`**: This option in `CREATE AGGREGATE` passes extra arguments to the final function, corresponding to the aggregate's input arguments.

## Server Programming Interface (SPI)

Reference: [PostgreSQL Documentation: Server Programming Interface](https://www.postgresql.org/docs/current/spi.html)

### Key Concepts

*   **Execution Context**: SPI allows C functions to execute SQL commands within the server. All SPI operations must be wrapped between `SPI_connect()` and `SPI_finish()`.
*   **Error Handling**: A critical feature of SPI is its error handling model. If any command executed via SPI (e.g., `SPI_execute`) fails, **control is not returned to the C function**. Instead, the current transaction or subtransaction is immediately rolled back, and an error is raised, unwinding the stack. Documented error-return codes from SPI functions are only for errors detected within the SPI layer itself, not for errors from the executed SQL.
*   **Memory Management**: Memory for query results (like `SPI_tuptable`) is allocated in a context that is automatically freed by `SPI_finish()`. For data that needs to be returned from the function, it must be copied into the upper executor context using functions like `SPI_palloc` or `SPI_copytuple`.
