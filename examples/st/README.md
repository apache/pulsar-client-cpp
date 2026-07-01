# Scalable Topics (`pulsar::st`) — API preview examples

These examples exercise the new typed scalable-topics C++ API under
[`include/pulsar/st/`](../../include/pulsar/st). They illustrate the proposed
surface and exist to gather community feedback.

> **Status: API definition only.** The implementation (`lib/st/`) does not exist
> yet, so these examples **compile but do not yet link**. They are wired into the
> CMake build as a compile-only `OBJECT` library (`StExamples` in
> [`examples/CMakeLists.txt`](../CMakeLists.txt)) — header-verified on every build,
> but not linked. Once `lib/st` lands they become normal `add_executable` targets.

The `pulsar::st` API requires **C++20** (the rest of the client stays C++17).
Syntax-check an example against the headers (no linking):

```sh
clang++ -std=c++20 -I ../../include -Wall -fsyntax-only SampleStProducer.cc
```

| File | Shows |
|---|---|
| `SampleStProducer.cc`          | blocking + asynchronous publishing, transactions |
| `SampleStStreamConsumer.cc`    | ordered (per-key) delivery, cumulative ack |
| `SampleStQueueConsumer.cc`     | parallel delivery, individual ack + nack, dead-letter |
| `SampleStCheckpointConsumer.cc`| externally held position via `Checkpoint` |
| `SampleStJsonSchema.cc`        | a struct as JSON with zero boilerplate (`jsonSchema<T>()`, reflect-cpp) |

## API at a glance

- **Typed builders** off one `PulsarClient`: `newProducer` / `newStreamConsumer` /
  `newQueueConsumer` / `newCheckpointConsumer`, each taking a `Schema<T>`.
- **Synchronous calls return `Expected<T>`** (a stand-in for `std::expected`,
  which is C++23): check it, or call `.value()` to throw `ClientException`.
  `Expected<T>` is `[[nodiscard]]`, so a failure cannot be silently dropped.
- **Asynchronous calls return `Future<T>`**: `addListener(...)` to react on
  completion without blocking, `get()` to block, or `co_await` it.
- **Schemas**: primitives are built in; structured types use `jsonSchema<T>()` /
  `avroSchema<T>()` (reflect-cpp derives the SerDe **and** the declared schema from
  the struct — no boilerplate), `protobufNativeSchema<T>()`, or a custom
  `Schema<T>(serde)`. reflect-cpp is a required dependency of `pulsar::st`.
