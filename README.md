# wx_store
Small process to centralize reading/writing events to a [RocksDB](https://github.com/facebook/rocksdb)-based store in a way that is tailored to the wx_api domain. There are two types of data stored:

## Events
Stored in the `events` column family. This is the majority of data stored with potentially 1000+ events/hour written. Read/write ratio is expected to be in the vicinity of **60*n*:1**, where *n* is the number of users, so fast event iterations are critical. A large component of what makes this implementation fast is some custom binary serialization that is detailed in the **Performance** section below.

## Other
Stored in the **default** column family.

# Tech
Communication with the storage engine is handled using [ZeroMQ](http://zeromq.org/), which provides a performant and ergonomic way of doing IPC/RPC and also a means of concurrency.

# Usage
- Command to build for production: `cargo build --release && strip target/release/wx_store`

Command payloads sent via ZeroMQ are an array of bytes, with the first byte as the **command type** and the rest of the bytes being a type-specific payload.

| byte  | command type | payload contents                                                                                                                                    |
|---|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 0 | PUT Other    | Tuple with the first element being a string key, second element the value as bytes                                                                  |
| 1 | GET Other    | Key as bytes                                                                                                                                        |
| 2 | PUT Event    | EventMessage serialized as bytes                                                                                                                    |
| 3 | GET Events   | u64 timestamp in microseconds, serialized as a string for sorting, then serialized into bytes. Auto-generated based on config if zero bytes passed. |

Responses similarly use the first byte to indicate success (0u8) or failure (1u8) with the rest of the payload being either the successful response or the error message.

# Performance
## Events
- write: 10k in 30 ms *(30 μs)*
- read: 10k in 13 ms *(1.3 μs)*
- delete: 10k in 198 ms *(20 μs)*
## Other
- write: 10k in 180 ms *(18 μs)*
- read: 10k in 41 ms *(4.1 μs)*
## Iteration testing
Iterating across 10k keys in RocksDB, serializing, and sending via IPC:
- 257 µs/key    serde_json::to_vec
- 131 µs/key    using bincode
- **4.4 µs/key    using hand-rolled serialization**
- 3.6 µs/key    DBRawIterator instead of DBIterator
- 3.2 µs/key    unsafe value_inner() instead of value()
- 2.8 µs/key    unsafe transmute on byte length instead of a conversion into a byte array
- 2.5 µs/key    unsafe transmute on number of results
- 2.1 µs/key    LZ4hc compression instead of Snappy or None
- Negligable effect: block size, disabling compression, re-using a buffer, initializing the buffer length

# Tests
All functionality is fairly low level, so instead of opting to mock, coverage is provided by integration tests.

 # TODO
 - Add ability to put/get running tail of upstream metrics (errors, heartbeat, etc.)
 - Add ability to put/get statuses with expiration
 - Potentially move shared client helper into this lib
 - Add cleanup function
 - Add idiomatic benchmark tests
 - Look into custom sort for u64 bytes instead of converting to string
 - Add enum for command bytes and return byte types
 - Implement [zero copy](http://zeromq.org/blog:zero-copy) when/if [issue #139](https://github.com/erickt/rust-zmq/issues/139) is resolved
 