# Open Data Fabric: RisingWave Engine

<div align="center">

[![Release](https://img.shields.io/github/v/release/kamu-data/kamu-engine-risingwave?include_prereleases&logo=rust&logoColor=orange&style=for-the-badge)](https://github.com/kamu-data/kamu-engine-risingwave/releases/latest)
[![CI](https://img.shields.io/github/actions/workflow/status/kamu-data/kamu-engine-risingwave/build.yaml?logo=githubactions&label=CI&logoColor=white&style=for-the-badge&branch=master)](https://github.com/kamu-data/kamu-engine-risingwave/actions)
[![Dependencies](https://deps.rs/repo/github/kamu-data/kamu-engine-risingwave/status.svg?&style=for-the-badge)](https://deps.rs/repo/github/kamu-data/kamu-engine-risingwave)
[![Chat](https://shields.io/discord/898726370199359498?style=for-the-badge&logo=discord&label=Discord)](https://discord.gg/nU6TXRQNXC)

</div>

This the implementation of the `Engine` contract of [Open Data Fabric](http://opendatafabric.org/) using the [RisingWave](https://github.com/risingwavelabs/risingwave) open-source streaming database. It is currently in use in [kamu](https://github.com/kamu-data/kamu-cli) data management tool.

This repository is a fork of the [RisingWave repo](https://github.com/risingwavelabs/risingwave) since it currently cannot be used as a library or extended in a modular way with ODF-specific sources and sinks.


## Features
This engine is experimental and has limited functionality.

We currently recommend it for queries like:
- Streaming aggregations using window functions
- Streaming aggregations using tumbling windows with `GROUP BY`
- Top-N aggregations via materialized views

More information and engine comparisons are [available here](https://docs.kamu.dev/cli/supported-engines/).


## Use
See RisingWave [examples](https://docs.risingwave.com/docs/current/get-started/), and [SQL reference](https://docs.risingwave.com/docs/current/sql-references/) for inspiration.

Have a look at [integration tests](./src/odf_adapter/tests/tests/) in this repo for examples of different transform categories.

Pay special attention to [`EMIT ON WINDOW CLOSE`](https://docs.risingwave.com/docs/current/emit-on-window-close/) as unlike Flink, RisingWave does not operate in event-time processing mode by default. In future we may hide this under the hood, but currently you need to specify this clause in your queries explicitly.


## Limitations
1) No support for event-time `JOIN`s. Only processing-time joins are supported. There might be some workarounds depending on the use case.
  
See: https://docs.risingwave.com/docs/current/query-syntax-join-clause/#process-time-temporal-joins

2) No support for allowed **lateness intervals**. Unlike Flink where lateness and watermark are separate concepts, RisingWave will drop all events below the current watermark.

See: https://docs.risingwave.com/docs/current/watermarks/

3) A `SOURCE` accepts append-only data. A `TABLE` with connector accepts both append-only data and updatable data, but it persists all data that flows in meaning that ODF checkpoints would grow significantly.

See: https://docs.risingwave.com/docs/current/data-ingestion/

4) Requires row IDs assignment for delete/update operations.

Currently ODF does not preserve a link between a retraction and the row being retracted. This is easy to implement for ingest merge strategies via additional column, but we would need to assess if this is a reasonable demand for all transform engines that emit retractions/corrections.

An alternative would be to specify primary key for RW source and ensure it uses that to associate delete with the record being deleted.


## Developing
This repository is a fork of the RisingWave engine. Please refer to the [upstream repo](https://github.com/risingwavelabs/risingwave) for developer documentation.

To help with really bad build time we add this to `Cargo.toml`:
```toml
[profile.dev]
debug = "line-tables-only"
```

To build the engine run:
```sh
# Use CARGO_BUILD_JOBS=N env var if this drains your RAM
./risedev b
```

Run the tests (note that tests use fixed ports and thus must run one at a time):
```sh
cd odf
make test
```

Key directories:
- `odf` - build scripts, container image, and other stuff
- `src/odf_adapter` - gRPC server that communicates with ODF coordinator and spawns RW and a subprocess
- `src/connectors/src/source/odf` - custom ODF source implementation
- `src/connectors/src/sink/odf` - custom ODF sink implementation

Environment variables:
- `RUST_LOG=debug` - standard Rust logging
- `RW_ODF_SOURCE_DEBUG=1` - enables debug data logging in the sink
- `RW_ODF_SINK_DEBUG=1` - enables debug data logging in the sink
- `RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK=10` - makes source split arrow record batches into smaller chunks
- `RW_ODF_SOURCE_SLEEP_CHUNK_MS=500` - makes source sleep some time before yielding a chunk