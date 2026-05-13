# flink-cdc-fanout

> **Status: reference / archive.** Captures a working pattern from a 2022 production-grade CDC pipeline. Pinned to Flink 1.13, AWS CDK v1, and the (since-renamed) Kinesis Data Analytics service. Not actively maintained — see the [versions section](#versions--current-state) before deploying.

An Apache Flink job that consumes [AWS DMS](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html) and [Striim](https://www.striim.com/) change-data-capture events from a single Kinesis stream and fans them out to:

- **Amazon Elasticsearch / OpenSearch** — one index per source table (`dms_flink_<table>`), with a deterministic document ID per row so re-emitted events upsert instead of duplicating.
- **Amazon S3** — newline-delimited JSON or Apache Parquet, partitioned by event time (`year=/month=/day=/hour=`).
- **Kinesis** or a `DiscardingSink` — for testing and re-streaming.

Originally forked from the [AWS Big Data Blog reference architecture](https://aws.amazon.com/blogs/big-data/streaming-etl-with-apache-flink-and-amazon-kinesis-data-analytics/) (taxi-trip demo). The taxi pipeline still ships in `cdk/` as a one-click deploy; the actual contribution is the CDC handling and routing logic in `src/main/java/`.

## What's in the box

```
src/main/java/io/nathanmauro/flink/streaming/etl/
├── StreamingEtl.java                 // Main job: Kinesis source → filter → keyBy(table) → ES/S3 sinks
├── App.java / ESApp.java             // Local test entry points (deserialize one event, print)
├── events/
│   ├── DmsEvent.java                 // Avro-generated POJO for AWS DMS Kinesis records
│   └── DmsEventLoc.java              // Variant with location/address fields
├── utils/
│   ├── DmsEventSchema.java           // Custom DeserializationSchema (JSON → DmsEvent)
│   ├── DmsEventLocSchema.java        //   "
│   ├── AmazonElasticsearchSink.java  // ES sink with AWS SigV4 request signing
│   ├── ChangeEventBucketAssigner.java// S3 partitioning by table + event timestamp
│   ├── StriimEventBucketAssigner.java//   " for Striim format
│   └── ParameterToolUtils.java       // Merges CLI args with KDA runtime properties
└── data/                             // Sample events + dynamic-type wrappers
src/main/avro/                        // Avro schemas for DMS / Striim / TripEvent
src/main/resources/                   // Sample CDC payloads, ES index policies, log4j config
cdk/                                  // AWS CDK v1 TypeScript stack (taxi-trip demo)
cognito-token.js                      // Postman pre-request script: Cognito → JWT for Kibana
poc.postman_environment.json          // Postman env with placeholder Cognito client/user
es_console.* / es_scripts.cmd         // Dev Tools snippets for Kibana / opensearch-cli
```

## How the fan-out works

1. **Source.** `FlinkKinesisConsumer` reads JSON-encoded CDC events from a Kinesis stream. AWS DMS emits one record per row change; Striim emits a similar `{ before, data, metadata }` envelope.
2. **Deserialize.** `DmsEventLocSchema` parses the envelope into a `DmsEventLoc` POJO with a dynamic-typed `data` map (string/long), since CDC column types are not known at compile time.
3. **Filter.** Drop control records (`record-type != "data"`) — these include `drop-table`, `truncate`, and bulk-load markers that should not become Elasticsearch documents.
4. **Key.** Stream is keyed by `metadata.table-name` so events for the same table land on the same task slot.
5. **Sink (Elasticsearch).** Each event is indexed into `dms_flink_<table>`. The doc ID is an MD5 of the primary-key columns (`StreamingEtl#getId`), so a row's lifetime maps to a single ES document regardless of how many updates flow through.
6. **Sink (S3 Parquet).** `StreamingFileSink` with a `DateTimeBucketAssigner` writes one file per hour per table. Rolling policy is the Flink default (128 MB or 60s).
7. **Sink (discarding).** `DiscardingSink` exists to validate the source/parse path without a real downstream.

## Runtime parameters

The job reads parameters from either Flink CLI args or Managed Flink "runtime properties". Group key = `FlinkApplicationProperties`.

| Parameter                       | Required | Purpose                                                    |
|---------------------------------|----------|------------------------------------------------------------|
| `InputKinesisStream`            | yes      | Source stream name                                         |
| `InputStreamRegion`             | no       | Defaults to the running region                             |
| `InputStreamInitalPosition`     | no       | `LATEST` / `TRIM_HORIZON` / `AT_TIMESTAMP` (Flink default) |
| `TimestampPosition`             | if `AT_TIMESTAMP` | ISO-8601 timestamp                                |
| `OutputElasticsearchEndpoint`   | one of   | `https://<domain>.<region>.es.amazonaws.com`               |
| `ElasticsearchRegion`           | no       | Defaults to the running region                             |
| `ElasticsearchBulkFlushMaxSizeMb` / `MaxActions` / `Interval` | no | Bulk tuning |
| `OutputBucket`                  | one of   | `s3://<bucket>/<prefix>` for Parquet output                |
| `OutputKinesisStream`           | one of   | Re-stream to Kinesis                                       |
| `OutputDiscarding`              | one of   | Sink to `/dev/null`                                        |

At least one `Output*` parameter is required.

## Build

```bash
mvn clean package
```

Produces `target/flink-cdc-fanout-0.1.0.jar` — a shaded fat-jar uploadable to Amazon Managed Service for Apache Flink (the service formerly known as Kinesis Data Analytics).

Run locally against a test event:

```bash
mvn exec:java -Dexec.mainClass="io.nathanmauro.flink.streaming.etl.App"
```

## Demo deploy (taxi-trip pipeline)

The `cdk/` stack from the original AWS sample is retained and deploys a self-contained demo:

```bash
cd cdk
npm install
npm run build
cdk deploy
```

Heads up: this uses AWS CDK v1 (`@aws-cdk/core`), which reached end-of-support on 2023-06-01. A `cdk synth` will work, but `cdk deploy` requires a v1 toolchain. The original [CloudFormation template](https://s3.amazonaws.com/aws-bigdata-blog/artifacts/kinesis-analytics-taxi-consumer/cfn-templates/StreamingEtl.template.json) is still the fastest path to see the demo working.

## Versions & current state

| Component                   | Version here | Latest (as of 2026)             |
|-----------------------------|--------------|---------------------------------|
| Apache Flink                | 1.13.2       | 1.20+                           |
| Flink Elasticsearch connector | 1.13.2     | Moved to separate repo, OpenSearch connector exists |
| Kinesis Data Analytics      | "KDA"        | Renamed to **Amazon Managed Service for Apache Flink** (2024) |
| AWS CDK                     | v1 (EOL)     | v2 (`aws-cdk-lib`)              |
| Avro                        | 1.9.2        | 1.11+                           |
| AWS Java SDK                | v1 (1.12.x)  | v2                              |

Treat this as a working reference for the **pattern** (CDC envelope → keyed stream → fan-out with stable doc IDs), not a turnkey deploy.

## License

MIT-0. See [LICENSE](LICENSE). Forked from `aws-samples/amazon-kinesis-analytics-streaming-etl`; original copyright retained.
