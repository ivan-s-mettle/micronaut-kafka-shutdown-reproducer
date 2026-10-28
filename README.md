# micronaut-kafka shutdown order reproducer

Since Micronaut Core 5.2.0, a `@KafkaListener` keeps consuming after the `DataSource` it uses has been closed during
`ApplicationContext.stop()`. With the default listener settings, records whose offsets are committed are never
processed.

## Run

Requires Docker and JDK 25.

```shell
./gradlew test -PmicronautVersion=5.2.4   # fails: ~390 records lost
./gradlew test -PmicronautVersion=5.1.3   # passes: 0 records lost
```

## What the test does

1. Produces 2,000 records to a single-partition topic.
2. Starts the context. `RecordListener` (default `OffsetStrategy.AUTO` and default error strategy) saves each record's
   offset to Postgres through `ProcessedRecordRepository`, which uses the `DataSource`.
3. Once 100 records are saved, calls `context.stop()` while the listener is part-way through its first polled batch.
4. Compares the consumer group's committed offset with the offsets saved in Postgres.

`io.micronaut.context.lifecycle` is logged at `DEBUG`, so the output shows every bean in the order it is destroyed.

## Results

| Micronaut | Lost records | `KafkaConsumerProcessor` destroyed | `DatasourceFactory` destroyed |
|---|---|---|---|
| 5.2.4 (core 5.2.20, micronaut-kafka 6.2.0) | 390, 391, 390 | 12th | 3rd |
| 5.1.3 (core 5.1.13, micronaut-kafka 6.1.0) | 0, 0, 0 | 2nd–4th | 23rd–28th |

On 5.2, `DatasourceFactory` closes the Hikari pool while the consumer is still processing. The next record fails with
`HikariDataSource ... has been closed`. The default error strategy (`NONE`) then skips the rest of that partition's
polled batch, and the consumer commits offset 500 on close.

On 5.1, `KafkaConsumerProcessor` is destroyed first. Its `close()` waits for the listener to finish the batch, and only
then are the repository and the pool destroyed.
