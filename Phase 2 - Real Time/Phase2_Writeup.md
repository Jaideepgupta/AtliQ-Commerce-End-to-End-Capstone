# AtliQ Commerce — Phase 2 Real-Time Lane

## How Phase 2 relates to Phase 1

In Phase 1, AtliQ Commerce uses a batch data pipeline where data moves from the
OLTP database through ADF and the medallion architecture to the analytics
layer. This is suitable for reporting and business analysis where daily or
periodic data is sufficient.

Phase 2 adds a separate "speed lane" alongside the batch lane. Order events
are produced in real time and sent to Kafka on Confluent Cloud. Databricks
Structured Streaming processes these events through Bronze, Silver and Gold
Delta tables. Airflow then performs scheduled operational work such as data
quality checks, table maintenance and the daily summary.

The two lanes serve different business needs. The batch lane is appropriate
for stable historical reporting, reconciliation and workloads where
near-real-time information is not required. The real-time lane is useful for
operational questions such as how many orders are arriving now or whether
revenue has suddenly changed during the last few minutes.

## Why Gold windows appear late

The Gold layer aggregates `payment_received` events into 5-minute tumbling
windows. A window is not immediately written when its time period ends because
the streaming pipeline uses a 10-minute watermark. The watermark allows the
system to wait for late-arriving events before considering a window complete.

Therefore, Gold results can appear approximately 10–15 minutes behind
real time. This delay is intentional and improves correctness when events
arrive late; it is not a pipeline failure.

## Why events are keyed by order_id

Kafka events are published using `order_id` as the message key. Kafka
guarantees ordering within a partition, and messages with the same key are
sent to the same partition. Therefore, lifecycle events belonging to the
same order can remain ordered, which is important when processing events such
as order placement, payment and cancellation.

## Data quality and orchestration

Airflow runs the scheduled streaming operations hourly. The first task checks
that Silver contains events from the last two hours. If no recent events
exist, the quality gate fails. When the check succeeds, Airflow runs OPTIMIZE
on the Silver and Gold tables and then rebuilds the daily summary.

This demonstrates that streaming does not replace orchestration: the stream
handles continuously arriving data, while scheduled orchestration handles
quality, maintenance and business rollups.