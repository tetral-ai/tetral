// Package queue owns Engine's durable work queue: the queue_jobs rows that
// services admit in their own transactions and that the Queue service, Job
// Runner and the Sandbox Service lease, and the LISTEN wakeups that tell
// consumers to look.
//
// OWNS:
//   - Table queue_jobs: one row per admitted job. Its status machine, the
//     writer of every transition and the delivery-scope rules are in
//     queue.go, beside the Job type.
//   - Table queue_partition_counters: each partition's last admission
//     sequence, which orders the jobs admitted to it.
//   - The wakeup channels and their listeners (wakeup.go).
//
// DESIGN RULES:
//   - A producer admits a job in the transaction of the change that needs it
//     (EnqueueTx, EnqueueBatchTx), so a rolled-back change leaves no job.
//   - Every write off leased matches the row's lease_token, except
//     ReclaimExpiredLeases, which reclaims an expired lease (queue.go).
//   - ready_at is the time available_at names on the database clock, and
//     only the queue timing metrics read it, so a wait observed across two
//     processes comes from one clock (Job.ReadyAt, ObserveQueueWaitIn).
//   - trace_context holds the producer's traceparent at enqueue, or nothing
//     when it had no trace, and the consumer continues that trace
//     (Job.TraceContext).
//   - A notification is only a hint: a consumer catches up after the first
//     LISTEN and after every reconnect (RunListener).
//
// FILES:
//
//	queue.go               types, limits, the status machine and its errors.
//	postgresql_store.go    PostgreSQLQueueStore: admission, leases, acks,
//	                       retries, reclaim, metrics and Sandbox maintenance.
//	job_runner_lease.go    direct Job Runner leasing across Workspaces.
//	recovery.go            the Runtime recovery payload.
//	wakeup.go              wake signals and the LISTEN loops.
package queue
