TODO
====

In order of priority:

- See `TODO|WARN|REVIEW` markers in the code
- (More) tests + complete docs & examples
- Implement per-job timeout ~~and ttl for job results.~~
- Clean up lifetimes & trait bounds, the current ones feel too restrective. I'd also like to see if we can make it work without the `Clone` usage.
- Better error handling:
  - *The worker should crash as little as possible. Ideally never if the redis connection is stable*
  - Go over the `unimplemented!()` cases and confirm that they are not reachable.
  - Go over the `unwrap()` cases and confirm that they cannot break. Make sure those that can yield human readable errors.
  - Detect dropped connections and retry / backoff mechanisms to handle temporary unavailability of the broker?
  - Automated retry strategy for failed job? (not panicked jobs)
  - ~~Provide explicit way to fail a job, not just `panic!`~~
  - Provide backtrace from panicked threads (if possible)
- Graceful shutdown of workers.
- Evaluate using `crossbeam` or other threadpools to support parallelisation in the `Worker` itself.
- Implement monitoring (cli? web? both?)
- Implement scheduling (see celery-beat / rust-scheduler)
- Evaluate turning `Brokkr` into a trait and the current impl into `RedisBrokkr` in order to support various backends through one interface. Not all of the usual distributed task queue backends are going to be suitable but a Postgres one seems relevant (see [pq](https://github.com/malthe/pq/) in Python world). Same for `Worker` which could then be implemented to either not spawn threads or do more complex / powerful process isolation similar to what `batch-rs` does.
