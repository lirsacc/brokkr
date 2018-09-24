brokkr
======

[![Build Status](https://travis-ci.org/lirsacc/brokkr.svg?branch=master)](https://travis-ci.org/lirsacc/brokkr)

:construction: Work In Progress :construction:

`brokkr` is a simple distributed task queue for Rust applications. It is backed by [Redis](https://redis.io/) and heavily inspired by [rq](http://python-rq.org/).

Motivations
-----------

I needed to integrate a distributed task queue with a Rust web application and so far I found [batch-rs](https://github.com/kureuil/batch-rs) and [oppgave](https://github.com/badboy/oppgave) which solve similar problems to this. However, I wanted a solution that would not use RabbitMQ and that would offer some of the same capabilities as [rq](http://python-rq.org/) (namely monitoring and saving of results) which is one of my goto tools in Python.

The result is `brokkr`. It is backed by a Redis server and ends up a bit more complex than push / pull queues such as [ost](https://github.com/soveran/ost) (Ruby) and [oppgave](https://github.com/badboy/oppgave) in that it provides a `Worker` implementation, observability of current workers and tasks (wip, you can already query the Redis server directly) and the ability to fetch task results from another process / machine. These features come at the cost of slightly lower reliability in the face of network failure compared to the simple [reliable queues pattern](https://redis.io/commands/rpoplpush).

`brokkr` is not supposed to a port of rq though. Initially, I aim to only implement what I need for a given use case (See [TODO](#todo) for planned features) in order to keep things simple. More complex features (such as job dependencies) will be considered but are not part of the initial roadmap.

Installation
------------

> TODO

(need to release to crates.io first)

Usage
-----

> TODO

See the [examples](./examples) and docs.

TODO
----

In order of priority:

- See `TODO|WARN|REVIEW` markers in the code
- (More) tests + docs
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
- Implement monitoring (cli? web? both?)
- Implement scheduling (see celery-beat / rust-scheduler)
- Evaluate turning `Brokkr` into a trait and the current impl into `RedisBrokkr` in order to support various backends through one interface. Not all of the usual distributed task queue backends are going to be suitable but a Postgres one seems relevant (see [pq](https://github.com/malthe/pq/) in Python world). Same for `Worker` which could then be implemented to either not spawn threads or do more complex / powerful process isolation similar to what `batch-rs` does.


Development
-----------

### Running the tests

```
cargo test
```

As tests do some actual work against a Redis backend, they'll need it to be available. If you have docker installed, you can easily run a development redis server with:

```
docker run --name redis -p 6379:6379 -d redis:alpine
```

If you run the redis server on a url / port that is not `redis://127.0.0.1/`, set the `BROKKR_URL` environment variable before running the tests.

```
BROKKR_URL=redis://[:password@]host:port/db cargo test
```
