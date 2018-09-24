brokkr
======

[![Build Status](https://travis-ci.org/lirsacc/brokkr.svg?branch=master)](https://travis-ci.org/lirsacc/brokkr)
[![Crates.io](https://img.shields.io/crates/v/brokkr.svg)](https://crates.io/crates/brokkr)


:construction: Work In Progress :construction:

`brokkr` is a simple distributed task queue for Rust applications. It is backed by [Redis](https://redis.io/) and heavily inspired by [rq](http://python-rq.org/).

Motivations
-----------

I needed to integrate a distributed task queue with a Rust web application and so far I found [batch-rs](https://github.com/kureuil/batch-rs) and [oppgave](https://github.com/badboy/oppgave) which solve similar problems to this. However, I wanted a solution that would not use RabbitMQ and that would offer some of the same capabilities as [rq](http://python-rq.org/) (namely monitoring and saving of results) which is one of my goto tools in Python.

The result is `brokkr`. It is backed by a Redis server and ends up a bit more complex than push / pull queues such as [ost](https://github.com/soveran/ost) (Ruby) and [oppgave](https://github.com/badboy/oppgave) in that it provides a `Worker` implementation, observability of current workers and tasks (wip, you can already query the Redis server directly) and the ability to fetch task results from another process / machine. These features come at the cost of slightly lower reliability in the face of network failure compared to the simple [reliable queues pattern](https://redis.io/commands/rpoplpush).

`brokkr` is not supposed to a port of rq though. Initially, I aim to only implement what I need for a given use case (See [TODO](./TODO.md) for planned features) in order to keep things simple. More complex features (such as job dependencies) will be considered but are not part of the initial roadmap.

Installation & Usage
--------------------

To use brokkr, add this to your Cargo.toml:

```toml
[dependencies]
brokkr = "0.1"
```

### Examples

> TODO

See the [examples](./examples) and documentation in the meantime.


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
