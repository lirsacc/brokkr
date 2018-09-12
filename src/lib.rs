//! brokkr - Minimalist distributed task queue for Rust.

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

extern crate chrono;
extern crate error_chain;
extern crate redis;
extern crate serde;
extern crate serde_json;
extern crate uuid;

#[macro_use]
mod errors;

use chrono::{NaiveDateTime, Utc};
use redis::{cmd, Commands, Connection, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use std::cell::Cell;
use std::sync::mpsc;
use std::{panic, process, str, thread, time};

use errors::{Error, Result};

static PREFIX: &'static str = "brokkr";

/// Create a Redis connection.
/// The url is looked up from the `BROKER_URL` env variable and defaults to
/// redis://127.0.0.1/.
///
/// # Panics
///
/// Panics if Redis endpoint cannot be reached.
///
fn connect() -> Connection {
  let broker_url = match ::std::env::var("BROKER_URL") {
    Ok(u) => u.to_owned(),
    _ => "redis://127.0.0.1/".to_owned(),
  };
  let client = redis::Client::open(broker_url.as_ref()).unwrap();
  client.get_connection().unwrap()
}

/// A Task represent a unit of work that can be queued and dequeued for
/// processing.
/// Most structs implementing DeserializeOwned + Serialize should automatically
/// implement this trait.
pub trait Encodable
where
  Self: DeserializeOwned + Serialize + Send + Sync,
{
}

impl<T: Sized + DeserializeOwned + Serialize + Send + Sync> Encodable for T {}

fn _delete(conn: &redis::Connection, key: &str) -> Result<bool> {
  match conn.del(key)? {
    Value::Int(i) => Ok(i > 0),
    _ => unimplemented!(),
  }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum WorkerStatus {
  IDLE,
  BUSY,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobStatus {
  QUEUED,
  STARTED,
  SUCCESS,
  FAILED,
}

/// A `Job` is brokkr's internal representation of a task and its lifecycle.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job<T, R> {
  pub id: Uuid,
  pub task: T,
  pub result: Option<R>,
  pub created_at: NaiveDateTime,
  pub started_at: Option<NaiveDateTime>,
  pub processed_at: Option<NaiveDateTime>,
  pub state: JobStatus,
}

impl<T: Encodable + Perform> Job<T, T::Result> {
  pub fn new(task: T) -> Self {
    Self {
      task: task,
      id: Uuid::new_v4(),
      result: None,
      created_at: Utc::now().naive_utc(),
      started_at: None,
      processed_at: None,
      state: JobStatus::QUEUED,
    }
  }

  pub fn start(&mut self) -> Result<()> {
    match self.state {
      JobStatus::QUEUED => {
        self.started_at = Some(Utc::now().naive_utc());
        self.state = JobStatus::STARTED;
        Ok(())
      }
      _ => invalid_transition!(JobStatus::STARTED, self.state),
    }
  }

  pub fn finish(&mut self, r: Option<T::Result>) -> Result<()> {
    let target = match r {
      Some(_) => JobStatus::SUCCESS,
      None => JobStatus::FAILED,
    };
    match self.state {
      JobStatus::STARTED => {
        self.processed_at = Some(Utc::now().naive_utc());
        self.result = r;
        self.state = target;
        Ok(())
      }
      _ => invalid_transition!(target, self.state),
    }
  }
}

/// Get the current machine / VM hostname.
/// This should work on most Linux and OS X machines.
fn get_hostname() -> String {
  // TODO: Couldn't initially get this to work correctly with libc which sounds
  // like a better solution.
  let output = process::Command::new("hostname").output().unwrap();
  let mut s = String::from_utf8(output.stdout).unwrap();
  s.pop(); // pop '\n'
  s
}

fn get_worker_id() -> String {
  format!("{}-{}", get_hostname(), process::id())
}

/// A `Brokkr` instance is in charge of interacting with the Redis backend.
pub struct Brokkr {
  pub name: String,
  queue_name: String,
  conn: Connection,
}

impl Brokkr {
  pub fn new(name: String) -> Self {
    let queue_name = format!("{}:{}", PREFIX, name);

    Self {
      name: name,
      queue_name: queue_name,
      conn: connect(),
    }
  }

  pub fn worker_key(&self, id: &str) -> String {
    format!("{}:{}:worker:{}", PREFIX, &self.name, id)
  }

  /// Get the number of remaining tasks for this queue.
  pub fn len(&self) -> u64 {
    self.conn.llen(&self.queue_name).unwrap_or(0)
  }

  /// Delete all remaining entries in the task queue.
  pub fn clear(&self) -> Result<()> {
    _delete(&self.conn, &self.queue_name)?;
    Ok(())
  }

  /// Delete all keys related to this queue, including workers semaphores,
  /// task results and failures.
  pub fn clear_all(&self) -> Result<()> {
    // WARN: This may fail when there are too many keys, maybe a loop can work.
    let res = cmd("EVAL")
      .arg("redis.call('del', '_', unpack(redis.call('keys', ARGV[1])))")
      .arg(0)
      .arg(format!("brokkr:{}:*", self.queue_name.to_owned()))
      .query(&self.conn)?;
    match res {
      Value::Nil => Ok(()),
      _ => unimplemented!(),
    }
  }

  /// Push a job to the queue.
  pub fn enqueue<T: Encodable + Perform>(&self, task: T) -> Result<Job<T, T::Result>> {
    let job = Job::new(task);
    self
      .conn
      .lpush(&self.queue_name, serde_json::to_string(&job)?)?;
    Ok(job)
  }

  /// Get the oldest task in the queue and mark it as in progress.
  pub fn dequeue<T: Encodable + Perform>(&self) -> Result<Option<Job<T, T::Result>>> {
    match self.conn.rpop(&self.queue_name)? {
      Value::Data(ref v) => {
        let job = serde_json::from_slice::<Job<T, T::Result>>(v)?;
        Ok(Some(job))
      }
      Value::Nil => Ok(None),
      _ => unimplemented!(),
    }
  }

  pub fn send_worker_heartbeat(&self, worker_id: &str) -> Result<NaiveDateTime> {
    let now = Utc::now();
    cmd("HSET")
      .arg(&self.worker_key(worker_id))
      .arg("beat")
      .arg(now.to_rfc3339())
      .query(&self.conn)?;
    Ok(now.naive_utc())
  }

  pub fn set_worker_job_and_status<T: Encodable + Perform>(
    &self,
    worker_id: &str,
    status: &WorkerStatus,
    job: Option<&Job<T, T::Result>>,
  ) -> Result<()> {
    cmd("HMSET")
      .arg(&self.worker_key(worker_id))
      .arg("status")
      .arg(format!("{:?}", status))
      .arg("job")
      .arg(match job {
        None => "".to_owned(),
        Some(j) => serde_json::to_string(&job)?,
      }).query(&self.conn)?;
    Ok(())
  }

  pub fn deregister_worker(&self, worker_id: &str) -> Result<()> {
    _delete(&self.conn, &self.worker_key(worker_id))?;
    Ok(())
  }
}

/// The `Perform` trait is used to mark a `Task` as executable and encode the
/// logic associated with this task.
pub trait Perform
where
  Self: panic::RefUnwindSafe + 'static,
{
  /// The result type for this task.
  type Result: Encodable + panic::RefUnwindSafe + 'static;
  /// The type of the context value that will be given to this job's handler.
  /// This should be used to share long-lived objects such as database
  /// connections, global variables, etc.
  type Context: Clone + Send + Sync + panic::RefUnwindSafe + 'static;

  /// Process the task
  fn process(&self, c: &Self::Context) -> Self::Result;
}

/// A `Worker` is responsible for performing work coming from a queue.
pub struct Worker<'a, T: Perform + Encodable> {
  brokkr: &'a Brokkr,
  id: String,
  status: Cell<WorkerStatus>,
  context: T::Context,
  timeout: time::Duration,
  period: time::Duration,
  heartbeat_period: time::Duration,
  last_heartbeat: Cell<time::Instant>,
}

impl<'a, T: Encodable + Perform> Worker<'a, T> {
  pub fn new(brokkr: &'a Brokkr, ctx: T::Context, timeout: time::Duration) -> Self {
    let period = time::Duration::from_millis(100);
    assert!(timeout >= period);
    let s = Self {
      brokkr: brokkr,
      id: get_worker_id().to_owned(),
      status: Cell::new(WorkerStatus::IDLE),
      context: ctx,
      timeout: timeout,
      period: period,
      heartbeat_period: time::Duration::from_millis(500),
      last_heartbeat: Cell::new(time::Instant::now()),
    };
    s.start();
    s
  }

  pub fn start(&self) {
    self
      .brokkr
      .set_worker_job_and_status::<T>(&self.id, &self.status.get(), None)
      .unwrap();
    self.send_heartbeat();
  }

  /// Send an heartbeat message to the Redis backend.
  pub fn send_heartbeat(&self) {
    self.brokkr.send_worker_heartbeat(&self.id).unwrap();
    self.last_heartbeat.set(time::Instant::now());
  }

  /// The actual work function.
  /// This will call `Perform.process` in a separate thread, unwind any panic
  /// as well as detect timeouts and update the Redis keys accordingly through
  /// the brokkr. This is meant to never crash and contain the crash of the
  /// provided process implementation.
  pub fn process(&self, job: Job<T, T::Result>) {
    self.status.set(WorkerStatus::BUSY);
    self.send_heartbeat();
    self
      .brokkr
      .set_worker_job_and_status::<T>(&self.id, &self.status.get(), Some(&job))
      .unwrap();

    let (sender, receiver) = mpsc::channel();
    // TODO: That `clone` shouldn't be necessary I feel...
    let ctx = self.context.clone();
    let start = time::Instant::now();
    let mut elapsed = time::Duration::from_millis(0);

    let _ = thread::spawn(move || {
      let cr = panic::catch_unwind(|| job.task.process(&ctx));
      // In case of error, the timeout was reached or parent was terminated
      // and the channel is not available anymore. This should be safe to ignore.
      sender.send(cr).unwrap_or(());
    });

    // TODO: Make sure errors are handled correctly and we push jobs to the
    // correct Failed / Done queues.
    loop {
      if elapsed > self.timeout {
        println!("TIMEOUT REACHED");
        break;
      }

      match receiver.recv_timeout(self.period) {
        Ok(r) => match r {
          Ok(_) => {
            println!("RESULT OK");
            break;
          }
          Err(err) => {
            println!("FAILED {:?}", err);
            break;
          }
        },
        Err(_) => {}
      }

      if time::Instant::now() - self.last_heartbeat.get() > self.heartbeat_period {
        self.send_heartbeat();
      }
      elapsed = time::Instant::now() - start;
    }

    self.status.set(WorkerStatus::IDLE);
    self
      .brokkr
      .set_worker_job_and_status::<T>(&self.id, &self.status.get(), None)
      .unwrap();
    self.send_heartbeat();
  }

  /// Process one job and exit.
  pub fn process_one(&self) {
    match self.brokkr.dequeue::<T>().unwrap() {
      None => (),
      Some(job) => {
        self.process(job);
      }
    };
  }
}

impl<'a, T: Encodable + Perform> Drop for Worker<'a, T> {
  fn drop(&mut self) {
    self.brokkr.deregister_worker(&self.id).unwrap();
  }
}
