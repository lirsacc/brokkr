//! brokkr is a simple distributed task queue library for Rust.
//!
//! It allows queueing tasks and processing them in the background with
//! independent workers and should be simple to integrate in existing
//! applications.
//!

#![deny(missing_docs)]

// #[macro_use]
// extern crate log;
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
use std::{panic, process, str, thread, time, fmt};

pub use errors::{Error, Result};

static PREFIX: &'static str = "brokkr";

/// Create a Redis connection.
/// The connection url is looked up from the `BROKKR_URL` environment variable
/// and defaults to `redis://127.0.0.1/`.
/// If you need a password, the url should look something like this:
/// `redis://:password@127.0.0.1/`.
///
/// # Panics
///
/// Panics if Redis endpoint cannot be reached.
///
fn connect() -> Connection {
  let url = match ::std::env::var("BROKKR_URL") {
    Ok(u) => u.to_owned(),
    _ => "redis://127.0.0.1/".to_owned(),
  };
  let client = redis::Client::open(url.as_ref()).unwrap();
  client.get_connection().unwrap()
}

/// Represent data that is safe to be passed around either through the Redis
/// backend (through Serde serialisation) or across threads for execution.
///
/// All tasks and task results structs must implement this trait, however most
/// simple structs should benefit from the provided default implementation.
pub trait Encodable
where
  Self: DeserializeOwned + Serialize + Send + Sync,
{
}

impl<T: Sized + DeserializeOwned + Serialize + Send + Sync> Encodable for T {}

/// Possible statuses of a `Worker` process.
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum WorkerStatus {
  /// The worker is waiting for jobs to be available
  Idle,
  /// The worker is processing a job
  Busy,
}

/// The different states that a `Job` can be in.
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum JobState {
  /// The job was created and in the queue
  Queued,
  /// The job has been picked by a `Worker` which is waiting for it to finish
  Started,
  /// The job has finished successfully
  Success,
  /// The job panicked
  Failed,
  /// The job timed out
  TimedOut,
}

/// A job and its metadata / state information.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job<T, R> {
  /// The unique id of this job
  pub id: Uuid,
  /// The actual application level task to be processed
  pub task: T,
  /// Result of processing the task
  pub result: Option<R>,
  /// Time when the job was created / queued
  pub created_at: NaiveDateTime,
  /// Time when processing started for this job
  pub started_at: Option<NaiveDateTime>,
  /// Time when processing finished for this job
  pub processed_at: Option<NaiveDateTime>,
  /// State of this job
  pub state: JobState,
}

impl<T: Encodable, R: Encodable> Job<T, R> {
  /// Create a job given a task object
  pub fn new(task: T) -> Self {
    Self {
      task,
      id: Uuid::new_v4(),
      result: None,
      created_at: Utc::now().naive_utc(),
      started_at: None,
      processed_at: None,
      state: JobState::Queued,
    }
  }

  fn start(&mut self) -> Result<()> {
    match self.state {
      JobState::Queued => {
        self.started_at = Some(Utc::now().naive_utc());
        self.state = JobState::Started;
        Ok(())
      }
      _ => invalid_transition!(JobState::Started, self.state),
    }
  }

  fn succeed(&mut self, r: R) -> Result<()> {
    match self.state {
      JobState::Started => {
        self.processed_at = Some(Utc::now().naive_utc());
        self.result = Some(r);
        self.state = JobState::Success;
        Ok(())
      }
      _ => invalid_transition!(JobState::Success, self.state),
    }
  }

  fn fail(&mut self) -> Result<()> {
    match self.state {
      JobState::Started => {
        self.processed_at = Some(Utc::now().naive_utc());
        self.result = None;
        self.state = JobState::Failed;
        Ok(())
      }
      _ => invalid_transition!(JobState::Failed, self.state),
    }
  }

  fn timeout(&mut self) -> Result<()> {
    match self.state {
      JobState::Started => {
        self.processed_at = Some(Utc::now().naive_utc());
        self.result = None;
        self.state = JobState::TimedOut;
        Ok(())
      }
      _ => invalid_transition!(JobState::TimedOut, self.state),
    }
  }
}

impl<T: fmt::Debug, R: fmt::Debug> fmt::Display for Job<T, R> {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "Job({}, {:?}, {:?}, {:?})", self.id.to_hyphenated().to_string(), self.state, self.task, self.result)
  }
}


fn _delete(conn: &redis::Connection, key: &str) -> Result<bool> {
  match conn.del(key)? {
    Value::Int(i) => Ok(i > 0),
    _ => unimplemented!(),
  }
}

/// Get the current machine / VM hostname.
/// This should work on most Linux and OS X machines.
fn get_hostname() -> String {
  // TODO: Couldn't initially get this to work correctly with libc which sounds
  // like a better solution.
  let output = process::Command::new("hostname").output().unwrap();
  let mut s = String::from_utf8(output.stdout).unwrap();
  s.pop(); // trailing newline.
  s
}

fn get_worker_id() -> String {
  format!("{}-{}", get_hostname(), process::id())
}

/// The `Perform` trait is used to mark a task as executable and encode the
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

  /// Process this task.
  fn process(&self, c: &Self::Context) -> Self::Result;
}

/// Interface with the Redis backend.
///
/// # Usage
///
/// ```
/// # #[macro_use]
/// # extern crate serde_derive;
/// # extern crate brokkr;
/// # use brokkr::Brokkr;
///
/// #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
/// struct Task {
///   name: String
/// }
///
/// #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
/// struct TaskResult;
///
/// # fn main() {
/// let brkkr = Brokkr::new("default".to_owned());
/// let job_id = brkkr.enqueue::<Task, TaskResult>(Task { name: "foo".to_owned() }).unwrap();
///
/// match brkkr.dequeue::<Task, TaskResult>().unwrap() {
///   Some(j) => {
///     assert_eq!(j.task, Task { name: "foo".to_owned() });
///     assert_eq!(j.result, None);
///     assert_eq!(j.id, job_id);
///    // You can now do something with it.
///   },
///   None => unimplemented!(),
/// }
///
/// // No more jobs to process
/// assert!(brkkr.dequeue::<Task, TaskResult>().unwrap().is_none());
/// # brkkr.clear_all();
/// # }
/// ```
///
pub struct Brokkr {
  /// Name of the queue.
  ///
  /// All the items stored in redis that are related to this queue will be
  /// prefixed with `brokkr:{name}:` to make it easy to inspect the redis server
  /// directly.
  pub name: String,
  queue_name: String,
  conn: Connection,
}

impl Brokkr {
  /// Create a new `Brokkr`.
  ///
  /// The Redis url is taken from the `BROKKR_URL` env variable falling
  /// back to `redis://127.0.0.1/`.
  /// You can add a password by prepending `:password` to the host.
  ///
  /// # Arguments
  ///
  /// * `name` - Name of the queue.
  ///
  pub fn new(name: String) -> Self {
    Self::with_connection(name, connect())
  }

  /// Create a new `Brokkr` with an explicit Redis connection. Use this if the
  /// default behaviour isn't suitable for your use case.
  ///
  /// # Arguments
  ///
  /// * `name` - Name of the queue.
  /// * `conn` - Already constructed Redis connection.
  ///
  /// # Usage
  ///
  /// ```
  /// # extern crate redis;
  /// # extern crate brokkr;
  /// # use redis;
  /// # use brokkr::Brokkr;
  /// let client = redis::Client::open("redis://127.0.0.1/").unwrap();
  /// let con = client.get_connection().unwrap();
  /// let brkkr = Brokkr::with_connection("default".to_owned(), con);
  /// ```
  ///
  pub fn with_connection(name: String, conn: Connection) -> Self {
    let queue_name = format!("{}:{}", PREFIX, name);
     Self {
      name,
      queue_name,
      conn,
    }
  }

  fn worker_key(&self, id: &str) -> String {
    format!("{}:{}:worker:{}", PREFIX, &self.name, id)
  }

  fn job_key(&self, id: &Uuid) -> String {
    format!("{}:{}:job:{}", PREFIX, &self.name, id)
  }

  /// Get the number of remaining tasks for this queue.
  pub fn queue_size(&self) -> u64 {
    self.conn.llen(&self.queue_name).unwrap_or(0)
  }

  /// Delete all remaining entries in the task queue.
  pub fn clear_queue(&self) -> Result<()> {
    _delete(&self.conn, &self.queue_name)?;
    Ok(())
  }

  /// Delete all keys related to this queue, including workers semaphores,
  /// task results and failures.
  ///
  /// Only use this if you are sure that all workers have been correctly
  /// stopped and results gathered as this might leave other processes in an
  /// inconcistent state.
  ///
  pub fn clear_all(&self) -> Result<()> {
    // WARN: This may fail when there are too many keys, maybe a loop can work.
    let res = cmd("EVAL")
      .arg("redis.call('del', '_', unpack(redis.call('keys', ARGV[1])))")
      .arg(0)
      .arg(format!("{}:{}:*", PREFIX, self.queue_name.to_owned()))
      .query(&self.conn)?;
    match res {
      Value::Nil => Ok(()),
      _ => unimplemented!(),
    }
  }

  /// Push a task to the queue.
  ///
  /// Teh result is the resulting job's uuid which can be used for later
  /// processing such a retrieving its result or failure information with
  /// `fetch_job`.
  pub fn enqueue<T: Encodable, R: Encodable>(&self, task: T) -> Result<Uuid> {
    let job: Job<T, R> = Job::new(task);
    self
      .conn
      .lpush(&self.queue_name, serde_json::to_string(&job)?)?;
    Ok(job.id)
  }

  /// Get the oldest job in the queue.
  pub fn dequeue<T: Encodable, R: Encodable>(&self) -> Result<Option<Job<T, R>>> {
    match self.conn.rpop(&self.queue_name)? {
      Value::Data(ref v) => {
        let job = serde_json::from_slice::<Job<T, R>>(v)?;
        Ok(Some(job))
      }
      Value::Nil => Ok(None),
      _ => unimplemented!(),
    }
  }

  fn send_worker_heartbeat(&self, worker_id: &str) -> Result<NaiveDateTime> {
    let now = Utc::now();
    cmd("HSET")
      .arg(&self.worker_key(worker_id))
      .arg("beat")
      .arg(now.to_rfc3339())
      .query(&self.conn)?;
    Ok(now.naive_utc())
  }

  fn set_worker_job_and_status<T: Encodable, R: Encodable>(
    &self,
    worker_id: &str,
    status: WorkerStatus,
    job: Option<&Job<T, R>>,
  ) -> Result<()> {
    cmd("HMSET")
      .arg(&self.worker_key(worker_id))
      .arg("status")
      .arg(format!("{:?}", status))
      .arg("job")
      .arg(serde_json::to_string(&job)?).query(&self.conn)?;
    Ok(())
  }

  /// Fetch a specific job by id.
  pub fn fetch_job<T: Encodable, R: Encodable>(&self, job_id: &Uuid) -> Result<Option<Job<T, R>>> {
    match self.conn.get(&self.job_key(&job_id))? {
      Value::Data(ref v) => {
        let job = serde_json::from_slice::<Job<T, R>>(v)?;
        Ok(Some(job))
      }
      Value::Nil => Ok(None),
      _ => unimplemented!(),
    }
  }

  fn set_job_result<T: Encodable, R: Encodable>(&self, job: &Job<T, R>) -> Result<()> {
    self.conn.set(&self.job_key(&job.id), serde_json::to_string(job)?)?;
    Ok(())
  }

  fn delete_job_result<T: Encodable, R: Encodable>(&self, job: &Job<T, R>) -> Result<()> {
    _delete(&self.conn, &self.job_key(&job.id))?;
    Ok(())
  }

  fn deregister_worker(&self, worker_id: &str) -> Result<()> {
    _delete(&self.conn, &self.worker_key(worker_id))?;
    Ok(())
  }
}

/// Worker process implementation.
///
/// A worker will register itself with the backend and send regular heartbeat
/// messages / status update for monitoring.
///
pub struct Worker<'a, T: Perform + Encodable + Clone> {
  brokkr: &'a Brokkr,
  id: String,
  // TODO: Using interior mutability here to not expose a mutable reference to
  // the user. Might not be necessary.
  status: Cell<WorkerStatus>,
  context: T::Context,
  timeout: time::Duration,
  period: time::Duration,
  heartbeat_period: time::Duration,
  last_heartbeat: Cell<time::Instant>,
}

impl<'a, T: Encodable + Perform + Clone> Worker<'a, T> {
  /// Create a worker process.
  ///
  /// # Arguments
  ///
  /// * `brokkr`  - The broker to use to communicate with the Redis backend.
  /// * `ctx`     - Context object. Use this for things which have costly
  ///               initialization but should be available to all tasks during
  ///               procssing.
  /// * `timeout` - Job timeout. All tasks taking more than this duration to
  ///               process will be marked as failed.
  ///
  ///
  pub fn new(brokkr: &'a Brokkr, ctx: T::Context, timeout: time::Duration) -> Self {
    let period = time::Duration::from_millis(100);
    assert!(timeout >= period);
    let s = Self {
      brokkr,
      id: get_worker_id().to_owned(),
      status: Cell::new(WorkerStatus::Idle),
      context: ctx,
      timeout,
      period,
      heartbeat_period: time::Duration::from_millis(500),
      last_heartbeat: Cell::new(time::Instant::now()),
    };
    s.start();
    s
  }

  fn start(&self) {
    self
      .brokkr
      .set_worker_job_and_status::<T, T::Result>(&self.id, self.status.get(), None)
      .unwrap();
    self.send_heartbeat();
  }

  /// Send an heartbeat message to the Redis backend.
  fn send_heartbeat(&self) {
    self.brokkr.send_worker_heartbeat(&self.id).unwrap();
    self.last_heartbeat.set(time::Instant::now());
  }

  /// The actual work function.
  ///
  /// This will call `Perform.process` in a separate thread, unwind any panic
  /// as well as detect timeouts and update the Redis keys accordingly through
  /// the brokkr. This is meant to never crash and contain the crash of the
  /// provided process implementation.
  fn process(&self, job: &mut Job<T, T::Result>) {
    self.status.set(WorkerStatus::Busy);
    self.send_heartbeat();

    // TODO: Can this work without Clone?
    let task = job.task.clone();
    let ctx = self.context.clone();

    job.start().unwrap();

    self
      .brokkr
      .set_worker_job_and_status(&self.id, self.status.get(), Some(&job))
      .unwrap();

    let (sender, receiver) = mpsc::channel();
    let start = time::Instant::now();
    let mut elapsed = time::Duration::from_millis(0);

    let _ = thread::spawn(move || {
      let cr = panic::catch_unwind(|| task.process(&ctx));
      // In case of error, the timeout was reached or parent was terminated
      // and the channel is not available anymore. This should be safe to ignore.
      sender.send(cr).unwrap_or(());
    });

    // TODO: Make sure errors are handled correctly (save as much failure info
    // as possible for reporting)
    loop {
      if elapsed > self.timeout {
        job.timeout().unwrap();
        break;
      }

      if let Ok(r) = receiver.recv_timeout(self.period) { match r {
        Ok(result) => {
          job.succeed(result).unwrap();
          break;
        }
        Err(err) => {
          println!("Failed {:?}", err);
          job.fail().unwrap();
          break;
        }
      }}

      if time::Instant::now() - self.last_heartbeat.get() > self.heartbeat_period {
        self.send_heartbeat();
      }
      elapsed = time::Instant::now() - start;
    }
    self.status.set(WorkerStatus::Idle);
    // TODO: Do both of these in one transaction.
    self.brokkr.set_job_result(&job).unwrap();
    self
      .brokkr
      .set_worker_job_and_status::<T, T::Result>(&self.id, self.status.get(), None)
      .unwrap();
    self.send_heartbeat();
  }

  /// Process a single task from the queue and exit. Exits immediatly if the
  /// queue is empty.
  ///
  pub fn process_one(&self) {
    match self.brokkr.dequeue::<T, T::Result>().unwrap() {
      None => (),
      Some(ref mut job) => {
        self.process(job);
      }
    };
  }

  /// Process tasks in a loop by polling the queue every `wait_timeout` ms.
  ///
  /// # Arguments
  ///
  /// * `wait_timeout` - Timeout to wait between polls (in ms)
  ///
  pub fn process_many(&self, wait_timeout: u64) {
    let wait_dur = time::Duration::from_millis(wait_timeout);
    loop {
      self.process_one();
      thread::sleep(wait_dur);
    }
  }
}

// This should make sure that in the large majority of cases the worker
// deregisters itself. For the casee where this doesn't trigger using the
// heartbeat timstamp as some kind of timeout is a good fallback strategy.
impl<'a, T: Encodable + Perform + Clone> Drop for Worker<'a, T> {
  fn drop(&mut self) {
    self.brokkr.deregister_worker(&self.id).unwrap();
  }
}
