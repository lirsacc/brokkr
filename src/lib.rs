//! brokkr is a simple distributed task queue library for Rust.
//!
//! It allows queueing tasks and processing them in the background with
//! independent workers and should be simple to integrate in existing
//! applications and deployments.
//!

#![deny(missing_docs)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

extern crate chrono;
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

use std::{cell::Cell, fmt, panic, process, str, sync::mpsc, thread, time};

pub use errors::{Error, Result};

static PREFIX: &'static str = "brokkr";

/// Create a Redis connection.
/// The connection url is looked up from the `BROKKR_URL` environment variable
/// and defaults to `redis://127.0.0.1/`.
/// If you need a password, the url should look something like this:
/// `redis://:password@127.0.0.1/`.
///
fn connect() -> Result<Connection> {
  let url = match ::std::env::var("BROKKR_URL") {
    Ok(u) => u.to_owned(),
    _ => "redis://127.0.0.1/".to_owned(),
  };
  let client = redis::Client::open(url.as_ref())?;
  Ok(client.get_connection()?)
}

/// Represent data that is safe to be passed around either through the Redis
/// backend (through Serde serialisation) or across threads for execution.
///
/// All tasks and task results structs must implement this trait, however most
/// simple structs should benefit from the provided default implementation.
pub trait Encodable
where
  Self: DeserializeOwned + Serialize + Send + Sync + Sized,
{
}

impl<T: DeserializeOwned + Serialize + Send + Sync + Sized> Encodable for T {}

/// Possible statuses of a `Worker` process.
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum WorkerStatus {
  /// The worker is waiting for jobs to be available.
  Idle,
  /// The worker is processing a job.
  Busy,
}

/// The different states that a `Job` can be in.
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum JobState {
  /// The job was created and in the queue.
  Queued,
  /// The job has been picked by a `Worker` which is waiting for it to finish.
  Started,
  /// The job has finished successfully.
  Success,
  /// The job failed in an expected way.
  Failed,
  /// The job panicked.
  Panicked,
  /// The job timed out.
  TimedOut,
}

/// A job and its metadata / state information.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job<T, R> {
  /// The unique id of this job.
  pub id: Uuid,
  /// The actual application level task to be processed.
  pub task: T,
  /// Result of processing the task.
  pub result: Option<R>,
  /// Failure information, that's the panic payload in case of panic or the.
  /// reported error in case of handled error.
  pub failure_info: Option<String>,
  /// Time when the job was created / queued.
  pub created_at: NaiveDateTime,
  /// Time when processing started for this job.
  pub started_at: Option<NaiveDateTime>,
  /// Time when processing finished for this job.
  pub processed_at: Option<NaiveDateTime>,
  /// State of this job.
  pub state: JobState,
}

impl<T: Encodable, R: Encodable> Job<T, R> {
  /// Create a job given a task object.
  pub fn new(task: T) -> Self {
    Self {
      task,
      id: Uuid::new_v4(),
      result: None,
      failure_info: None,
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

  fn panic(&mut self, failure_info: &str) -> Result<()> {
    match self.state {
      JobState::Started => {
        self.processed_at = Some(Utc::now().naive_utc());
        self.result = None;
        self.failure_info = Some(failure_info.to_owned());
        self.state = JobState::Panicked;
        Ok(())
      }
      _ => invalid_transition!(JobState::Panicked, self.state),
    }
  }

  fn fail(&mut self, failure_info: &str) -> Result<()> {
    match self.state {
      JobState::Started => {
        self.processed_at = Some(Utc::now().naive_utc());
        self.result = None;
        self.failure_info = Some(failure_info.to_owned());
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

  fn has_failed(&self) -> bool {
    match self.state {
      JobState::Failed | JobState::Panicked => true,
      _ => false,
    }
  }
}

impl<T: fmt::Debug, R: fmt::Debug> fmt::Display for Job<T, R> {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "Job({}, {:?}, {:?}, {:?})",
      self.id.to_hyphenated().to_string(),
      self.state,
      self.task,
      self.result
    )
  }
}

fn delete(conn: &redis::Connection, key: &str) -> Result<bool> {
  match conn.del(key)? {
    Value::Int(i) => Ok(i > 0),
    _ => unimplemented!(),
  }
}

fn delete_prefix(conn: &redis::Connection, prefix: &str) -> Result<()> {
  // WARN: This may fail when there are too many keys, maybe a loop can work.
  let res = cmd("EVAL")
    .arg("redis.call('del', '_', unpack(redis.call('keys', ARGV[1])))")
    .arg(0)
    .arg(format!("{}*", prefix))
    .query(conn)?;
  match res {
    Value::Nil => Ok(()),
    _ => unimplemented!(),
  }
}

// Get the current machine / VM hostname.
// This should work on most Linux and OS X machines.
// TODO: Couldn't initially get this to work correctly with libc which sounds
// like a better solution.
fn get_hostname() -> String {
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
  Self: Encodable + panic::RefUnwindSafe + 'static,
{
  /// The result type for this task.
  type Result: Encodable + panic::RefUnwindSafe + 'static;
  /// The error type for this task.
  type Error: Encodable + panic::RefUnwindSafe + 'static;
  /// The type of the context value that will be given to this job's handler.
  /// This should be used to share long-lived objects such as database
  /// connections, global variables, etc.
  type Context: Clone + Send + Sync + panic::RefUnwindSafe + 'static;

  /// Processing logic for this task.
  /// Returning the `Result` type is considered a success for the task while
  /// returning the `Error` type will mark the job as failed, including the
  /// serialised error in the job metadata.
  ///
  /// You shoud try to avoid surfacing panics as much as possible, however if
  /// this function does panic, the `Worker` will catch it and mark the job as
  /// panicked.
  fn process(&self, c: &Self::Context) -> ::std::result::Result<Self::Result, Self::Error>;
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
/// let brkkr = Brokkr::new("example_queue".to_owned());
/// let job_id = brkkr.enqueue::<Task, TaskResult>(Task { name: "foo".to_owned() }).unwrap();
///
/// match brkkr.dequeue::<Task, TaskResult>().unwrap() {
///   Some(j) => {
///     assert_eq!(j.task, Task { name: "foo".to_owned() });
///     assert_eq!(j.result, None);
///     assert_eq!(j.id, job_id);
///    // You can now do something with it.
///   },
///   None => panic!("Someone stole my job"),
/// }
///
/// // No more jobs to process
/// assert!(brkkr.dequeue::<Task, TaskResult>().unwrap().is_none());
/// # brkkr.clear_all();
/// # }
/// ```
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
  pub fn new(name: String) -> Self {
    Self::with_connection(name, connect().unwrap())
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
  pub fn with_connection(name: String, conn: Connection) -> Self {
    let queue_name = format!("{}:{}", PREFIX, name);
    Self {
      name,
      queue_name,
      conn,
    }
  }

  fn worker_key(&self, id: &str) -> String {
    format!("{}:worker:{}", &self.queue_name, id)
  }

  fn job_key(&self, id: &Uuid) -> String {
    format!("{}:job:{}", &self.queue_name, id)
  }

  /// Get the number of remaining tasks for this queue.
  pub fn queue_size(&self) -> u64 {
    self.conn.llen(&self.queue_name).unwrap_or(0)
  }

  /// Delete all remaining entries in the task queue.
  pub fn clear_queue(&self) -> Result<()> {
    delete(&self.conn, &self.queue_name)?;
    Ok(())
  }

  /// Delete all keys related to this queue, including workers semaphores,
  /// task results and failures.
  ///
  /// Only use this if you are sure that all workers have been correctly
  /// stopped and results gathered as this might leave other processes in an
  /// inconcistent state.
  pub fn clear_all(&self) -> Result<()> {
    delete_prefix(&self.conn, &self.queue_name)
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
      .arg(serde_json::to_string(&job)?)
      .query(&self.conn)?;
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

  fn set_job_result<T: Encodable, R: Encodable>(
    &self,
    job: &Job<T, R>,
    ttl: &time::Duration,
  ) -> Result<()> {
    // For failures, we do not set a ttl as the user is supposed to handle them.
    if job.has_failed() {
      self
        .conn
        .set(&self.job_key(&job.id), serde_json::to_string(job)?)?;
    } else {
      self.conn.set_ex(
        &self.job_key(&job.id),
        serde_json::to_string(job)?,
        ttl.as_secs() as usize,
      )?;
    }
    Ok(())
  }

  /// Delete result of a specific job from the backend
  pub fn delete_job_result(&self, job_id: &Uuid) -> Result<()> {
    delete(&self.conn, &self.job_key(&job_id))?;
    Ok(())
  }

  /// Delete results of all jobs in the queue from the backend
  pub fn delete_all_job_results(&self) -> Result<()> {
    delete_prefix(&self.conn, &format!("{}:job:", &self.queue_name))
  }

  fn deregister_worker(&self, worker_id: &str) -> Result<()> {
    delete(&self.conn, &self.worker_key(worker_id))?;
    Ok(())
  }
}

/// Worker process.
///
/// The worker is responsible for consuming a single queue and processing tasks
/// as they become available. It will will register itself with the backend and
/// send regular status update for monitoring.
///
/// The worker itself does not take care of concurrency. In order to process
/// multiple tasks on the same machines you need to start multiple worker
/// processes yourself.
///
/// # Usage
///
///
/// ```rust,no_run
/// # #[macro_use]
/// # extern crate serde_derive;
/// # extern crate brokkr;
/// use std::time::Duration;
/// use brokkr::{Brokkr, Worker, Perform};
///
/// #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
/// struct Task {
///   name: String
/// }
///
/// #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
/// struct TaskResult;
///
/// impl Perform for Task {
///   type Result = String;
///   type Error = ();
///   type Context = ();
///
///   fn process(&self, _: &Self::Context) -> Result<Self::Result, Self::Error> {
///     Ok(format!("Done: {}", self.name).to_owned())
///   }
/// }
/// # fn main() {
///
/// let brokkr = Brokkr::new("default".into());
///
/// let worker: Worker<Task> = Worker::new(
///   &brokkr,
///   (),
///   Duration::from_millis(500),
///   Duration::from_secs(10),
/// );
///
/// worker.process_many(Duration::from_secs(1));
/// # }
/// ```
pub struct Worker<'a, T: Perform + Clone> {
  brokkr: &'a Brokkr,
  id: String,
  // REVIEW: Using interior mutability here to not expose a mutable reference to
  // the user. Might not be necessary.
  status: Cell<WorkerStatus>,
  context: T::Context,
  timeout: time::Duration,
  period: time::Duration,
  heartbeat_period: time::Duration,
  result_ttl: time::Duration,
  // REVIEW: Interior mutability, see above.
  last_heartbeat: Cell<time::Instant>,
}

impl<'a, T: Perform + Clone> Worker<'a, T> {
  /// Create a worker process.
  ///
  /// # Arguments
  ///
  /// * `brokkr`      - The broker to use to communicate with the Redis backend.
  /// * `ctx`         - Context object. Use this for things which have costly
  ///                   initialization but should be available to all tasks
  ///                   during procssing.
  /// * `timeout`     - Job timeout. All tasks taking more than this duration
  ///                   to process will be marked as failed.
  /// * `result_ttl`  - Job result ttl.
  pub fn new(
    brokkr: &'a Brokkr,
    ctx: T::Context,
    timeout: time::Duration,
    result_ttl: time::Duration,
  ) -> Self {
    Self {
      brokkr,
      id: get_worker_id().to_owned(),
      status: Cell::new(WorkerStatus::Idle),
      context: ctx,
      timeout,
      period: time::Duration::from_millis(100),
      result_ttl,
      heartbeat_period: time::Duration::from_millis(500),
      last_heartbeat: Cell::new(time::Instant::now()),
    }
  }

  /// Send an heartbeat message to the Redis backend.
  fn send_heartbeat(&self) {
    if time::Instant::now() - self.last_heartbeat.get() > self.heartbeat_period {
      self.brokkr.send_worker_heartbeat(&self.id).unwrap();
      self.last_heartbeat.set(time::Instant::now());
    }
  }

  /// The actual work function.
  ///
  /// This will call `Perform.process` in a separate thread, unwind any panic
  /// as well as detect timeouts and update the Redis keys accordingly through
  /// the brokkr. This is meant to never crash and contain the crash of the
  /// provided process implementation.
  fn process(&self, job: &mut Job<T, T::Result>) {
    debug!("[Brokkr:Worker:{}] Process Job {}.", self.id, job.id);
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
      // and the channel is not available anymore.
      // This should be safe to ignore.
      sender.send(cr).unwrap_or(());
    });

    loop {
      if elapsed > self.timeout {
        job.timeout().unwrap();
        break;
      }

      if let Ok(r) = receiver.recv_timeout(self.period) {
        match r {
          Ok(process_result) => {
            match process_result {
              Ok(result) => {
                job.succeed(result).unwrap();
              }
              Err(error) => {
                job.fail(&serde_json::to_string(&error).unwrap()).unwrap();
              }
            };
          }
          Err(e) => {
            // TODO: Haven't found a clean  way to get a full backtrace out of
            // the spawned thread yet but it would be a nice addition.
            if let Some(e) = e.downcast_ref::<&'static str>() {
              job.panic(e).unwrap();
            } else {
              job.panic("[Brokkr] An unknown error occured").unwrap();
            }
          }
        }
        break;
      }

      self.send_heartbeat();
      elapsed = time::Instant::now() - start;
    }
    self.status.set(WorkerStatus::Idle);
    // TODO: Do both of these in one transaction.
    self.brokkr.set_job_result(&job, &self.result_ttl).unwrap();
    self
      .brokkr
      .set_worker_job_and_status::<T, T::Result>(&self.id, self.status.get(), None)
      .unwrap();
    self.send_heartbeat();
  }

  /// Process a single task from the queue and exit. Exits immediatly if the
  /// queue is empty.
  ///
  /// Use this to build your own processing loop if `process_many` doesn't fit.
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
  /// * `wait_timeout` - Timeout in ms to wait between polls to the queue.
  ///
  pub fn process_many(&self, wait_timeout: time::Duration) {
    debug!("[Brokkr:Worker:{}] Starting processing loop.", self.id);
    self
      .brokkr
      .set_worker_job_and_status::<T, T::Result>(&self.id, self.status.get(), None)
      .unwrap();

    self.send_heartbeat();

    loop {
      self.process_one();
      thread::sleep(wait_timeout);
      self.send_heartbeat();
    }
  }
}

// This should make sure that in the large majority of cases the worker
// deregisters itself. For the casee where this doesn't trigger using the
// heartbeat timstamp as some kind of timeout is a good fallback strategy.
impl<'a, T: Perform + Clone> Drop for Worker<'a, T> {
  fn drop(&mut self) {
    self.brokkr.deregister_worker(&self.id).unwrap();
  }
}
