// This is pretty basic cron-based scheduler.
//
// The scheduler will attempt to push tasks to the queue at
// regular interval / times (any cron expression is supported)
// without guarantee that the tasks are executed at these times.
// If your worker pool has capacity at that time it should be close
// enough for most applications, however if the backpressure is too
// high there might be significant delay between scheduled time and
// execution time.
//
// Exact time with this kind of distributed queues is hard and you may fare
// better with a different model. A more production ready implementation could
// enqueue the tasks in a window, including before the expected time to account
// for the delay, while the execution logic should measure drift or deduplicate
// to discard stale tasks.
//
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

extern crate chrono;
extern crate job_scheduler;
extern crate pretty_env_logger;

extern crate brokkr;

use brokkr::{Brokkr, Perform};
use job_scheduler::{Job, JobScheduler};
use std::result::Result;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EveryTenSeconds {}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EveryMinuteTask {}

impl Perform for EveryTenSeconds {
  type Result = bool;
  type Error = ();
  type Context = ();

  fn process(&self, _: &Self::Context) -> Result<Self::Result, Self::Error> {
    println!(
      "Performing task every ten seconds at {:?}",
      chrono::Utc::now().naive_utc()
    );
    Ok(true)
  }
}

impl Perform for EveryMinuteTask {
  type Result = bool;
  type Error = ();
  type Context = ();

  fn process(&self, _: &Self::Context) -> Result<Self::Result, Self::Error> {
    println!(
      "Performing task on the minute at {:?}",
      chrono::Utc::now().naive_utc()
    );
    Ok(true)
  }
}

fn main() {
  pretty_env_logger::init();
  let ten_seconds_queue = Brokkr::new("example_scheduler:10".into());
  let minute_queue = Brokkr::new("example_scheduler:minute".into());

  ten_seconds_queue.clear_all().unwrap();
  minute_queue.clear_all().unwrap();

  let mut scheduler = JobScheduler::new();

  scheduler.add(Job::new("1/10 * * * * *".parse().unwrap(), || {
    debug!("Scheduling EveryTenSeconds task");
    ten_seconds_queue
      .enqueue::<EveryTenSeconds, bool>(EveryTenSeconds {})
      .unwrap();
  }));

  scheduler.add(Job::new("0 * * * * *".parse().unwrap(), || {
    debug!("Scheduling EveryMinuteTask task");
    minute_queue
      .enqueue::<EveryMinuteTask, bool>(EveryMinuteTask {})
      .unwrap();
  }));

  loop {
    debug!("Tick ({:?})", chrono::Utc::now().naive_utc());
    scheduler.tick();
    std::thread::sleep(Duration::from_millis(500));
  }
}
