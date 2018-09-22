#[macro_use]
extern crate serde_derive;

extern crate pretty_env_logger;
extern crate uuid;

extern crate brokkr;

use brokkr::{Brokkr, Perform, Worker};
use std::result::Result;
use std::thread::sleep;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Task {
  id: u64,
  msg: String,
}

impl Perform for Task {
  type Result = u64;
  type Error = String;
  type Context = ();

  fn process(&self, _: &Self::Context) -> Result<Self::Result, Self::Error> {
    println!("Performing task id={}, msg={}", self.id, self.msg);
    if self.id == 3 {
      return Err("I don't like 3.".to_owned());
    }
    if self.id == 5 {
      panic!("Not 5!");
    }
    if self.id == 8 {
      sleep(Duration::from_millis(1000));
    }
    Ok(self.id + 100)
  }
}

fn main() {
  pretty_env_logger::init();

  let brokkr = Brokkr::new("default".into());

  let w: Worker<Task> = Worker::new(
    &brokkr,
    (),
    Duration::from_millis(500),
    Duration::from_secs(10),
  );

  let mut job_ids: Vec<uuid::Uuid> = vec![];

  // Clean up from previous runs.
  brokkr.clear_all().unwrap();

  println!("Enqueuing jobs in {}", brokkr.name);

  let d = Duration::from_millis(100);

  for i in 0..10 {
    println!("Pushing job {}", i);
    let job_id = brokkr
      .enqueue::<Task, u64>(Task {
        id: i,
        msg: format!("Foo {}", i),
      }).unwrap();
    job_ids.push(job_id);
    sleep(d);
  }

  println!("Queue length: {}", brokkr.queue_size());

  for _ in 0..10 {
    let job_id = job_ids.remove(0);
    w.process_one();
    let job = brokkr.fetch_job::<Task, u64>(&job_id);
    println!("Job: {}", job.unwrap().unwrap());
    sleep(d);
  }

  let t = brokkr.dequeue::<Task, u64>().unwrap();
  // Should be None
  println!("Fetched {:?}", t);

  brokkr.clear_queue().unwrap();

  let t = brokkr.dequeue::<Task, u64>().unwrap();
  // Should be None
  println!("Fetched {:?}", t);
}
