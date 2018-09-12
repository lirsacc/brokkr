#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

extern crate pretty_env_logger;

extern crate brokkr;

use brokkr::{Brokkr, Perform, Worker};
use std::thread::sleep;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct Task {
  id: u64,
  msg: String,
}

impl Perform for Task {
  type Result = u64;
  type Context = ();

  fn process(&self, _: &Self::Context) -> Self::Result {
    println!("Performing task id={}, msg={}", self.id, self.msg);
    if self.id == 5 {
      panic!("Not 5!");
    }
    if self.id == 8 {
      sleep(Duration::from_millis(1000));
    }
    self.id + 100
  }
}

fn main() {
  pretty_env_logger::init();
  let q = Brokkr::new("default".into());
  let w: Worker<Task> = Worker::new(&q, (), Duration::from_millis(500));

  // Clean up from previous runs.
  q.clear_all().unwrap();

  println!("Enqueuing jobs in {}", q.name);

  let d = Duration::from_millis(3000);

  for i in 0..10 {
    println!("Pushing job {}", i);
    q.enqueue(Task {
      id: i,
      msg: format!("Foo {}", i),
    }).unwrap();
    sleep(d);
  }

  println!("Queue length: {}", q.len());

  for _ in 0..10 {
    // println!("Fetching job");
    // let j = q.dequeue::<Task>().unwrap().unwrap();
    // println!("Fetched job {}, task {:?}", j.id, j.task);
    // j.task.process(&());
    w.process_one();
    sleep(d);
  }

  let t = q.dequeue::<Task>().unwrap();
  println!("Fetched {:?}", t);

  q.clear().unwrap();

  let t = q.dequeue::<Task>().unwrap();
  println!("Fetched {:?}", t);
}
