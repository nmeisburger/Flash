use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

type Task = Box<dyn FnOnce() + Send + 'static>;

enum Message {
  Task(Task),
  Terminate,
}

struct Worker {
  thread: Option<thread::JoinHandle<()>>,
  sender: mpsc::Sender<Message>,
  _id: usize,
}

impl Worker {
  fn new(id: usize) -> Worker {
    let (send, recv): (Sender<Message>, Receiver<Message>) = mpsc::channel();

    let thread = thread::spawn(move || {
      while let Ok(task) = recv.recv() {
        match task {
          Message::Task(f) => f(),
          Message::Terminate => break,
        }
      }
    });
    Worker {
      thread: Some(thread),
      sender: send,
      _id: id,
    }
  }
}

pub struct ThreadPool {
  workers: Vec<Worker>,
}

impl ThreadPool {
  pub fn new(size: usize) -> ThreadPool {
    ThreadPool {
      workers: (0..size).map(|id| Worker::new(id)).collect(),
    }
  }

  pub fn assign_to_worker<F>(&self, worker: usize, f: F)
  where
    F: FnOnce() + Send + 'static,
  {
    let task = Message::Task(Box::new(f));
    self.workers[worker]
      .sender
      .send(task)
      .expect("Error sending task to worker");
  }
}

impl Drop for ThreadPool {
  fn drop(&mut self) {
    for worker in &self.workers {
      worker
        .sender
        .send(Message::Terminate)
        .expect("Should be able to send exit message");
    }

    for worker in &mut self.workers {
      match worker.thread.take() {
        Some(thread) => thread.join().unwrap(),
        None => panic!("Expected 'Some(thread)' got 'None'"),
      }
    }
  }
}

pub struct TaskPool<T> {
  ongoing_tasks: Vec<thread::JoinHandle<T>>,
}

impl<T> TaskPool<T>
where
  T: Send + 'static,
{
  pub fn new() -> TaskPool<T> {
    TaskPool {
      ongoing_tasks: Vec::new(),
    }
  }

  pub fn with_capacity(n: usize) -> TaskPool<T> {
    TaskPool {
      ongoing_tasks: Vec::with_capacity(n),
    }
  }

  pub fn add_task<F>(&mut self, f: F)
  where
    F: (FnOnce() -> T) + Send + 'static,
  {
    self.ongoing_tasks.push(thread::spawn(f));
  }

  pub fn wait(&mut self) -> Vec<T> {
    self
      .ongoing_tasks
      .drain(..)
      .map(|thread| thread.join().expect("Task should succeed"))
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_task_pool() {
    let mut tp = TaskPool::new();

    for i in 0..10 {
      tp.add_task(move || vec![i, i * i, i * i * i]);
    }

    for (i, res) in tp.wait().iter().enumerate() {
      assert_eq!(res.len(), 3);
      assert_eq!(res[0], i);
      assert_eq!(res[1], i * i);
      assert_eq!(res[2], i * i * i);
    }
  }
}
