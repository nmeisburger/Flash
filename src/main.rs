mod config;
mod doph;
#[allow(dead_code)]
mod evaluate;
#[allow(dead_code)]
mod heap_array;
#[allow(dead_code)]
mod lsh;
#[allow(dead_code)]
mod reader;
#[allow(dead_code)]
mod thread_pool;

use config::{Config, DataConfig, LSHConfig};
use doph::DOPH;
use evaluate::average_cosine_similarity;
use lsh::{HashType, IDType, LSH};
use reader::read_data_svm;

use std::env;

const TEST_CONFIG: Config = Config {
  lsh: LSHConfig {
    tables: 0,
    k: 0,
    range_pow: 0,
    reservoir_size: 0,
  },

  data: DataConfig {
    filename: "",
    avg_dim: 0,
    num_data: 0,
    num_query: 0,
  },

  topk: 0,
  simk: 0,
};

fn main() {
  let args: Vec<String> = env::args().collect();

  if args.len() != 2 {
    eprintln!("Usage: ./flash <config name>");
    return;
  }

  let config = match args[1].as_str() {
    "test" => &TEST_CONFIG,
    x => {
      panic!("Invalid mode \"{}\" entered", x);
    }
  };

  println!(
    "Reading {} vectors as insertion dataset",
    config.data.num_data
  );

  let data = read_data_svm(
    config.data.filename,
    config.data.num_data,
    config.data.avg_dim,
    config.data.num_query,
  );

  println!("\t-Done");

  let doph = DOPH::new(
    config.lsh.tables,
    config.lsh.k,
    config.lsh.range_pow as HashType,
  );

  let mut lsh = LSH::new(
    config.lsh.tables,
    config.lsh.range_pow,
    config.lsh.reservoir_size,
  );

  println!("Hashing data");

  let hashes = doph.hash(data);

  println!("\t-Done");

  println!("Inserting data");

  lsh.insert_range(
    config.data.num_query as IDType,
    config.data.num_data,
    &hashes,
  );

  println!("\t-Done");

  println!("Reading {} vectors as query dataset", config.data.num_query);

  let query = read_data_svm(
    config.data.filename,
    config.data.num_query,
    config.data.avg_dim,
    0,
  );

  println!("\t-Done");

  println!("Hashing queries");

  let query_hashes = doph.hash(query);

  println!("\t-Done");

  println!("Querying data");

  let results = lsh.query(&query_hashes, config.topk);

  println!("\t-Done");

  println!(
    "Reading all {} vectors for evaluation",
    config.data.num_data + config.data.num_query
  );

  let all_data = read_data_svm(
    config.data.filename,
    config.data.num_data + config.data.num_query,
    config.data.avg_dim,
    0,
  );

  println!("\t-Done");

  println!("Computing average cosine similarity");

  let sim = average_cosine_similarity(0, config.data.num_query, results, &all_data, config.simk);

  println!("Average cosine similarity @{} is {}", config.simk, sim);
}
