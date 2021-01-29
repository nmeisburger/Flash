mod config;
mod doph;
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
use lsh::{HashType, LSH};
use reader::read_file;

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

  let data = read_file(
    config.data.filename,
    config.data.num_data,
    config.data.avg_dim,
    config.data.num_query,
  );

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

  let hashes = doph.hash(data);

  lsh.insert_range(0, config.data.num_data, &hashes);

  let query = read_file(
    config.data.filename,
    config.data.num_query,
    config.data.avg_dim,
    0,
  );

  let query_hashes = doph.hash(query);

  let _results = lsh.query(&query_hashes, config.topk);
}
