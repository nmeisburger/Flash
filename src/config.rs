pub struct LSHConfig {
  pub tables: usize,
  pub k: usize,
  pub range_pow: usize,
  pub reservoir_size: usize,
}

pub struct DataConfig {
  pub filename: &'static str,
  pub avg_dim: usize,
  pub num_data: usize,
  pub num_query: usize,
}

pub struct Config {
  pub lsh: LSHConfig,
  pub data: DataConfig,
  pub topk: usize,
}
