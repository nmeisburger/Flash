use crate::lsh::HashType;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct Data {
  pub markers: Vec<usize>,
  pub indices: Vec<HashType>,
  pub values: Vec<f32>,
  len: usize,
}

impl Data {
  pub fn len(&self) -> usize {
    self.len
  }
}

pub fn partition(total_len: usize, num: usize) -> Vec<usize> {
  let base = total_len / num;
  let rmdr = total_len % num;

  (0..num)
    .map(|i| if i < rmdr { base + 1 } else { base })
    .collect()
}

pub fn read_data_svm(filename: &str, num_lines: usize, avg_dim: usize, skip: usize) -> Data {
  let input = File::open(filename).expect("File should open");

  let reader = BufReader::new(input);

  let mut markers: Vec<usize> = Vec::with_capacity(num_lines + 1);
  let mut indices: Vec<HashType> = Vec::with_capacity(num_lines * avg_dim);
  let mut values: Vec<f32> = Vec::with_capacity(num_lines * avg_dim);
  let mut lines_read = 0;

  markers.push(0);

  for line in reader.lines().skip(skip) {
    match line {
      Ok(s) => {
        for pair in s.split(' ').skip(1) {
          let i = pair.find(':').expect("Pair should have ':'");
          indices.push(pair[..i].parse::<HashType>().expect("Should be integer"));
          values.push(pair[i + 1..].parse::<f32>().expect("Should be float"));
        }
        markers.push(indices.len());
      }
      Err(_) => panic!("Error reading file '{}'", filename),
    }
    markers.push(indices.len());

    lines_read += 1;
    if lines_read >= num_lines {
      break;
    }
  }

  return Data {
    markers,
    indices,
    values,
    len: num_lines,
  };
}

pub fn read_data_svm_partitioned(
  filename: &str,
  total_len: usize,
  num_partitions: usize,
  avg_dim: usize,
  skip: usize,
) -> Vec<Data> {
  let input = File::open(filename).expect("File should open");

  let reader = BufReader::new(input);

  let partition_lens = partition(total_len, num_partitions);

  let mut results = Vec::with_capacity(num_partitions);

  let mut curr_len = partition_lens[0];
  let mut idx = 0;
  let mut lines_read = 0;

  let mut markers: Vec<usize> = Vec::with_capacity(curr_len + 1);
  markers.push(0);
  let mut indices: Vec<HashType> = Vec::with_capacity(curr_len * avg_dim);
  let mut values: Vec<f32> = Vec::with_capacity(curr_len * avg_dim);

  for line in reader.lines().skip(skip) {
    match line {
      Ok(s) => {
        for pair in s.split(' ').skip(1) {
          let i = pair.find(':').expect("Pair should have ':'");
          indices.push(pair[..i].parse::<HashType>().expect("Should be integer"));
          values.push(pair[i + 1..].parse::<f32>().expect("Should be float"));
        }
      }
      Err(_) => panic!("Error reading file '{}'", filename),
    }
    markers.push(indices.len());

    lines_read += 1;
    if lines_read >= curr_len {
      results.push(Data {
        indices,
        markers,
        values: values,
        len: curr_len,
      });
      idx += 1;
      if idx >= num_partitions {
        break;
      }
      curr_len = partition_lens[idx];
      lines_read = 0;

      markers = Vec::with_capacity(curr_len + 1);
      markers.push(0);
      indices = Vec::with_capacity(curr_len * avg_dim);
      values = Vec::with_capacity(curr_len * avg_dim);
    }
  }

  return results;
}
