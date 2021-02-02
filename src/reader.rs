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
        markers.push(indices.len());
      }
      Err(_) => panic!("Error reading file '{}'", filename),
    }

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

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::prelude::Write;

  fn create_and_write_data(filename: &str) {
    let mut file = File::create(filename).expect("Should be able to open file for test");

    file
      .write(b"1 3:9.125 11:0.5 321:-0.125\n0 2:2.0 17:-1.5 18:-45 33:-1\n1 88:-1 91:0 120:-0 18223:-2.125\n1 4:-0.5\n0 177:-83.5 12:56.25")
      .expect("write should succeed");
  }

  #[test]
  fn test_read_svm() {
    let filename = "./temp_reader_test_file";

    create_and_write_data(filename);

    let data = read_data_svm(filename, 5, 3, 0);

    let markers = vec![0, 3, 7, 11, 12, 14];
    let indices = vec![3, 11, 321, 2, 17, 18, 33, 88, 91, 120, 18223, 4, 177, 12];
    let values = vec![
      9.125, 0.5, -0.125, 2.0, -1.5, -45.0, -1.0, -1.0, 0.0, 0.0, -2.125, -0.5, -83.5, 56.25,
    ];

    assert_eq!(data.len(), 5);
    assert_eq!(data.markers.len(), markers.len());
    assert_eq!(data.indices.len(), indices.len());
    assert_eq!(data.values.len(), values.len());

    for (i, &v) in data.markers.iter().enumerate() {
      assert_eq!(v, markers[i]);
    }

    for (i, &v) in data.indices.iter().enumerate() {
      assert_eq!(v, indices[i]);
    }

    for (i, &v) in data.values.iter().enumerate() {
      assert_eq!(v, values[i]);
    }

    std::fs::remove_file(filename).expect("Shoudl be able to delete file after test");
  }

  #[test]
  fn test_read_svm_with_skip() {
    let filename = "./temp_reader_test_file";

    create_and_write_data(filename);

    let data = read_data_svm(filename, 3, 3, 2);

    let markers = vec![0, 4, 5, 7];
    let indices = vec![88, 91, 120, 18223, 4, 177, 12];
    let values = vec![-1.0, 0.0, 0.0, -2.125, -0.5, -83.5, 56.25];

    assert_eq!(data.len(), 3);
    assert_eq!(data.markers.len(), markers.len());
    assert_eq!(data.indices.len(), indices.len());
    assert_eq!(data.values.len(), values.len());

    for (i, &v) in data.markers.iter().enumerate() {
      assert_eq!(v, markers[i]);
    }

    for (i, &v) in data.indices.iter().enumerate() {
      assert_eq!(v, indices[i]);
    }

    for (i, &v) in data.values.iter().enumerate() {
      assert_eq!(v, values[i]);
    }

    std::fs::remove_file(filename).expect("Shoudl be able to delete file after test");
  }
}
