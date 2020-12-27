use crate::lsh::HashType;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct Data {
  pub markers: Vec<usize>,
  pub indices: Vec<HashType>,
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

pub fn read_file(filename: &str, num_lines: usize, avg_dim: usize, skip: usize) -> Data {
  let input = File::open(filename).expect("File should open");

  let reader = BufReader::new(input);

  let mut markers: Vec<usize> = Vec::with_capacity(num_lines + 1);
  let mut indices: Vec<HashType> = Vec::with_capacity((num_lines) * avg_dim);
  let mut lines_read = 0;

  markers.push(0);

  for line in reader.lines().skip(skip) {
    match line {
      Ok(s) => {
        let chars = s.chars();
        let mut curr_val: HashType = 0;
        let mut at_val: bool = false;
        for c in chars {
          if c == ' ' {
            at_val = true;
          } else if c == ':' {
            at_val = false;
            indices.push(curr_val);
            curr_val = 0;
          } else if at_val {
            curr_val *= 10;
            curr_val += ((c as u8) - ('0' as u8)) as HashType
          }
        }
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
    len: num_lines,
  };
}

pub fn read_file_partitioned(
  filename: &str,
  parition_lens: &[usize],
  avg_dim: usize,
  skip: usize,
) -> Vec<Data> {
  let input = File::open(filename).expect("File should open");

  let reader = BufReader::new(input);

  let mut results = Vec::with_capacity(parition_lens.len());

  let mut curr_len = parition_lens[0];
  let mut idx = 0;
  let mut lines_read = 0;

  let mut markers: Vec<usize> = Vec::with_capacity(curr_len + 1);
  markers.push(0);
  let mut indices: Vec<HashType> = Vec::with_capacity((curr_len) * avg_dim);

  for line in reader.lines().skip(skip) {
    match line {
      Ok(s) => {
        let chars = s.chars();
        let mut curr_val: HashType = 0;
        let mut at_val: bool = false;
        for c in chars {
          if c == ' ' {
            at_val = true;
          } else if c == ':' {
            at_val = false;
            indices.push(curr_val);
            curr_val = 0;
          } else if at_val {
            curr_val *= 10;
            curr_val += ((c as u8) - ('0' as u8)) as HashType
          }
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
        len: curr_len,
      });
      idx += 1;
      if idx >= parition_lens.len() {
        break;
      }
      curr_len = parition_lens[idx];
      lines_read = 0;

      markers = Vec::with_capacity(curr_len + 1);
      markers.push(0);
      indices = Vec::with_capacity((curr_len) * avg_dim);
    }
  }

  return results;
}
