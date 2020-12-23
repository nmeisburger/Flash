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

fn read_file(filename: &str, num_lines: usize, avg_dim: usize, skip: usize) -> Data {
  let input = File::open(filename).expect("File should open");

  let reader = BufReader::new(input);

  let mut markers: Vec<usize> = Vec::with_capacity(num_lines + 1 - skip);
  let mut indices: Vec<HashType> = Vec::with_capacity((num_lines - skip) * avg_dim);

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
  }

  return Data {
    markers,
    indices,
    len: num_lines,
  };
}
