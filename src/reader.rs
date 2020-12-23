use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct Data {
  indices: Vec<usize>,
  data: Vec<u32>,
  num: usize,
}

fn read_file(filename: &str, num_lines: usize, avg_dim: usize, skip: usize) -> Data {
  let input = File::open(filename).expect("File should open");

  let reader = BufReader::new(input);

  let mut indices: Vec<usize> = Vec::with_capacity(num_lines + 1 - skip);
  let mut data: Vec<u32> = Vec::with_capacity((num_lines - skip) * avg_dim);

  indices.push(0);

  for line in reader.lines().skip(skip) {
    match line {
      Ok(s) => {
        let chars = s.chars();
        let mut curr_val: u32 = 0;
        let mut at_val: bool = false;
        for c in chars {
          if c == ' ' {
            at_val = true;
          } else if c == ':' {
            at_val = false;
            data.push(curr_val);
            curr_val = 0;
          } else if at_val {
            curr_val *= 10;
            curr_val += ((c as u8) - ('0' as u8)) as u32
          }
        }
      }
      Err(_) => panic!("Error reading file '{}'", filename),
    }
    indices.push(data.len());
  }

  return Data {
    indices,
    data,
    num: num_lines,
  };
}
