use crate::heap_array::HeapAllocatedArray;

use rand::{thread_rng, Rng};
use std::collections::HashMap;

pub type IDType = u32;
pub type HashType = u32;

pub struct LSH {
  data: HeapAllocatedArray<IDType>,
  tables: usize,
  rows: usize,
  reservoir_size: usize,
  row_size: usize,
  table_size: usize,
  rand_values: HeapAllocatedArray<usize>,
}

impl LSH {
  fn new(tables: usize, range_pow: usize, reservoir_size: usize) -> Self {
    let mut rng = thread_rng();
    let mut rand_values = HeapAllocatedArray::new(reservoir_size * 20);
    for i in 1..reservoir_size * 20 {
      rand_values[i] = rng.gen::<usize>() % i;
    }

    let rows = 1 << range_pow;
    let mut lsh = LSH {
      data: HeapAllocatedArray::with_value(tables * rows * (reservoir_size + 1), IDType::MAX),
      tables,
      rows: rows,
      reservoir_size,
      row_size: reservoir_size + 1,
      table_size: rows * (reservoir_size + 1),
      rand_values,
    };

    for t in 0..tables {
      for r in 0..rows {
        lsh.data[t * lsh.table_size + r * lsh.row_size] = 0;
      }
    }

    return lsh;
  }

  fn insert(&mut self, ids: &[IDType], hashes: &[HashType]) {
    for n in 0..ids.len() {
      let id = ids[n];

      for t in 0..self.tables {
        let hash = hashes[n * self.tables + t] as usize;
        let offset = t * self.table_size + hash * self.row_size;
        let count = self.data[offset] as usize;

        self.data[offset] += 1;
        if count < self.reservoir_size {
          self.data[offset + count + 1] = id;
        } else {
          let r = self.rand_values[count];
          if r < self.row_size {
            self.data[offset + 1 + r] = id;
          }
        }
      }
    }
  }

  fn query(&self, hashes: &[HashType], k: usize) -> HeapAllocatedArray<IDType> {
    let num_query = hashes.len() / self.tables;
    let mut result: HeapAllocatedArray<IDType> =
      HeapAllocatedArray::with_default(num_query * (k + 1));

    let mut counts: HashMap<IDType, u32> =
      HashMap::with_capacity(self.reservoir_size * self.tables);

    for q in 0..num_query {
      for t in 0..self.tables {
        let hash = hashes[q * self.tables + t] as usize;
        let offset = t * self.table_size + hash * self.row_size;
        let count = self.data[offset] as usize;

        for i in 1..count + 1 {
          let id = self.data[offset + i];
          match counts.get(&id) {
            Some(&cnt) => {
              counts.insert(id, cnt + 1);
            }
            None => {
              counts.insert(id, 1);
            }
          }
        }
      }

      let mut topk: Vec<_> = counts.drain().collect();
      topk.sort_by(|a, b| b.1.cmp(&a.1));

      let start = (k + 1) * q;
      let num = std::cmp::min(topk.len(), k);
      result[start] = num as IDType;
      for i in 0..num {
        result[start + i + 1] = topk[i].0;
      }
    }

    return result;
  }

  fn override_rand_values(&mut self, vals: &[usize]) {
    for i in 0..self.reservoir_size * 20 {
      self.rand_values[i] = vals[i];
    }
  }
}

impl std::fmt::Display for LSH {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    for t in 0..self.tables {
      write!(f, "Table: {}\n", t)?;
      for r in 0..self.rows {
        let start = t * self.table_size + r * self.row_size;
        write!(f, "    Row {}[{}]: ", r, self.data[start])?;
        for i in 1..(self.data[start] + 1) as usize {
          write!(f, "{} ", self.data[start + i])?;
        }
        write!(f, "\n")?;
      }
      write!(f, "\n")?;
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn do_simple_insert() -> LSH {
    let ids = [1, 2, 3, 4];
    let hashes = [0, 0, 1, 2, 2, 1, 0, 3, 3, 0, 0, 3, 2, 3, 0, 3];

    let mut lsh = LSH::new(4, 2, 4);

    lsh.insert(&ids, &hashes);

    return lsh;
  }

  #[test]
  fn test_simple_insert() {
    let lsh = do_simple_insert();

    let xx = IDType::MAX;

    let expected = [
      1, 1, xx, xx, xx, 0, xx, xx, xx, xx, 2, 2, 4, xx, xx, 1, 3, xx, xx, xx, 2, 1, 3, xx, xx, 1,
      2, xx, xx, xx, 0, xx, xx, xx, xx, 1, 4, xx, xx, xx, 3, 2, 3, 4, xx, 1, 1, xx, xx, xx, 0, xx,
      xx, xx, xx, 0, xx, xx, xx, xx, 0, xx, xx, xx, xx, 0, xx, xx, xx, xx, 1, 1, xx, xx, xx, 3, 2,
      3, 4, xx,
    ];

    for i in 0..80 {
      assert_eq!(lsh.data[i], expected[i]);
    }
  }
}
