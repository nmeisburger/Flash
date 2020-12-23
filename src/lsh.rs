use crate::heap_array::HeapAllocatedArray;

use rand::{thread_rng, Rng};
use std::collections::HashMap;

type IDType = u32;
type HashType = u32;

pub struct LSH {
  data: HeapAllocatedArray<IDType>,
  tables: usize,
  _rows: usize,
  reservoir_size: usize,
  row_size: usize,
  table_size: usize,
  rng: rand::rngs::ThreadRng,
}

// TODO: Precompute random values

impl LSH {
  fn new(tables: usize, rows: usize, reservoir_size: usize) -> Self {
    let mut lsh = LSH {
      data: HeapAllocatedArray::with_value(tables * rows * (reservoir_size + 1), IDType::MAX),
      tables,
      _rows: rows,
      reservoir_size,
      row_size: reservoir_size + 1,
      table_size: rows * (reservoir_size + 1),
      rng: thread_rng(),
    };

    for t in 0..tables {
      for r in 0..rows {
        lsh.data[t * lsh.row_size + r * lsh.row_size] = 0;
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
          let r = self.rng.gen::<usize>() % count;
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
}
