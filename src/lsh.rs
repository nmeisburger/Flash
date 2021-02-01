use crate::heap_array::HeapAllocatedArray;

use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::rc::Rc;

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

pub struct QueryResult {
  results: Rc<HeapAllocatedArray<IDType>>,
  len: usize,
  k: usize,
}

impl QueryResult {
  fn new(results: HeapAllocatedArray<IDType>, len: usize, k: usize) -> Self {
    QueryResult {
      results: Rc::new(results),
      len,
      k,
    }
  }

  fn nth(&self, idx: usize) -> ResultIter {
    let start = idx * (self.k + 1);
    ResultIter {
      results: Rc::clone(&self.results),
      curr: start,
      end: start + (self.results[start] as usize),
    }
  }

  fn len(&self) -> usize {
    self.len
  }

  fn count(&self, idx: usize) -> usize {
    self.results[idx * (self.k + 1)] as usize
  }
}

struct ResultIter {
  results: Rc<HeapAllocatedArray<IDType>>,
  curr: usize,
  end: usize,
}

impl Iterator for ResultIter {
  type Item = IDType;

  fn next(&mut self) -> Option<Self::Item> {
    if self.curr >= self.end {
      return None;
    }
    self.curr += 1;
    return Some(self.results[self.curr]);
  }
}

impl LSH {
  pub fn new(tables: usize, range_pow: usize, reservoir_size: usize) -> Self {
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

  pub fn insert(&mut self, ids: &[IDType], hashes: &[HashType]) {
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
          if r < self.reservoir_size {
            self.data[offset + 1 + r] = id;
          }
        }
      }
    }
  }

  pub fn insert_range(&mut self, id_start: IDType, count: usize, hashes: &[HashType]) {
    for n in 0..count {
      let id = id_start + n as IDType;

      for t in 0..self.tables {
        let hash = hashes[n * self.tables + t] as usize;
        let offset = t * self.table_size + hash * self.row_size;
        let count = self.data[offset] as usize;

        self.data[offset] += 1;
        if count < self.reservoir_size {
          self.data[offset + count + 1] = id;
        } else {
          let r = self.rand_values[count];
          if r < self.reservoir_size {
            self.data[offset + 1 + r] = id;
          }
        }
      }
    }
  }

  pub fn query(&self, hashes: &[HashType], k: usize) -> QueryResult {
    let num_query = hashes.len() / self.tables;
    let mut result: HeapAllocatedArray<IDType> =
      HeapAllocatedArray::with_default(num_query * (k + 1));

    let mut counts: HashMap<IDType, u32> =
      HashMap::with_capacity(self.reservoir_size * self.tables);

    for q in 0..num_query {
      for t in 0..self.tables {
        let hash = hashes[q * self.tables + t] as usize;
        let offset = t * self.table_size + hash * self.row_size;
        let count = std::cmp::min(self.data[offset] as usize, self.reservoir_size);
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

    return QueryResult::new(result, num_query, k);
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
        for i in 1..(std::cmp::min(self.data[start] as usize, self.reservoir_size) + 1) {
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

  #[test]
  fn test_query_result() {
    let data = [3, 8, 9, 2, 0, 0, 1, 1, 1, 1, 4, 90, 91, 92, 93];
    let mut arr = HeapAllocatedArray::new(15);
    for i in 0..15 {
      arr[i] = data[i];
    }
    let res = QueryResult::new(arr, 3, 4);

    let ex1 = vec![8, 9, 2];
    for (i, x) in res.nth(0).enumerate() {
      assert_eq!(ex1[i], x);
    }
    assert_eq!(res.count(0), 3);

    let ex2: Vec<IDType> = vec![];
    for (i, x) in res.nth(1).enumerate() {
      assert_eq!(ex2[i], x);
    }
    assert_eq!(res.count(1), 0);

    let ex3 = vec![90, 91, 92, 93];
    for (i, x) in res.nth(2).enumerate() {
      assert_eq!(ex3[i], x);
    }
    assert_eq!(res.count(2), 4);
  }

  fn do_simple_insert() -> LSH {
    let ids = [1, 2, 3, 4];
    let hashes = [0, 0, 1, 3, 2, 1, 0, 2, 3, 0, 0, 3, 2, 3, 0, 3];

    let mut lsh = LSH::new(4, 2, 4);

    lsh.insert(&ids, &hashes);

    return lsh;
  }

  fn do_second_insert(lsh: &mut LSH) {
    let mut new_rands = [15; 80];

    new_rands[5] = 2;

    lsh.override_rand_values(&new_rands);

    let ids = [5, 6, 7];
    let hashes = [2, 1, 0, 1, 0, 2, 0, 3, 2, 3, 0, 3];

    lsh.insert(&ids, &hashes);
  }

  #[test]
  fn test_simple_insert() {
    let lsh = do_simple_insert();

    let xx = IDType::MAX;

    let expected = [
      1, 1, xx, xx, xx, 0, xx, xx, xx, xx, 2, 2, 4, xx, xx, 1, 3, xx, xx, xx, 2, 1, 3, xx, xx, 1,
      2, xx, xx, xx, 0, xx, xx, xx, xx, 1, 4, xx, xx, xx, 3, 2, 3, 4, xx, 1, 1, xx, xx, xx, 0, xx,
      xx, xx, xx, 0, xx, xx, xx, xx, 0, xx, xx, xx, xx, 0, xx, xx, xx, xx, 1, 2, xx, xx, xx, 3, 1,
      3, 4, xx,
    ];

    for i in 0..80 {
      assert_eq!(lsh.data[i], expected[i]);
    }
  }

  #[test]
  fn test_reservoir_overflow() {
    let mut lsh = do_simple_insert();

    do_second_insert(&mut lsh);

    let xx = IDType::MAX;

    let expected = [
      2, 1, 6, xx, xx, 0, xx, xx, xx, xx, 4, 2, 4, 5, 7, 1, 3, xx, xx, xx, 2, 1, 3, xx, xx, 2, 2,
      5, xx, xx, 1, 6, xx, xx, xx, 2, 4, 7, xx, xx, 6, 2, 3, 7, 5, 1, 1, xx, xx, xx, 0, xx, xx, xx,
      xx, 0, xx, xx, xx, xx, 0, xx, xx, xx, xx, 1, 5, xx, xx, xx, 1, 2, xx, xx, xx, 5, 1, 3, 4, 6,
    ];

    for i in 0..80 {
      assert_eq!(lsh.data[i], expected[i]);
    }
  }

  #[test]
  fn test_query() {
    let mut lsh = do_simple_insert();

    do_second_insert(&mut lsh);

    println!("{}", lsh);

    let hashes = [0, 2, 3, 3, 1, 1, 2, 1, 1, 2, 2, 0];
    let result = lsh.query(&hashes, 4);

    let result1: Vec<IDType> = result.nth(0).collect();
    let match1 = result1[0] == 6
      && result1[1] == 1
      && ((result1[2] == 4 && result1[3] == 3) || (result1[2] == 3 && result1[3] == 4));
    assert!(match1);
    assert_eq!(result.count(0), 4);

    let result2: Vec<IDType> = result.nth(1).collect();
    let match2 = result2[0] == 5 && result2[1] == 2;
    assert!(match2);
    assert_eq!(result.count(1), 2);

    let result3: Vec<IDType> = result.nth(2).collect();
    let match3 = result3[0] == 6;
    assert!(match3);
    assert_eq!(result.count(2), 1);
  }

  #[test]
  fn test_insert_range() {
    let ids = [1, 2, 3, 4];
    let hashes = [0, 0, 1, 3, 2, 1, 0, 2, 3, 0, 0, 3, 2, 3, 0, 3];

    let mut lsh1 = LSH::new(4, 2, 4);

    lsh1.insert(&ids, &hashes);

    let mut lsh2 = LSH::new(4, 2, 4);

    lsh2.insert_range(1, 4, &hashes);

    for i in 0..lsh1.data.len() {
      assert_eq!(lsh1.data[i], lsh2.data[i]);
    }
  }
}
