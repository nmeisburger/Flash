use crate::heap_array::HeapAllocatedArray;
use crate::lsh::HashType;
use crate::reader::SVMData;

use rand::{thread_rng, Rng};
// use std::num::Wrapping;

const MAX_DENSIFY_RETRY: HashType = 100;

pub struct DOPH {
  k: usize,
  l: usize,
  num_hashes: usize,
  range_pow: HashType,
  log_num_hash: HashType,
  binsize: HashType,

  seeds: HeapAllocatedArray<HashType>,
  randa: HashType,
  randb: HashType,
}

impl DOPH {
  pub fn new(l: usize, k: usize, range_pow: HashType) -> DOPH {
    let num_hashes = k * l;

    let mut log_num_hash = 1;
    while log_num_hash * 2 < (num_hashes as HashType) {
      log_num_hash *= 2;
    }

    let mut rng = thread_rng();

    let mut seeds = HeapAllocatedArray::new(num_hashes);
    for i in 0..num_hashes {
      seeds[i] = rng.gen();
    }

    DOPH {
      k: k,
      l: l,
      num_hashes: num_hashes,
      range_pow: range_pow,
      log_num_hash: log_num_hash,
      binsize: (1 << range_pow) / ((num_hashes) as HashType),
      seeds: seeds,
      randa: rng.gen(),
      randb: rng.gen(),
    }
  }

  pub fn hash(&self, data: SVMData) -> HeapAllocatedArray<HashType> {
    let mut hashes_indices = HeapAllocatedArray::with_default(self.l * data.len());

    let mut hashes: HeapAllocatedArray<HashType> = HeapAllocatedArray::new(self.num_hashes);
    let mut min_hashes: HeapAllocatedArray<HashType> = HeapAllocatedArray::new(self.num_hashes);

    for n in 0..data.len() {
      min_hashes.fill(HashType::MAX);
      // Compute min-hash for each bin
      for i in data.markers[n]..data.markers[n + 1] {
        let val = data.indices[i];
        let mut h = val * self.randa;
        h ^= h >> 13;
        h *= 0x85ebca6b;
        let final_hash = (h * val << 5) >> (32 - self.range_pow);
        let bin = final_hash / self.binsize;
        if min_hashes[bin as usize] > final_hash {
          min_hashes[bin as usize] = final_hash;
        }
      }

      // Densify hash
      for i in 0..self.num_hashes {
        let mut next = min_hashes[i];
        if next != HashType::MAX {
          hashes[i] = next;
          continue;
        }
        let mut cnt: HashType = 0;
        while next == HashType::MAX {
          cnt += 1;
          let idx = std::cmp::min(
            self.rand_hash(i as HashType, cnt),
            self.num_hashes as HashType,
          );
          next = min_hashes[idx as usize];
          if cnt >= MAX_DENSIFY_RETRY {
            next = 0; // TODO: Default value?
            eprintln!("Densification Failure");
            break;
          }
        }

        hashes[i] = next;
      }

      // Combine L * K hashes into L hashes
      for t in 0..self.l {
        let mut idx = 0;
        for i in 0..self.k {
          let val = hashes[t * self.k + i];
          let mut h = val * self.seeds[t * self.k + i];
          h ^= h >> 13;
          h ^= self.seeds[t * self.k + i];
          idx += h * val;
        }
        idx = (idx << 2) >> (32 - self.range_pow);

        hashes_indices[n * self.l + t] = idx;
      }
    }

    return hashes_indices;
  }

  fn rand_hash(&self, bin: HashType, count: HashType) -> HashType {
    let temp = ((bin + 1) << 10) + count;
    return (self.randb * temp << 3) >> (32 - self.log_num_hash);
  }
}

// #[cfg(test)]
// mod tests {
//   use super::*;

//   #[test]
//   fn test_doph() {
//     let markers = vec![0, 4, 5, 7];
//     let indices = vec![88, 91, 120, 18223, 4, 177, 12];
//     let values = vec![-1.0, 0.125, 0.0, -2.125, -0.5, -83.5, 56.25];

//     let data = SVMData {
//       markers,
//       indices,
//       values,
//       len: 3,
//     };

//     let doph = DOPH::new(4, 4, 4);

//     let hashes = doph.hash(data);

//     assert_eq!(hashes.len(), 12);
//   }
// }
