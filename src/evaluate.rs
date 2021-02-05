use crate::lsh::QueryResult;
use crate::reader::SVMData;

fn sparse_multiply(a: usize, b: usize, data: &SVMData) -> f32 {
  let mut ia = data.markers[a];
  let ea = data.markers[a + 1];
  let mut ib = data.markers[b];
  let eb = data.markers[b + 1];

  let mut total = 0.0;
  while ia < ea && ib < eb {
    if data.indices[ia] == data.indices[ib] {
      total += data.values[ia] * data.values[ib];
      ia += 1;
      ib += 1;
    } else if data.indices[ia] < data.indices[ib] {
      ia += 1;
    } else {
      ib += 1;
    }
  }

  return total;
}

fn magnitude(x: usize, data: &SVMData) -> f32 {
  let s = data.markers[x];
  let e = data.markers[x + 1];

  let mut total = 0.0;

  for i in s..e {
    total += data.values[i] * data.values[i];
  }

  return total.sqrt();
}

pub fn average_cosine_similarity(
  query_start: usize,
  query_count: usize,
  results: QueryResult,
  data: &SVMData,
  k: usize,
) -> f32 {
  let mut total = 0.0;
  let mut count = 0;
  for q in query_start..(query_start + query_count) {
    let mut c = 0;
    for r in results.nth(q) {
      let sim =
        sparse_multiply(q, r as usize, data) / (magnitude(q, data) * magnitude(r as usize, data));

      total += sim;
      count += 1;
      c += 1;
      if c >= k {
        break;
      }
    }
  }

  return total / count as f32;
}
