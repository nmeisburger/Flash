use std::alloc::{alloc, dealloc, Layout};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, Index, IndexMut};
use std::ptr;

struct MemPtr<T> {
  ptr: *const T,
  _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for MemPtr<T> {}
unsafe impl<T: Sync> Sync for MemPtr<T> {}

impl<T> MemPtr<T> {
  fn new(ptr: *mut T) -> MemPtr<T> {
    MemPtr {
      ptr: ptr,
      _marker: PhantomData,
    }
  }

  fn ptr(&self) -> *mut T {
    self.ptr as *mut T
  }
}

pub struct HeapAllocatedArray<T> {
  ptr: MemPtr<T>,
  len: usize,
  layout: Layout,
}

impl<T> HeapAllocatedArray<T> {
  pub fn new(len: usize) -> Self {
    let alignment = mem::align_of::<T>();
    let size = mem::size_of::<T>();
    let layout = Layout::from_size_align(size * len, alignment).expect("Layout should be valid");

    unsafe {
      let ptr = alloc(layout) as *mut T;
      HeapAllocatedArray {
        ptr: MemPtr::new(ptr),
        len: len,
        layout: layout,
      }
    }
  }

  fn get(&self, idx: usize) -> T {
    if idx >= self.len {
      panic!(
        "Invalid index {} for HeapAllocatedArray of length {}",
        idx, self.len
      );
    }

    unsafe { ptr::read(self.ptr.ptr().offset(idx as isize)) }
  }

  fn set(&mut self, idx: usize, val: T) {
    if idx >= self.len {
      panic!(
        "Invalid index {} for HeapAllocatedArray of length {}",
        idx, self.len
      );
    }

    unsafe {
      ptr::write(self.ptr.ptr().offset(idx as isize), val);
    }
  }

  pub fn len(&self) -> usize {
    self.len
  }
}

impl<T> HeapAllocatedArray<T>
where
  T: Ord,
{
  pub fn sort(&mut self, start: usize, len: usize) {
    unsafe {
      let s = std::slice::from_raw_parts_mut(self.ptr.ptr().offset(start as isize), len);
      s.sort();
    }
  }
}

impl<T> Drop for HeapAllocatedArray<T> {
  fn drop(&mut self) {
    unsafe {
      dealloc(self.ptr.ptr() as *mut u8, self.layout);
    }

    println!("called drop")
  }
}

impl<T> Index<usize> for HeapAllocatedArray<T> {
  type Output = T;

  fn index(&self, idx: usize) -> &T {
    if idx >= self.len {
      panic!(
        "Invalid index {} for HeapAllocatedArray of length {}",
        idx, self.len
      );
    }

    unsafe {
      self
        .ptr
        .ptr()
        .offset(idx as isize)
        .as_ref()
        .expect("Heap ptr should not be null")
    }
  }
}

impl<T> IndexMut<usize> for HeapAllocatedArray<T> {
  fn index_mut(&mut self, idx: usize) -> &mut T {
    if idx >= self.len {
      panic!(
        "Invalid index {} for HeapAllocatedArray of length {}",
        idx, self.len
      );
    }

    unsafe {
      self
        .ptr
        .ptr()
        .offset(idx as isize)
        .as_mut()
        .expect("Heap ptr should not be null")
    }
  }
}

impl<T> Deref for HeapAllocatedArray<T> {
  type Target = [T];

  fn deref(&self) -> &[T] {
    unsafe { std::slice::from_raw_parts(self.ptr.ptr(), self.len()) }
  }
}

impl<T> HeapAllocatedArray<T>
where
  T: Default,
{
  pub fn with_default(len: usize) -> HeapAllocatedArray<T> {
    let mut h = Self::new(len);
    for i in 0..len {
      h[i] = T::default();
    }
    return h;
  }
}

impl<T> HeapAllocatedArray<T>
where
  T: Copy,
{
  pub fn with_value(len: usize, value: T) -> HeapAllocatedArray<T> {
    let mut h = Self::new(len);
    for i in 0..len {
      h[i] = value;
    }
    return h;
  }
}

impl<T> fmt::Display for HeapAllocatedArray<T>
where
  T: fmt::Display,
{
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[")?;

    for i in 0..self.len() {
      if i != 0 {
        write!(f, ", ")?;
      }
      write!(f, "{}", self[i])?;
    }

    write!(f, "]")
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_array_get_set() {
    let mut arr: HeapAllocatedArray<i32> = HeapAllocatedArray::new(4);

    arr.set(2, 33);
    assert_eq!(33, arr.get(2));

    arr.set(1, 17);
    assert_eq!(17, arr.get(1));
    assert_eq!(33, arr.get(2));

    arr.set(2, -923);
    assert_eq!(-923, arr.get(2));

    assert_eq!(4, arr.len());
  }

  #[test]
  fn test_array_index() {
    let mut arr: HeapAllocatedArray<i32> = HeapAllocatedArray::new(7);

    arr.set(5, 33);
    assert_eq!(33, arr.get(5));
    assert_eq!(33, arr[5]);

    arr[3] = 77;
    assert_eq!(77, arr.get(3));

    assert_eq!(77, arr[3]);

    assert_eq!(7, arr.len());
  }

  #[test]
  fn test_deref() {
    let mut arr: HeapAllocatedArray<usize> = HeapAllocatedArray::new(5);

    for i in 0..5 {
      arr[i] = i * i;
    }

    for (i, x) in arr.iter().enumerate() {
      assert_eq!(i * i, *x);
    }
  }

  #[test]
  fn test_with_default() {
    let h: HeapAllocatedArray<usize> = HeapAllocatedArray::with_default(1000);

    for &x in h.iter() {
      assert_eq!(0, x);
    }
  }

  #[test]
  fn test_with_value() {
    let h: HeapAllocatedArray<i8> = HeapAllocatedArray::with_value(1000, -3);

    for &x in h.iter() {
      assert_eq!(-3, x);
    }
  }

  #[test]
  fn test_sort() {
    let a = [56, 3, -47, 34, 33, 5, -2, 34, 0, 6, -11, 5, 1];
    let b = [56, 3, -47, -2, 0, 5, 6, 33, 34, 34, -11, 5, 1];
    let mut h: HeapAllocatedArray<i32> = HeapAllocatedArray::with_default(a.len());
    for i in 0..a.len() {
      h[i] = a[i];
    }

    h.sort(2, 8);

    for i in 0..h.len() {
      assert_eq!(b[i], h[i]);
    }
  }
}
