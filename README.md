# Flash

Rust implementation of the FLASH algorithm.

## Testing 
Run `$ cargo test` to run all unit tests.

## Compiling 
Run `$ cargo build` to generate the executable `./target/debug/flash`. This build will be compiled in debug mode, and will thus have additional checks for integer overflow and be less optimized. To compile an optimized build run `$ cargo build --release` which will build an optimized build in the `./target/release/flash` executable.
