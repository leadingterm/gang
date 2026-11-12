//! The single `gang` binary: forwards argv to the `gang-main` facade.
use mimalloc::MiMalloc;

/// musl's allocator serializes threads under contention; mimalloc is `NativeLink`'s own choice.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> std::process::ExitCode {
    gang_main::run(&std::env::args_os().collect::<Vec<_>>())
}
