# Galaxy renderer

This maintained source was imported from the `galaxy-renderer` supplied with
the approved `replicant.react` reference application. The original author gave
explicit permission for its use in Replicant. Rendering behavior and existing
capabilities are preserved.

The browser-only `cdylib` intentionally stays outside the native Cargo
workspace. Run `make galaxy-wasm` from the repository root to build it with
`wasm-pack` and package the generated JS/WASM files for `apps/web`. The build is
locked by this crate's `Cargo.lock`; generated files are ignored and rebuilt by
frontend development, build, and CI commands.
