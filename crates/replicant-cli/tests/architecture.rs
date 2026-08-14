use std::{fs, path::Path};

#[test]
fn gameplay_implementations_stay_outside_the_frontend() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![source];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).expect("read CLI source directory") {
            let path = entry.expect("read CLI source entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = fs::read_to_string(&path).expect("read CLI Rust source");
                for (pattern, responsibility) in [
                    ("Client::builder(", "managed client construction"),
                    ("replicant_client::raw", "raw API access"),
                    ("replicant_client::managed", "managed API internals"),
                    ("pub use replicant_", "reusable gameplay exports"),
                    ("fn execute_", "gameplay execution"),
                    ("fn reconcile_", "gameplay reconciliation"),
                ] {
                    assert!(
                        !source.contains(pattern),
                        "{responsibility} belongs in a reusable crate, not {}",
                        path.display()
                    );
                }
            }
        }
    }
}
