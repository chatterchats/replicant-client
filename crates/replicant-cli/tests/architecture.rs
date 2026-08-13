use std::{fs, path::Path};

#[test]
fn managed_client_construction_stays_in_runtime() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![source];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).expect("read CLI source directory") {
            let path = entry.expect("read CLI source entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = fs::read_to_string(&path).expect("read CLI Rust source");
                assert!(
                    !source.contains("Client::builder("),
                    "managed clients must be constructed by replicant-runtime, not {}",
                    path.display()
                );
            }
        }
    }
}
