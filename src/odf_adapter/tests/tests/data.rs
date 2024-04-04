use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use chrono::{DateTime, Utc};
use indoc::indoc;

pub struct Dataset {
    pub data_slices: Vec<PathBuf>,
}

pub fn create_dataset(workspace: &Path, name: &str, schema: &[&str], chunks: &[&str]) -> Dataset {
    let schema: Vec<_> = schema.iter().map(|f| format!("\"{}\"", f)).collect();
    let schema = format!("[{}]", schema.join(", "));

    let manifest = indoc!(
        r#"
        ---
        version: 1
        kind: DatasetSnapshot
        content:
          name: "{name}"
          kind: Root
          metadata:
            - kind: AddPushSource
              sourceName: default
              read:
                kind: Csv
                header: false
                schema: {schema}
              merge:
                kind: Append
        "#
    )
    .replace("{name}", name)
    .replace("{schema}", &schema);

    let chunks: Vec<_> = chunks.iter().map(|c| (*c, Utc::now())).collect();
    create_dataset_common(workspace, name, &manifest, &chunks)
}

pub fn create_dataset_snapshot(
    workspace: &Path,
    name: &str,
    schema: &[&str],
    pk: &[&str],
    chunks: &[(&str, DateTime<Utc>)],
) -> Dataset {
    let schema: Vec<_> = schema.iter().map(|f| format!("\"{}\"", f)).collect();
    let schema = format!("[{}]", schema.join(", "));

    let pk: Vec<_> = pk.iter().map(|f| format!("\"{}\"", f)).collect();
    let pk = format!("[{}]", pk.join(", "));

    let manifest = indoc!(
        r#"
        ---
        version: 1
        kind: DatasetSnapshot
        content:
          name: "{name}"
          kind: Root
          metadata:
            - kind: AddPushSource
              sourceName: default
              read:
                kind: Csv
                header: false
                schema: {schema}
              merge:
                kind: Snapshot
                primaryKey: {pk}
        "#
    )
    .replace("{name}", name)
    .replace("{schema}", &schema)
    .replace("{pk}", &pk);

    create_dataset_common(workspace, name, &manifest, chunks)
}

fn create_dataset_common(
    workspace: &Path,
    name: &str,
    manifest: &str,
    chunks: &[(&str, DateTime<Utc>)],
) -> Dataset {
    if !(workspace.join(".kamu").exists()) {
        Command::new("kamu")
            .current_dir(workspace)
            .arg("init")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .exit_ok()
            .unwrap();
    }

    let mut child = Command::new("kamu")
        .current_dir(workspace)
        .args(["add", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();

    stdin.write_all(manifest.as_bytes()).unwrap();
    drop(stdin);
    child.wait().unwrap().exit_ok().unwrap();

    let data_dir = workspace.join(format!(".kamu/datasets/{name}/data"));
    let mut data_slices = Vec::new();

    for (chunk, event_time) in chunks {
        let mut child = Command::new("kamu")
            .current_dir(workspace)
            .args(["ingest", "--stdin", &name])
            .arg("--event-time")
            .arg(event_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(chunk.as_bytes()).unwrap();
        drop(stdin);
        child.wait().unwrap().exit_ok().unwrap();

        // Find and add new data slice
        for entry in std::fs::read_dir(&data_dir).unwrap() {
            let path = entry.unwrap().path();
            if !data_slices.contains(&path) {
                data_slices.push(path);
            }
        }
    }

    let data_slices = data_slices.into_iter().map(|p| data_dir.join(p)).collect();

    Dataset { data_slices }
}
