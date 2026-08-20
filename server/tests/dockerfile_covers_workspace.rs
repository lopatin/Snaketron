//! The release image builds from an explicit list of workspace members, so a
//! new member is invisible to it until someone remembers to add a `COPY`.
//!
//! Nobody remembered when `service-api` was added: `cargo build` inside the
//! image failed loading the workspace manifest, both the production release
//! and the dev certification build with that Dockerfile, and CI never builds
//! an image — so every check stayed green while nothing could deploy.
//!
//! This is a text comparison rather than a docker build on purpose. Building
//! the real image takes minutes and needs a daemon; the bug is a missing line,
//! and a missing line is cheap to detect.

use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<root>/server`.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("server/ always has a parent")
        .to_path_buf()
}

/// Workspace members, read from the manifest rather than hardcoded — a
/// hardcoded copy here would drift exactly the way the Dockerfile did.
fn members() -> Vec<String> {
    let manifest = std::fs::read_to_string(workspace_root().join("Cargo.toml"))
        .expect("workspace manifest must be readable");
    let start = manifest
        .find("members")
        .and_then(|i| manifest[i..].find('[').map(|j| i + j + 1))
        .expect("workspace manifest must declare members");
    let end = start
        + manifest[start..]
            .find(']')
            .expect("members list must be closed");

    manifest[start..end]
        .split(',')
        .map(|entry| entry.trim().trim_matches('"').trim())
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect()
}

fn assert_every_member_present(dockerfile: &str) {
    let path = workspace_root().join("server").join(dockerfile);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));

    let missing: Vec<String> = members()
        .into_iter()
        // A member is covered if the build context mentions it at all: the
        // release image copies whole directories (`COPY common ./common`)
        // while the dev image copies manifests (`COPY common/Cargo.toml ...`),
        // and both spellings contain the member name.
        .filter(|member| !text.contains(member.as_str()))
        .collect();

    assert!(
        missing.is_empty(),
        "{} does not copy workspace member(s) {missing:?}. \
         cargo cannot load a workspace whose member directory is absent, so the \
         image build fails with \"failed to load manifest for workspace member\" \
         — and no CI job builds an image, so this will not be caught anywhere else.",
        path.display()
    );
}

#[test]
fn the_release_image_copies_every_workspace_member() {
    assert_every_member_present("Dockerfile");
}

#[test]
fn the_dev_image_copies_every_workspace_member() {
    assert_every_member_present("Dockerfile.dev");
}

/// Guards the guard: if the members list stops parsing, the checks above would
/// silently pass over an empty set.
#[test]
fn the_member_list_actually_parses() {
    let members = members();
    assert!(
        members.len() >= 5,
        "expected the real member list, got {members:?}"
    );
    assert!(
        members.iter().any(|m| m == "service-api"),
        "service-api is the member that exposed this bug: {members:?}"
    );
}
