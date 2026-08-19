fn main() {
    let iface_files = &["proto/stream_exchange.proto", "proto/game_relay.proto"];
    let analytics_files = &["proto/analytics/v1/events.proto"];
    let dirs = &["."];

    // Temporarily skip proto compilation if protoc is not available
    if std::env::var("SKIP_PROTO_COMPILE").is_ok() {
        println!("cargo:warning=Skipping proto compilation (SKIP_PROTO_COMPILE is set)");
        return;
    }

    // The analytics schema is the source of truth for the Iceberg table, so its
    // descriptor set is embedded in the binary. A schema registry would add a
    // startup network failure mode and a second source of truth that can
    // disagree with the binary actually emitting the bytes.
    let descriptor_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("analytics.bin");
    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(analytics_files, dirs)
        .unwrap_or_else(|e| panic!("analytics protobuf compilation failed: {}", e));
    for file in analytics_files {
        println!("cargo:rerun-if-changed={}", file);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(iface_files, dirs)
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));

    // recompile protobufs only if any of the proto files changes.
    for file in iface_files {
        println!("cargo:rerun-if-changed={}", file);
    }
}
