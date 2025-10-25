fn main() {
    let iface_files = &["proto/stream_exchange.proto", "proto/game_relay.proto"];
    let dirs = &["."];

    // Temporarily skip proto compilation if protoc is not available
    if std::env::var("SKIP_PROTO_COMPILE").is_ok() {
        println!("cargo:warning=Skipping proto compilation (SKIP_PROTO_COMPILE is set)");
        return;
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
