fn main() {
    let iface_files = &["proto/stream_exchange.proto", "proto/game_relay.proto"];
    let dirs = &["."];

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(iface_files, dirs)
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));

    // recompile protobufs only if any of the proto files changes.
    for file in iface_files {
        println!("cargo:rerun-if-changed={}", file);
    }
}