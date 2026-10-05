fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Keep builds self-contained: generated files live under target/, and the
    // protobuf compiler is supplied by Cargo for the build platform.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_build::configure()
        .compile_protos(&["proto/storage.proto", "proto/control.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/storage.proto");
    println!("cargo:rerun-if-changed=proto/control.proto");
    Ok(())
}
