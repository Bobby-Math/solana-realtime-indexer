use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Compile Solana Geyser protobuf definitions
    tonic_build::configure()
        .build_server(false)
        .out_dir(&out_dir)
        .compile_protos(
            &["protos/solana/geyser.proto"],
            &["protos"],
        )?;

    println!("cargo:rerun-if-changed=protos/solana/geyser.proto");

    Ok(())
}