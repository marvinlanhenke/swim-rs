use std::io::Result;

fn main() -> Result<()> {
    let includes = ["./proto/"];
    let protos = ["./proto/swim.proto"];

    prost_build::Config::new()
        .type_attribute("NodeId", "#[derive(Eq, Hash)]")
        .compile_protos(&protos, &includes)?;

    Ok(())
}
