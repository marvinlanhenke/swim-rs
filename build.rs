use std::io::Result;

fn main() -> Result<()> {
    let includes = ["./proto/"];
    let protos = ["./proto/v1/swim.proto"];

    prost_build::Config::new()
        .type_attribute("NodeJoined", "#[derive(Hash)]")
        .type_attribute("NodeSuspected", "#[derive(Hash)]")
        .type_attribute("NodeRecovered", "#[derive(Hash)]")
        .type_attribute("NodeDeceased", "#[derive(Hash)]")
        .compile_protos(&protos, &includes)?;

    Ok(())
}
