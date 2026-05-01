fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_directory = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "schemas".to_owned());

    pioneer_protocol::write_protocol_schemas(output_directory)?;
    Ok(())
}
