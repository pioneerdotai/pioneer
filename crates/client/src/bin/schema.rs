fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_directory = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "schemas/client".to_owned());

    pioneer_client::schema::write_client_schemas(output_directory)?;
    Ok(())
}
