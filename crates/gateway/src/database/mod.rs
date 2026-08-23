mod connection;
pub(crate) mod maintenance;
pub(crate) mod startup;
mod zstd_column;

#[cfg(test)]
pub(crate) use connection::initialize;
pub(crate) use connection::{
    gateway_database_path, initialize_existing_for_operations, initialize_with_startup,
};
