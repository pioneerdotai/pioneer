mod devices;
mod profile;
mod view;
pub(crate) use profile::ProfileEditor;

#[cfg(test)]
pub(crate) use profile::{Native, Registrar};
