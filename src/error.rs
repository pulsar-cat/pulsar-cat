use anyhow::Error as AnyhowError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PulsarCatError {
    #[error("Pulsar error: {0}")]
    Pulsar(pulsar::Error),
    #[error("Application error: {0}")]
    Application(AnyhowError),
}

impl From<pulsar::Error> for PulsarCatError {
    fn from(e: pulsar::Error) -> Self {
        PulsarCatError::Pulsar(e)
    }
}

impl From<AnyhowError> for PulsarCatError {
    fn from(e: AnyhowError) -> Self {
        PulsarCatError::Application(e)
    }
}
