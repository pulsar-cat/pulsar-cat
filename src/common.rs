use pulsar::{Authentication, Pulsar, PulsarBuilder, TokioExecutor};

use crate::{cli_options::AuthOpts, error::PulsarCatError};

fn handle_auth(
    mut builder: PulsarBuilder<TokioExecutor>,
    auth_opts: &AuthOpts,
) -> Result<PulsarBuilder<TokioExecutor>, PulsarCatError> {
    if let Some(token) = &auth_opts.token {
        builder = builder.with_auth(Authentication {
            name: "token".to_owned(),
            data: Vec::from(token.as_str()),
        });
    }

    Ok(builder)
}

pub async fn get_base_client(
    service_url: &str,
    auth_opts: &AuthOpts,
) -> Result<Pulsar<TokioExecutor>, PulsarCatError> {
    let builder = Pulsar::builder(service_url, TokioExecutor);
    let builder = handle_auth(builder, auth_opts)?;

    let pulsar = builder.build().await?;
    Ok(pulsar)
}
