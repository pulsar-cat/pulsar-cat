mod consume_op;
mod list_op;
mod produce_op;

pub use crate::error::PulsarCatError;

pub trait OpValidate {
    fn validate(&self) -> Result<(), PulsarCatError>;
}

pub use consume_op::run_consume;
pub use list_op::run_list;
pub use produce_op::run_produce;
