use lazy_static::lazy_static;
use tracing_subscriber::EnvFilter;

pub mod config;
pub mod swim;

lazy_static! {
    static ref TRACING: () = {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    };
}

fn init_tracing() {
    lazy_static::initialize(&TRACING);
}
