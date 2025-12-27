use rand::RngCore;
use rand::rngs::OsRng;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

pub fn generate_api_key() -> String {
    let mut bytes = [0u8; 32];

    let mut rng = OsRng;
    rng.fill_bytes(&mut bytes);

    URL_SAFE_NO_PAD.encode(bytes)
}