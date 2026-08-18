use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign(secret: &str, job_id: &str, sha256: &str, expires_at: i64) -> String {
    let message = message(job_id, sha256, expires_at);
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(message.as_bytes());
    format!("{expires_at}.{}", hex::encode(mac.finalize().into_bytes()))
}

fn message(job_id: &str, sha256: &str, expires_at: i64) -> String {
    format!("v1|{job_id}|{sha256}|{expires_at}")
}

#[cfg(test)]
mod tests {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    use super::{message, sign};

    #[test]
    fn signs_the_shared_upload_contract() {
        let token = sign("secret", "job-1", &"a".repeat(64), 1_800_000_000);
        let (expires_at, signature) = token.split_once('.').unwrap();
        assert_eq!(expires_at, "1800000000");

        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(message("job-1", &"a".repeat(64), 1_800_000_000).as_bytes());
        assert_eq!(signature, hex::encode(mac.finalize().into_bytes()));
    }
}
