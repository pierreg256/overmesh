use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use anyhow::{Context, Result};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};

pub fn generate(path: &Path, size: u64, seed: u64) -> Result<String> {
    let file = File::create(path)
        .with_context(|| format!("failed to create dataset {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut hasher = Sha256::new();
    let mut remaining = size;
    let mut buffer = [0_u8; 64 * 1024];

    while remaining > 0 {
        let chunk_size = remaining.min(buffer.len() as u64) as usize;
        rng.fill_bytes(&mut buffer[..chunk_size]);
        writer.write_all(&buffer[..chunk_size])?;
        hasher.update(&buffer[..chunk_size]);
        remaining -= chunk_size as u64;
    }
    writer.flush()?;

    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use super::*;

    #[test]
    fn generation_is_deterministic() {
        let first = env::temp_dir().join("overmesh-dataset-first.bin");
        let second = env::temp_dir().join("overmesh-dataset-second.bin");
        let first_hash = generate(&first, 1024, 42).expect("first dataset");
        let second_hash = generate(&second, 1024, 42).expect("second dataset");
        assert_eq!(first_hash, second_hash);
        assert_eq!(
            fs::read(first).expect("first bytes"),
            fs::read(second).expect("second bytes")
        );
    }
}
