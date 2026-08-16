use std::path::PathBuf;

use axum::body::Body;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};

use crate::manifest::{BlockDescriptor, sha256_bytes};

pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub struct SpoolContent {
    _directory: TempDir,
    pub path: PathBuf,
    pub length: u64,
    pub content_sha256: String,
    pub blocks: Vec<BlockDescriptor>,
}

pub async fn spool_body(body: Body, block_size: usize) -> anyhow::Result<SpoolContent> {
    anyhow::ensure!(block_size > 0, "block size must be greater than zero");
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("content.bin");
    let file = File::create(&path).await?;
    let mut writer = BufWriter::new(file);
    let mut stream = body.into_data_stream();
    let mut content_hasher = Sha256::new();
    let mut block_buffer = Vec::with_capacity(block_size);
    let mut blocks = Vec::new();
    let mut length = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        writer.write_all(&chunk).await?;
        content_hasher.update(&chunk);
        length += u64::try_from(chunk.len())?;

        let mut remaining = chunk.as_ref();
        while !remaining.is_empty() {
            let available = block_size - block_buffer.len();
            let consumed = available.min(remaining.len());
            block_buffer.extend_from_slice(&remaining[..consumed]);
            remaining = &remaining[consumed..];
            if block_buffer.len() == block_size {
                push_block(&mut blocks, &block_buffer)?;
                block_buffer.clear();
            }
        }
    }
    if !block_buffer.is_empty() || length == 0 {
        push_block(&mut blocks, &block_buffer)?;
    }
    writer.flush().await?;

    Ok(SpoolContent {
        _directory: directory,
        path,
        length,
        content_sha256: format!("sha256:{}", hex::encode(content_hasher.finalize())),
        blocks,
    })
}

fn push_block(blocks: &mut Vec<BlockDescriptor>, bytes: &[u8]) -> anyhow::Result<()> {
    let offset = blocks.iter().map(|block| block.length).sum();
    blocks.push(BlockDescriptor {
        index: u32::try_from(blocks.len())?,
        offset,
        length: u64::try_from(bytes.len())?,
        sha256: sha256_bytes(bytes),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;

    use super::*;

    #[tokio::test]
    async fn spools_and_hashes_block_boundaries() {
        let content = b"abcdefghij";
        let spooled = spool_body(Body::from(content.as_slice()), 4)
            .await
            .expect("spool");
        assert_eq!(spooled.length, 10);
        assert_eq!(spooled.blocks.len(), 3);
        assert_eq!(spooled.blocks[0].length, 4);
        assert_eq!(spooled.blocks[2].length, 2);
        assert_eq!(spooled.content_sha256, sha256_bytes(content));
        assert_eq!(
            tokio::fs::read(&spooled.path).await.expect("content"),
            content
        );
    }
}
