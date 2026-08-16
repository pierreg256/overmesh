use std::path::PathBuf;

use axum::body::Body;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};

use crate::manifest::{BlockDescriptor, sha256_bytes};

pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum SpoolBodyError {
    #[error("request body exceeds the configured limit")]
    TooLarge,
    #[error("block size must be greater than zero")]
    InvalidBlockSize,
    #[error("request body stream failed: {0}")]
    Body(#[from] axum::Error),
    #[error("request body spool I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("request body length conversion failed: {0}")]
    Length(#[from] std::num::TryFromIntError),
    #[error("request body length overflow")]
    LengthOverflow,
}

#[derive(Debug)]
pub struct SpoolContent {
    _directory: TempDir,
    pub path: PathBuf,
    pub length: u64,
    pub content_sha256: String,
    pub blocks: Vec<BlockDescriptor>,
}

pub struct SpoolBuilder {
    directory: TempDir,
    path: PathBuf,
    writer: BufWriter<File>,
    content_hasher: Sha256,
    blocks: Vec<BlockDescriptor>,
    length: u64,
}

impl SpoolBuilder {
    pub async fn new() -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("content.bin");
        let writer = BufWriter::new(File::create(&path).await?);
        Ok(Self {
            directory,
            path,
            writer,
            content_hasher: Sha256::new(),
            blocks: Vec::new(),
            length: 0,
        })
    }

    pub async fn append_block(
        &mut self,
        bytes: &[u8],
        client_block_id: Option<String>,
    ) -> anyhow::Result<()> {
        self.writer.write_all(bytes).await?;
        self.content_hasher.update(bytes);
        self.blocks.push(BlockDescriptor {
            index: u32::try_from(self.blocks.len())?,
            offset: self.length,
            length: u64::try_from(bytes.len())?,
            sha256: sha256_bytes(bytes),
            client_block_id,
        });
        self.length = self
            .length
            .checked_add(u64::try_from(bytes.len())?)
            .ok_or_else(|| anyhow::anyhow!("spooled content length overflow"))?;
        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<SpoolContent> {
        if self.blocks.is_empty() {
            self.append_block(&[], None).await?;
        }
        self.writer.flush().await?;
        Ok(SpoolContent {
            _directory: self.directory,
            path: self.path,
            length: self.length,
            content_sha256: format!("sha256:{}", hex::encode(self.content_hasher.finalize())),
            blocks: self.blocks,
        })
    }
}

pub async fn spool_body(body: Body, block_size: usize) -> Result<SpoolContent, SpoolBodyError> {
    spool_body_inner(body, block_size, None).await
}

pub async fn spool_body_limited(
    body: Body,
    block_size: usize,
    max_length: u64,
) -> Result<SpoolContent, SpoolBodyError> {
    spool_body_inner(body, block_size, Some(max_length)).await
}

async fn spool_body_inner(
    body: Body,
    block_size: usize,
    max_length: Option<u64>,
) -> Result<SpoolContent, SpoolBodyError> {
    if block_size == 0 {
        return Err(SpoolBodyError::InvalidBlockSize);
    }
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
        let next_length = length
            .checked_add(u64::try_from(chunk.len())?)
            .ok_or(SpoolBodyError::LengthOverflow)?;
        if max_length.is_some_and(|limit| next_length > limit) {
            return Err(SpoolBodyError::TooLarge);
        }
        writer.write_all(&chunk).await?;
        content_hasher.update(&chunk);
        length = next_length;

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

fn push_block(blocks: &mut Vec<BlockDescriptor>, bytes: &[u8]) -> Result<(), SpoolBodyError> {
    let offset = blocks.iter().map(|block| block.length).sum();
    blocks.push(BlockDescriptor {
        index: u32::try_from(blocks.len())?,
        offset,
        length: u64::try_from(bytes.len())?,
        sha256: sha256_bytes(bytes),
        client_block_id: None,
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

    #[tokio::test]
    async fn aborts_spooling_as_soon_as_the_limit_is_exceeded() {
        let error = spool_body_limited(Body::from("abcdef"), 4, 5)
            .await
            .expect_err("oversized body");
        assert!(matches!(error, SpoolBodyError::TooLarge));
    }
}
