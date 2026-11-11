//! 已签署的固定 293 制品身份；路径只来自只读发布目录。

use crate::error::AppError;
use crate::service::native_contract::NativeReleaseRef;
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

pub(super) const ROOT: &str = "/runtime-releases";
const ASSET: &str = "ccrb-runtime-linux-x64";
const SOURCE: &str = "8968405e26db478af44eabc4635ab5ca557057b702a54460a59c13e1b253e978";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u8,
    release: String,
    platform: String,
    asset: String,
    source_version: String,
    source_sha256: String,
    launcher_sha256: String,
    launcher_bytes: u64,
    image_sha256: String,
    image_bytes: u64,
    worker_schema: u8,
    raw_input_protocol: u8,
    resource_contract: u8,
}

/// 已核验清单提供的精确文件与执行体合同。
pub(super) struct ReleaseArtifact {
    pub path: PathBuf,
    pub launcher_sha256: String,
    pub launcher_bytes: u64,
    pub image_sha256: String,
    pub image_bytes: u64,
}

fn invalid() -> AppError {
    AppError::ServiceUnavailable("原生内核发布身份核验失败".into())
}

fn read_capped(path: &Path, limit: u64) -> Result<Vec<u8>, AppError> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(limit + 1).read_to_end(&mut bytes))
        .map_err(|_| invalid())?;
    if bytes.len() as u64 > limit {
        return Err(invalid());
    }
    Ok(bytes)
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn load(
    root: &Path,
    key: &[u8],
    reference: &NativeReleaseRef,
) -> Result<ReleaseArtifact, AppError> {
    let tag = reference
        .release
        .strip_prefix("v0.1.0-beta.")
        .ok_or_else(invalid)?;
    if tag.is_empty()
        || tag.len() > 9
        || tag.starts_with('0')
        || !tag.bytes().all(|b| b.is_ascii_digit())
        || !is_digest(&reference.launcher_sha256)
    {
        return Err(invalid());
    }
    let dir = root
        .join(&reference.release)
        .join(&reference.launcher_sha256);
    let bytes = read_capped(&dir.join("runtime-release.json"), 4096)?;
    let signature = read_capped(&dir.join("runtime-release.sig"), 64)?;
    UnparsedPublicKey::new(&ED25519, key)
        .verify(&bytes, &signature)
        .map_err(|_| invalid())?;
    let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if manifest.schema_version != 1
        || manifest.release != reference.release
        || manifest.platform != "linux-x64"
        || manifest.asset != ASSET
        || manifest.source_version != "2.1.293"
        || manifest.source_sha256 != SOURCE
        || manifest.launcher_sha256 != reference.launcher_sha256
        || !(1..=27_933_052).contains(&manifest.launcher_bytes)
        || !is_digest(&manifest.image_sha256)
        || !(1..=134_217_728).contains(&manifest.image_bytes)
        || manifest.worker_schema != 2
        || manifest.raw_input_protocol != 1
        || manifest.resource_contract != 1
    {
        return Err(invalid());
    }
    Ok(ReleaseArtifact {
        path: dir.join(ASSET),
        launcher_sha256: manifest.launcher_sha256,
        launcher_bytes: manifest.launcher_bytes,
        image_sha256: manifest.image_sha256,
        image_bytes: manifest.image_bytes,
    })
}

impl ReleaseArtifact {
    /// 从独立部署信任根解析只读发布制品。
    /// @param reference 已冻结的发布标签和分发摘要。
    /// @return 通过签名和协议准入的制品身份。
    pub(super) fn signed(reference: &NativeReleaseRef) -> Result<Self, AppError> {
        let key = std::env::var("V3_NATIVE_RELEASE_PUBLIC_KEY")
            .ok()
            .and_then(|value| hex::decode(value).ok())
            .filter(|value| value.len() == 32)
            .ok_or_else(invalid)?;
        #[cfg(test)]
        if let Ok(directory) = std::env::var("CCRB_TEST_RELEASE_ROOT") {
            return load(Path::new(&directory), &key, reference);
        }
        load(Path::new(ROOT), &key, reference)
    }
}
