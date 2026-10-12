//! Registry for independently managed Clash cores.
//!
//! The registry describes installed binaries and the selected core without changing the
//! existing lifecycle. It is deliberately additive: the current Mihomo sidecars remain the
//! source of truth for startup until a core has passed the same validation path.

use crate::{
    config::{Config, IVerge},
    utils::dirs,
};
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::SystemTime};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CoreKind {
    Mihomo,
    MihomoAlpha,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManagedCore {
    pub id: String,
    pub display_name: String,
    pub kind: CoreKind,
    pub executable_name: String,
    pub path: String,
    pub installed: bool,
    pub installed_version: Option<String>,
    pub active: bool,
    pub verified: bool,
    pub last_verified_at: Option<i64>,
    pub can_update: bool,
    pub can_remove: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CoreRegistry {
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default)]
    pub entries: Vec<CoreRegistryEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CoreRegistryEntry {
    pub id: String,
    pub kind: CoreKind,
    #[serde(default)]
    pub last_verified_at: Option<i64>,
}

fn executable_name(id: &str) -> Result<String> {
    if !IVerge::VALID_CLASH_CORES.contains(&id) {
        bail!("unsupported managed core: {id}");
    }
    Ok(format!("{id}{}", std::env::consts::EXE_SUFFIX))
}

fn executable_path(id: &str) -> Result<PathBuf> {
    Ok(std::env::current_exe()
        .context("failed to locate the application executable")?
        .with_file_name(executable_name(id)?))
}

fn registry_path() -> Result<PathBuf> {
    Ok(dirs::app_home_dir()?.join("core-registry.yaml"))
}

async fn load_registry() -> Result<CoreRegistry> {
    let path = registry_path()?;
    match crate::utils::help::read_yaml(&path).await {
        Ok(registry) => Ok(registry),
        Err(error) if error.to_string().contains("file not found") => Ok(CoreRegistry {
            schema_version: 1,
            entries: Vec::new(),
        }),
        Err(error) => Err(error),
    }
}

fn core_kind(id: &str) -> Result<CoreKind> {
    match id {
        "verge-mihomo" => Ok(CoreKind::Mihomo),
        "verge-mihomo-alpha" => Ok(CoreKind::MihomoAlpha),
        _ => bail!("unsupported managed core: {id}"),
    }
}

fn version_for(path: &std::path::Path) -> Option<String> {
    crate::feat::read_core_version(path).ok()
}

pub async fn list_managed_cores() -> Result<Vec<ManagedCore>> {
    let registry = load_registry().await?;
    let active = Config::verge().await.latest_arc().get_valid_clash_core();
    let mut result = Vec::new();

    for id in IVerge::VALID_CLASH_CORES {
        let path = executable_path(id)?;
        let installed = path.is_file();
        let verified_version = installed.then(|| version_for(&path)).flatten();
        let verified = installed && verified_version.is_some();
        let entry = registry.entries.iter().find(|entry| entry.id == *id);
        result.push(ManagedCore {
            id: (*id).to_owned(),
            display_name: match core_kind(id)? {
                CoreKind::Mihomo => "Mihomo".to_owned(),
                CoreKind::MihomoAlpha => "Mihomo Alpha".to_owned(),
            },
            kind: core_kind(id)?,
            executable_name: executable_name(id)?,
            path: path.display().to_string(),
            installed,
            installed_version: verified_version,
            active: active == *id,
            verified,
            last_verified_at: entry.and_then(|entry| entry.last_verified_at),
            can_update: installed,
            can_remove: installed && active != *id,
        });
    }

    Ok(result)
}

pub fn activate_managed_core(id: &str) -> Result<()> {
    let path = executable_path(id)?;
    if !path.is_file() {
        bail!("managed core is not installed: {id}");
    }
    if version_for(&path).is_none() {
        bail!("managed core failed validation: {id}");
    }
    Ok(())
}

pub async fn record_verification(id: &str) -> Result<()> {
    let kind = core_kind(id)?;
    let mut registry = load_registry().await?;
    registry.schema_version = 1;
    if let Some(entry) = registry.entries.iter_mut().find(|entry| entry.id == id) {
        entry.kind = kind;
        entry.last_verified_at = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        );
    } else {
        registry.entries.push(CoreRegistryEntry {
            id: id.to_owned(),
            kind,
            last_verified_at: Some(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            ),
        });
    }
    let path = registry_path()?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    crate::utils::help::save_yaml(&path, &registry, Some("# Managed Clash core registry")).await
}

