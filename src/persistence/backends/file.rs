// Copyright © 2024 Pathway

use log::warn;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use futures::channel::oneshot;
use glob::Pattern as GlobPattern;

use crate::fs_helpers::ensure_directory;
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;

use super::BackendPutFuture;

const TEMPORARY_OBJECT_SUFFIX: &str = ".tmp";

#[derive(Debug)]
pub struct FilesystemKVStorage {
    root_path: PathBuf,
    root_glob_pattern: GlobPattern,
    #[allow(dead_code)]
    path_prefix_len: usize,
}

impl FilesystemKVStorage {
    pub fn new(root_path: &Path) -> Result<Self, Error> {
        let root_path_str = root_path.to_str().ok_or(Error::PathIsNotUtf8)?;
        // Normalize path separators for glob pattern - use forward slashes on all platforms
        let normalized_path = root_path_str.replace('\\', "/");
        let root_glob_pattern = GlobPattern::new(&format!("{normalized_path}/**/*"))?;
        ensure_directory(root_path)?;
        Ok(Self {
            root_path: root_path.to_path_buf(),
            root_glob_pattern,
            path_prefix_len: root_path_str.len() + 1,
        })
    }

    fn write_file(temp_path: &Path, final_path: &Path, value: &[u8]) -> Result<(), Error> {
        #[cfg(windows)]
        {
            // On Windows, use async operations when available for better overlapped I/O
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                return handle.block_on(async {
                    tokio::fs::write(temp_path, value).await?;
                    tokio::fs::rename(temp_path, final_path).await?;
                    Ok(())
                });
            }
        }
        
        let mut output_file = File::create(temp_path)?;
        output_file.write_all(value)?;
        // Note: if we need Pathway to tolerate not only Pathway failures,
        // but only OS crash or power loss, the below line must be uncommented.
        // output_file.sync_all()?;
        std::fs::rename(temp_path, final_path)?;
        Ok(())
    }

    /// Convert a normalized key (with forward slashes) to a platform-specific path
    fn key_to_path(&self, key: &str) -> PathBuf {
        // Split by forward slashes and join using platform-specific separators
        let components: Vec<&str> = key.split('/').collect();
        let mut path = self.root_path.clone();
        for component in components {
            if !component.is_empty() {
                path = path.join(component);
            }
        }
        path
    }
}

impl PersistenceBackend for FilesystemKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let mut keys = Vec::new();
        let file_and_folder_paths = glob::glob(self.root_glob_pattern.as_str())?.flatten();
        for entry in file_and_folder_paths {
            if !entry.is_file() {
                continue;
            }
            if let Some(path_str) = entry.to_str() {
                let is_temporary = path_str.ends_with(TEMPORARY_OBJECT_SUFFIX);
                if !is_temporary {
                    // Get relative path from the root directory to handle cross-platform paths
                    let relative_path = entry.strip_prefix(&self.root_path)
                        .map_err(|_| Error::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to get relative path"
                        )))?;
                    if let Some(key) = relative_path.to_str() {
                        // Normalize path separators for consistency
                        let normalized_key = key.replace('\\', "/");
                        keys.push(normalized_key);
                    }
                }
            } else {
                warn!("The path is not UTF-8 encoded: {}", entry.display());
            }
        }
        Ok(keys)
    }

    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error> {
        let path = self.key_to_path(key);
        
        #[cfg(windows)]
        {
            // On Windows, use async operations when available for better overlapped I/O
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                return Ok(handle.block_on(tokio::fs::read(path))?);
            }
        }
        
        Ok(std::fs::read(path)?)
    }

    fn put_value(&self, key: &str, value: Vec<u8>) -> BackendPutFuture {
        let (sender, receiver) = oneshot::channel();

        let final_path = self.key_to_path(key);
        let tmp_path = final_path.with_extension(
            final_path.extension()
                .map(|ext| format!("{}.tmp", ext.to_string_lossy()))
                .unwrap_or_else(|| TEMPORARY_OBJECT_SUFFIX.to_string())
        );
        
        if let Some(parent) = final_path.parent() {
            if let Err(e) = ensure_directory(parent) {
                sender.send(Err(Error::Io(e))).expect("Receiver should be listening");
                return receiver;
            }
        }
        
        #[cfg(windows)]
        {
            // On Windows, prefer async operations when available
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let result = async {
                        tokio::fs::write(&tmp_path, &value).await?;
                        tokio::fs::rename(&tmp_path, &final_path).await?;
                        Ok::<(), std::io::Error>(())
                    }.await;
                    let _ = sender.send(result.map_err(Error::Io));
                });
                return receiver;
            }
        }
        
        std::thread::spawn(move || {
            let put_value_result = Self::write_file(&tmp_path, &final_path, &value);
            let _ = sender.send(put_value_result);
        });
        
        receiver
    }

    fn remove_key(&self, key: &str) -> Result<(), Error> {
        let path = self.key_to_path(key);
        
        #[cfg(windows)]
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                return Ok(handle.block_on(tokio::fs::remove_file(path))?);
            }
        }
        
        std::fs::remove_file(path)?;
        Ok(())
    }
}
