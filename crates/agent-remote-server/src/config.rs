use std::collections::HashMap;

use agent_remote_protocol::{ErrorCode, ProtocolError};
use serde::Deserialize;

#[derive(Debug, Default, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    pub setup: String,
}

impl ServerConfig {
    pub fn load_from_str(s: &str) -> Result<Self, ProtocolError> {
        if s.trim().is_empty() {
            return Ok(Self::default());
        }
        toml::from_str(s).map_err(|e| {
            ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("invalid server config: {e}"),
            )
        })
    }

    pub fn setup_for(&self, profile: Option<&str>) -> Result<&str, ProtocolError> {
        match profile {
            None => Ok(""),
            Some(name) => self
                .profiles
                .get(name)
                .map(|p| p.setup.as_str())
                .ok_or_else(|| {
                    ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        format!("unknown profile: {name}"),
                    )
                }),
        }
    }
}
