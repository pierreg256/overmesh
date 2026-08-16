use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FaultId(String);

impl FaultId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn commit_rank(&self) -> Option<u8> {
        self.0
            .strip_prefix("FAULT-COMMIT-")
            .and_then(|value| value.parse().ok())
    }
}

impl fmt::Display for FaultId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for FaultId {
    type Err = FaultParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if SUPPORTED_FAULTS.contains(&value) {
            Ok(Self(value.to_owned()))
        } else {
            Err(FaultParseError(value.to_owned()))
        }
    }
}

#[derive(Debug, Error)]
#[error("unsupported fault id: {0}")]
pub struct FaultParseError(String);

#[derive(Debug, Clone, Default)]
pub struct FaultSchedule {
    faults: Vec<FaultId>,
}

impl FaultSchedule {
    pub fn new(faults: Vec<FaultId>) -> Self {
        Self { faults }
    }

    pub fn contains(&self, id: &str) -> bool {
        self.faults.iter().any(|fault| fault.as_str() == id)
    }

    pub fn first_commit_rank(&self) -> Option<u8> {
        self.faults.iter().filter_map(FaultId::commit_rank).min()
    }
}

pub const SUPPORTED_FAULTS: &[&str] = &[
    "FAULT-NET-001",
    "FAULT-NET-002",
    "FAULT-NET-003",
    "FAULT-NET-004",
    "FAULT-NET-005",
    "FAULT-NET-006",
    "FAULT-NET-007",
    "FAULT-NET-008",
    "FAULT-RING-001",
    "FAULT-RING-002",
    "FAULT-RING-003",
    "FAULT-RING-004",
    "FAULT-RING-005",
    "FAULT-RING-006",
    "FAULT-RING-007",
    "FAULT-PROC-001",
    "FAULT-PROC-002",
    "FAULT-PROC-003",
    "FAULT-PROC-004",
    "FAULT-PROC-005",
    "FAULT-PROC-006",
    "FAULT-COMMIT-001",
    "FAULT-COMMIT-002",
    "FAULT-COMMIT-003",
    "FAULT-COMMIT-004",
    "FAULT-COMMIT-005",
    "FAULT-COMMIT-006",
    "FAULT-COMMIT-007",
    "FAULT-COMMIT-008",
    "FAULT-COMMIT-009",
    "FAULT-COMMIT-010",
    "FAULT-COMMIT-011",
    "FAULT-COMMIT-012",
    "FAULT-RACE-001",
    "FAULT-RACE-002",
    "FAULT-RACE-003",
    "FAULT-RACE-004",
    "FAULT-RACE-005",
    "FAULT-RACE-006",
    "FAULT-RACE-007",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_known_faults() {
        let fault = FaultId::from_str("FAULT-COMMIT-008").expect("known fault");
        assert_eq!(fault.commit_rank(), Some(8));
        assert!(FaultId::from_str("FAULT-UNKNOWN-001").is_err());
    }
}
