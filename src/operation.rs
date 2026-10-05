use std::collections::{BTreeMap, HashSet};

use crate::Result;
use serde::{Deserialize, Serialize};

pub type OperationId = String;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Set(String),
    Append(String),
    Remove(Vec<OperationId>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub timestamp: u128,
    pub action: Action,
}

impl Operation {
    pub fn new(timestamp: u128, action: Action) -> Self {
        Self {
            id: format!("{:032x}", rand::random::<u128>()),
            timestamp,
            action,
        }
    }
}

/// Deduplicate deliveries, preserving distinct operations with equal payloads.
pub fn merge(logs: impl IntoIterator<Item = Vec<Operation>>) -> Result<Vec<Operation>> {
    let mut unique = BTreeMap::new();
    for operation in logs.into_iter().flatten() {
        if let Some(existing) = unique.insert(operation.id.clone(), operation.clone()) {
            if existing != operation {
                return Err("conflicting payloads for one operation ID".into());
            }
        }
    }
    let mut operations: Vec<Operation> = unique.into_values().collect();
    operations.sort_by(|a, b| (a.timestamp, &a.id).cmp(&(b.timestamp, &b.id)));
    Ok(operations)
}

pub fn decode(log: Vec<String>) -> Result<Vec<Operation>> {
    log.into_iter()
        .map(|entry| Ok(serde_json::from_str(&entry)?))
        .collect()
}

pub fn scalar(operations: &[Operation]) -> Result<Option<String>> {
    match operations.last().map(|operation| &operation.action) {
        Some(Action::Set(value)) => Ok((!value.is_empty()).then(|| value.clone())),
        Some(_) => Err("list operation in a scalar log".into()),
        None => Ok(None),
    }
}

/// Observed append IDs remain removed even if their entries arrive later.
pub fn list(operations: &[Operation]) -> Result<Vec<(OperationId, String)>> {
    let mut removed = HashSet::new();
    for operation in operations {
        if let Action::Remove(ids) = &operation.action {
            removed.extend(ids.iter().cloned());
        }
    }
    let mut result = Vec::new();
    for operation in operations {
        match &operation.action {
            Action::Append(value) if !removed.contains(&operation.id) => {
                result.push((operation.id.clone(), value.clone()))
            }
            Action::Set(_) => return Err("scalar operation in a list log".into()),
            _ => {}
        }
    }
    Ok(result)
}

pub fn difference(source: &[Operation], destination: &[Operation]) -> Vec<Operation> {
    let present: HashSet<_> = destination.iter().map(|operation| &operation.id).collect();
    source
        .iter()
        .filter(|operation| !present.contains(&operation.id))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(id: &str, timestamp: u128) -> Operation {
        Operation {
            id: id.into(),
            timestamp,
            action: Action::Append("same".into()),
        }
    }

    #[test]
    fn merge_is_commutative_associative_and_idempotent() {
        let a = vec![append("a", 1)];
        let b = vec![append("b", 1)];
        let c = vec![Operation {
            id: "remove-a".into(),
            timestamp: 2,
            action: Action::Remove(vec!["a".into()]),
        }];
        let ab = merge([a.clone(), b.clone()]).unwrap();
        assert_eq!(ab, merge([b.clone(), a.clone()]).unwrap());
        assert_eq!(
            merge([ab.clone(), c.clone()]).unwrap(),
            merge([a, merge([b, c]).unwrap()]).unwrap()
        );
        assert_eq!(merge([ab.clone(), ab.clone()]).unwrap(), ab);
    }

    #[test]
    fn one_identity_cannot_describe_conflicting_mutations() {
        let a = append("same-id", 1);
        let mut b = a.clone();
        b.action = Action::Append("different payload".into());
        assert!(merge([vec![a], vec![b]]).is_err());
    }

    #[test]
    fn deliveries_deduplicate_but_intentional_duplicates_survive() {
        let a = append("a", 10);
        let b = append("b", 10);
        let merged = merge([vec![b.clone(), a.clone()], vec![a.clone(), b]]).unwrap();
        assert_eq!(list(&merged).unwrap().len(), 2);
        assert_eq!(merged[0].id, "a");
    }

    #[test]
    fn interrupted_copy_can_resume_without_a_timestamp_cutoff() {
        let source = vec![append("a", 10), append("b", 20), append("c", 30)];
        assert_eq!(difference(&source, &source[..1]).len(), 2);
        assert_eq!(difference(&source, &source).len(), 0);
        assert_eq!(difference(&[append("b", 10)], &[append("a", 10)]).len(), 1);
    }

    #[test]
    fn removal_survives_late_delivery_and_keeps_later_appends() {
        let a = append("a", 1);
        let remove = Operation {
            id: "r".into(),
            timestamp: 2,
            action: Action::Remove(vec![a.id.clone()]),
        };
        let b = append("b", 3);
        let merged = merge([vec![remove.clone(), b.clone()], vec![a.clone(), remove]]).unwrap();
        assert_eq!(list(&merged).unwrap(), vec![(b.id, "same".into())]);
    }

    #[test]
    fn deletion_projects_as_absent() {
        let log = merge([vec![
            Operation::new(1, Action::Set("value".into())),
            Operation::new(2, Action::Set(String::new())),
        ]])
        .unwrap();
        assert_eq!(scalar(&log).unwrap(), None);
    }
}
